"""GitHub REST client shared by the extract tasks.

One client per flow run holds the token, a thread-safe rate limiter that every
request passes through, the API odometer (request counts per repo/endpoint), and a
stop flag that halts every repo thread when the run is interrupted.
"""

import os
import threading
import time
from collections import Counter
from datetime import datetime

import httpx

GRAPHQL_URL = "https://api.github.com/graphql"


class TransientGitHubError(RuntimeError):
    """A failure worth retrying: secondary rate limit, 5xx, network error, truncated body."""


class GitHubAPIError(RuntimeError):
    """A failure that retrying will not fix: bad token, missing resource, invalid request."""


class NotFoundError(GitHubAPIError):
    """HTTP 404 (e.g. a PR deleted between listing it and asking for its reviews)."""


class RunInterrupted(RuntimeError):
    """The flow run was interrupted (Ctrl-C, or another repo failed): send no more requests."""


def is_transient(task, task_run, state) -> bool:
    """Prefect `retry_condition_fn`: retry a task only when it failed for a transient reason."""
    try:
        state.result()
    except TransientGitHubError:
        return True
    except Exception:
        return False
    return False


def resolve_token(secret_block: str | None = None) -> str | None:
    """GitHub PAT from $GITHUB_TOKEN, else from a Prefect Secret block (used by the deployment)."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token.strip()
    if secret_block:
        from prefect.blocks.system import Secret

        return Secret.load(secret_block).get()
    if os.environ.get("PH_ALLOW_ANONYMOUS") == "1":  # development only: 60 requests/hour
        return None
    raise GitHubAPIError("No GitHub token found: export GITHUB_TOKEN=<your PAT> before running the flow.")


class RateLimiter:
    """Primary rate limit (5,000 requests/hour) shared by every thread in the flow run.

    Every response reports X-RateLimit-Remaining and X-RateLimit-Reset. Before each
    request, if the remaining budget is down to `reserve`, the caller sleeps until the
    reset time so the budget never reaches zero. The lock makes the other threads queue
    behind the sleeping one instead of spending the last few requests. The sleep wakes
    early if the run's stop flag is set.
    """

    def __init__(self, stop: threading.Event, reserve: int = 25):
        self.stop = stop
        self.reserve = reserve
        self.remaining: int | None = None
        self.limit: str | None = None
        self.reset_at = 0.0
        self.sleeps = 0
        self.slept_seconds = 0.0
        self._lock = threading.Lock()

    def acquire(self, log) -> None:
        with self._lock:
            if self.remaining is None:
                return
            if self.remaining > self.reserve:
                self.remaining -= 1  # claim a slot until the response reports the real number
                return
            delay = self.reset_at - time.time() + 5
            if delay > 0:
                log.warning(
                    "Rate limit: %d of %s requests left; sleeping %.0f s until the reset at %s",
                    self.remaining, self.limit, delay,
                    datetime.fromtimestamp(self.reset_at).strftime("%H:%M:%S"),
                )
                if self.stop.wait(delay):
                    raise RunInterrupted("run interrupted while waiting for the rate-limit reset")
                self.sleeps += 1
                self.slept_seconds += delay
            self.remaining = None  # unknown until the next response reports it

    def update(self, headers: httpx.Headers) -> None:
        remaining, reset = headers.get("X-RateLimit-Remaining"), headers.get("X-RateLimit-Reset")
        if remaining is None or reset is None:
            return
        with self._lock:
            self.remaining = int(remaining)
            self.reset_at = float(reset)
            self.limit = headers.get("X-RateLimit-Limit")


class Odometer:
    """Counts every GitHub API request made during one flow run (and the GraphQL points spent)."""

    def __init__(self):
        self._counts: Counter = Counter()
        self.graphql_points = 0
        self._lock = threading.Lock()

    def tick(self, repo: str, endpoint: str) -> None:
        with self._lock:
            self._counts[(repo, endpoint)] += 1

    def add_points(self, points: int) -> None:
        with self._lock:
            self.graphql_points += points

    @property
    def total(self) -> int:
        with self._lock:
            return sum(self._counts.values())

    def by_repo(self) -> dict[str, dict[str, int]]:
        with self._lock:
            out: dict[str, dict[str, int]] = {}
            for (repo, endpoint), n in sorted(self._counts.items()):
                out.setdefault(repo, {})[endpoint] = n
            return out


def _error_message(resp: httpx.Response) -> str:
    try:
        return str(resp.json().get("message", ""))[:300]
    except ValueError:
        return resp.text[:300]


class GitHubClient:
    """Thin httpx wrapper: rate limiting, request counting, and error classification.

    REST and GraphQL have separate hourly budgets (5,000 requests vs. 5,000 points),
    so each gets its own RateLimiter.
    """

    def __init__(self, token: str | None):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "uva-ds3022-package-health",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.Client(headers=headers, timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=True)
        self.stop = threading.Event()  # set on Ctrl-C (or a failed repo) so every repo thread stops
        self.limiter = RateLimiter(self.stop)
        self.graphql_limiter = RateLimiter(self.stop)
        self.odometer = Odometer()

    def close(self) -> None:
        self._http.close()

    def _pause(self, seconds: float) -> None:
        """Sleep, but give up as soon as the run is interrupted."""
        if self.stop.wait(seconds):
            raise RunInterrupted("run interrupted while backing off")

    def get(self, url: str, *, repo: str, endpoint: str, log) -> tuple[list | dict, str | None]:
        """GET one page and return (parsed JSON body, URL of the Link header's rel="next" or None)."""
        if self.stop.is_set():
            raise RunInterrupted("run interrupted: no more requests")
        self.limiter.acquire(log)
        self.odometer.tick(repo, endpoint)
        try:
            resp = self._http.get(url)
        except httpx.TransportError as exc:
            raise TransientGitHubError(f"{type(exc).__name__} requesting {url}: {exc}") from exc
        self.limiter.update(resp.headers)
        if resp.status_code != 200:
            self._raise_for_status(resp, url, log)
        try:
            body = resp.json()
        except ValueError as exc:
            raise TransientGitHubError(f"Unparseable JSON body from {url}: {exc}") from exc
        return body, resp.links.get("next", {}).get("url")

    def graphql(self, query: str, variables: dict, *, repo: str, log) -> dict:
        """POST one GraphQL query and return the parsed body. The response headers carry the
        GraphQL budget; the query's own `rateLimit` field reports the points it cost."""
        if self.stop.is_set():
            raise RunInterrupted("run interrupted: no more requests")
        self.graphql_limiter.acquire(log)
        self.odometer.tick(repo, "graphql")
        try:
            resp = self._http.post(GRAPHQL_URL, json={"query": query, "variables": variables})
        except httpx.TransportError as exc:
            raise TransientGitHubError(f"{type(exc).__name__} requesting {GRAPHQL_URL}: {exc}") from exc
        self.graphql_limiter.update(resp.headers)
        if resp.status_code != 200:
            self._raise_for_status(resp, GRAPHQL_URL, log)
        try:
            body = resp.json()
        except ValueError as exc:
            raise TransientGitHubError(f"Unparseable JSON body from {GRAPHQL_URL}: {exc}") from exc
        if body.get("errors"):
            kinds = {error.get("type") for error in body["errors"]}
            message = "; ".join(error.get("message", "") for error in body["errors"])[:300]
            if "RATE_LIMITED" in kinds or "timeout" in message.lower() or "something went wrong" in message.lower():
                raise TransientGitHubError(f"GraphQL error for {repo}: {message}")
            if "NOT_FOUND" in kinds:
                raise NotFoundError(f"GraphQL NOT_FOUND for {repo}: {message}")
            raise GitHubAPIError(f"GraphQL error for {repo}: {message}")
        self.odometer.add_points((body["data"].get("rateLimit") or {}).get("cost", 0))
        return body

    def _raise_for_status(self, resp: httpx.Response, url: str, log) -> None:
        status, message = resp.status_code, _error_message(resp)
        if status in (403, 429):
            retry_after = resp.headers.get("Retry-After")
            if retry_after is not None:
                wait = float(retry_after)
                log.warning("HTTP %d secondary rate limit on %s: honoring Retry-After, sleeping %.0f s", status, url, wait)
                self._pause(wait)
                raise TransientGitHubError(f"HTTP {status} secondary rate limit on {url} (waited {wait:.0f} s)")
            if resp.headers.get("X-RateLimit-Remaining") == "0":
                # The limiter now holds remaining=0, so the retry sleeps until the reset.
                raise TransientGitHubError(f"HTTP {status} primary rate limit exhausted on {url}")
            if status == 429 or "rate limit" in message.lower():
                log.warning("HTTP %d secondary rate limit on %s without Retry-After: sleeping 60 s", status, url)
                self._pause(60)
                raise TransientGitHubError(f"HTTP {status} secondary rate limit on {url}")
            raise GitHubAPIError(f"HTTP {status} on {url}: {message}")
        if status >= 500:
            raise TransientGitHubError(f"HTTP {status} on {url}: {message}")
        if status == 404:
            raise NotFoundError(f"HTTP 404 on {url}: {message}")
        if status == 401:
            raise GitHubAPIError(f"HTTP 401 on {url}: GITHUB_TOKEN is invalid or expired ({message})")
        raise GitHubAPIError(f"HTTP {status} on {url}: {message}")
