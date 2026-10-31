"""Task 1 helpers: decide how this run starts, then land every response page under data/raw/.

Landing layout (one JSON file per response page):
    data/raw/<owner>__<repo>/commits/page_0001.json
    data/raw/<owner>__<repo>/pulls/page_0001.json
    data/raw/<owner>__<repo>/releases/page_0001.json
    data/raw/<owner>__<repo>/reviews/pr_<number>/page_0001.json   (one call per PR)
    data/raw/<owner>__<repo>/pulls_graphql/page_0001.json         (GraphQL mode: PRs with their reviews)
"""

import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.tasks import exponential_backoff

from .client import GRAPHQL_URL, GitHubClient, NotFoundError, is_transient
from .config import DATA_DIR, DB_PATH, GITHUB_API, LOADED_MARKER, PER_PAGE, WINDOW_START
from .logs import TaskLogger

LOG_NAME = "task1_extract"
WINDOW_START_TS = datetime.fromisoformat(f"{WINDOW_START}T00:00:00+00:00")

# GraphQL mode: one query returns 50 PRs, newest update first, each with its first 100 reviews.
# Aliases give every field its REST name, so the same staging models read both sources.
PR_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        state
        created_at: createdAt
        updated_at: updatedAt
        closed_at: closedAt
        merged_at: mergedAt
        draft: isDraft
        author_association: authorAssociation
        user: author { login type: __typename }
        reviews(first: 100) {
          pageInfo { hasNextPage }
          nodes {
            id: databaseId
            state
            submitted_at: submittedAt
            user: author { login type: __typename }
            pull_request: pullRequest { number }
          }
        }
      }
    }
  }
}
"""

# Retries with exponential backoff (15 s, 30 s, 60 s, 120 s, +-50% jitter) for transient
# failures only. A retry resumes from the pages already on disk, so it costs no extra quota.
RETRY = dict(
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=15),
    retry_jitter_factor=0.5,
    retry_condition_fn=is_transient,
    cache_policy=NO_CACHE,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_watermarks() -> dict[tuple[str, str], datetime]:
    """Newest updated_at loaded per (repo, endpoint), read from Task 2's _load_state table."""
    if not DB_PATH.exists():
        return {}
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        exists = con.execute(
            "select count(*) from information_schema.tables where table_name = '_load_state'"
        ).fetchall()[0][0]
        rows = con.execute("select repo, endpoint, watermark from _load_state").fetchall() if exists else []
    finally:
        con.close()
    return {(repo, endpoint): wm.replace(tzinfo=timezone.utc) for repo, endpoint, wm in rows if wm}


def plan_run(log: TaskLogger) -> tuple[str, dict]:
    """Start fresh, resume, or fetch the full window (README, "Resume or start fresh?")."""
    watermarks = read_watermarks()
    if LOADED_MARKER.exists():
        shutil.rmtree(DATA_DIR)
        log.info("Found data/raw/_LOADED: everything on disk is already in DuckDB. "
                 "Start fresh: deleted data/raw/ and fetching only records updated since the _load_state watermarks.")
        return "incremental", watermarks
    landed = list(DATA_DIR.rglob("page_*.json")) if DATA_DIR.exists() else []
    if landed:
        stale = list(DATA_DIR.rglob("*.tmp"))
        for tmp in stale:
            tmp.unlink()
        log.info("No _LOADED marker and %d landed pages on disk: the last run stopped before loading. "
                 "Resume: keeping data/raw/, skipping pages already on disk (removed %d half-written .tmp files).",
                 len(landed), len(stale))
        return "resume", watermarks
    if watermarks:
        log.info("No data/raw/ but _load_state exists: fetching records updated since the watermarks.")
        return "incremental", watermarks
    log.info("First run: no data/raw/ and no _load_state. Fetching the full window from %s.", WINDOW_START)
    return "full", watermarks


def page_file(repo: str, endpoint: str, page: int, pr_number: int | None = None) -> Path:
    folder = DATA_DIR / repo.replace("/", "__") / endpoint
    if pr_number is not None:
        folder = folder / f"pr_{pr_number}"
    return folder / f"page_{page:04d}.json"


def read_page(path: Path, log: TaskLogger) -> dict | None:
    """A landed page if it is complete; None if it is missing or unreadable (it is then re-fetched)."""
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.warning("Corrupt landing file %s (%s): re-fetching that page", path.relative_to(DATA_DIR), exc)
        return None
    if not isinstance(body, dict) or not isinstance(body.get("data"), (list, dict)):
        log.warning("Landing file %s has no data: re-fetching that page", path.relative_to(DATA_DIR))
        return None
    return body


def write_page(path: Path, body: dict) -> None:
    """Write to a temp file and rename it, so a crash never leaves a half-written page behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(body), encoding="utf-8")
    os.replace(tmp, path)


def paginate(client: GitHubClient, repo: str, endpoint: str, first_url: str, log: TaskLogger,
             *, stop=None, pr_number: int | None = None) -> dict:
    """Land every page of one endpoint, following the Link header's rel="next".

    Pages already on disk are reused instead of fetched. GitHub numbers its pages, so the
    URL of the page after a reused one is the first URL with `page=N+1`; a reused page with
    fewer than PER_PAGE records was the last one. `stop(data)` ends pagination early.
    """
    page, url = 1, first_url
    stats = {"pages": 0, "records": 0, "fetched": 0, "reused": 0}
    while url:
        path = page_file(repo, endpoint, page, pr_number)
        body = read_page(path, log)
        if page == 1 and body is not None and body.get("url") != first_url:
            log.warning("Landed %s pages for %s came from another query (%s): discarding them",
                        endpoint, repo, body.get("url"))
            shutil.rmtree(path.parent)
            body = None
        if body is None:
            data, next_url = client.get(url, repo=repo, endpoint=endpoint, log=log)
            if not isinstance(data, list):
                raise ValueError(f"Expected a JSON list from {url}, got {type(data).__name__}")
            write_page(path, {"repo": repo, "endpoint": endpoint, "url": url, "page": page,
                              "fetched_at": utc_now(), "data": data})
            stats["fetched"] += 1
        else:
            data = body["data"]
            next_url = f"{first_url}&page={page + 1}" if len(data) >= PER_PAGE else None
            stats["reused"] += 1
        stats["pages"] += 1
        stats["records"] += len(data)
        if stop is not None and stop(data):
            break
        url, page = next_url, page + 1
    return stats


def paginate_graphql(client: GitHubClient, repo: str, cutoff: datetime, log: TaskLogger) -> dict:
    """GraphQL mode: land PR pages (each PR with up to 100 reviews), following pageInfo.endCursor.

    Same resume rule as REST: a page already on disk is reused and its endCursor gives the next
    page. Stops after the page that crosses `cutoff` (PRs come newest update first). PRs created
    in the window that have more than 100 reviews are returned so REST can fetch the rest.
    """
    owner, name = repo.split("/")
    page, cursor = 1, None
    stats = {"pages": 0, "records": 0, "fetched": 0, "reused": 0, "reviews": 0, "overflow": []}
    while True:
        path = page_file(repo, "pulls_graphql", page)
        body = read_page(path, log)
        if body is None:
            variables = {"owner": owner, "name": name, "cursor": cursor}
            response = client.graphql(PR_QUERY, variables, repo=repo, log=log)
            body = {"repo": repo, "endpoint": "pulls_graphql", "url": GRAPHQL_URL, "variables": variables,
                    "page": page, "fetched_at": utc_now(), "data": response}
            write_page(path, body)
            stats["fetched"] += 1
        else:
            stats["reused"] += 1
        connection = body["data"]["data"]["repository"]["pullRequests"]
        prs = connection["nodes"]
        stats["pages"] += 1
        stats["records"] += len(prs)
        stats["reviews"] += sum(len(pr["reviews"]["nodes"]) for pr in prs)
        stats["overflow"] += [pr["number"] for pr in prs if pr["reviews"]["pageInfo"]["hasNextPage"]
                              and parse_ts(pr["created_at"]) >= WINDOW_START_TS]
        if not prs or parse_ts(prs[-1]["updated_at"]) < cutoff or not connection["pageInfo"]["hasNextPage"]:
            return stats
        cursor, page = connection["pageInfo"]["endCursor"], page + 1


def updated_before(cutoff: datetime):
    """Stop rule for PRs sorted by updated_at descending: stop after the page that crosses `cutoff`."""
    return lambda data: not data or parse_ts(data[-1]["updated_at"]) < cutoff


def prs_needing_reviews(repo: str, updated_since: datetime) -> list[int]:
    """PRs in this run's landed pulls pages that were created in the window and updated since
    `updated_since` (the window start on a full run, the watermark on an incremental one)."""
    numbers: set[int] = set()
    for path in (DATA_DIR / repo.replace("/", "__") / "pulls").glob("page_*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))["data"]
        numbers.update(pr["number"] for pr in data
                       if parse_ts(pr["created_at"]) >= WINDOW_START_TS
                       and parse_ts(pr["updated_at"]) >= updated_since)
    return sorted(numbers, reverse=True)


@task(task_run_name="fetch-{endpoint}-{repo}", **RETRY)
def fetch_listing(client: GitHubClient, repo: str, endpoint: str, first_url: str,
                  pr_cutoff: datetime | None = None) -> dict:
    """Commits, pulls, or releases for one repo (helper task of Task 1)."""
    log = TaskLogger(LOG_NAME)
    started = time.monotonic()
    stop = updated_before(pr_cutoff) if pr_cutoff else None
    stats = paginate(client, repo, endpoint, first_url, log, stop=stop)
    stats["seconds"] = round(time.monotonic() - started)
    log.info("%-24s %-8s %6d records on %4d pages (%d fetched, %d reused from disk) in %d s",
             repo, endpoint, stats["records"], stats["pages"], stats["fetched"], stats["reused"], stats["seconds"])
    return stats


@task(task_run_name="fetch-pulls-graphql-{repo}", **RETRY)
def fetch_pulls_graphql(client: GitHubClient, repo: str, pr_cutoff: datetime) -> dict:
    """GraphQL mode: PRs and their reviews for one repo in one cursor-paginated query."""
    log = TaskLogger(LOG_NAME)
    started = time.monotonic()
    stats = paginate_graphql(client, repo, pr_cutoff, log)
    stats["seconds"] = round(time.monotonic() - started)
    log.info("%-24s %-8s %6d PRs with %d reviews on %d GraphQL pages (%d fetched, %d reused from disk) in %d s; "
             "GraphQL budget left: %s points", repo, "graphql", stats["records"], stats["reviews"], stats["pages"],
             stats["fetched"], stats["reused"], stats["seconds"], client.graphql_limiter.remaining)
    return stats


@task(task_run_name="fetch-reviews-{repo}", **RETRY)
def fetch_reviews(client: GitHubClient, repo: str, pr_numbers: list[int]) -> dict:
    """One /pulls/{number}/reviews call (plus any extra pages) per PR created in the window."""
    log = TaskLogger(LOG_NAME)
    started = time.monotonic()
    stats = {"prs": len(pr_numbers), "pages": 0, "records": 0, "fetched": 0, "reused": 0, "missing": 0}
    for i, number in enumerate(pr_numbers, start=1):
        url = f"{GITHUB_API}/repos/{repo}/pulls/{number}/reviews?per_page={PER_PAGE}"
        try:
            pr_stats = paginate(client, repo, "reviews", url, log, pr_number=number)
        except NotFoundError as exc:
            log.warning("%s PR #%d: %s (skipped)", repo, number, exc)
            stats["missing"] += 1
            continue
        for key in ("pages", "records", "fetched", "reused"):
            stats[key] += pr_stats[key]
        if i % 250 == 0:
            log.info("%-24s reviews  %d/%d PRs done (%d pages fetched, %d reused) | odometer: %d requests "
                     "this run, rate budget left: %s", repo, i, len(pr_numbers), stats["fetched"], stats["reused"],
                     client.odometer.total, client.limiter.remaining)
    stats["seconds"] = round(time.monotonic() - started)
    log.info("%-24s %-8s %6d records on %4d pages for %d PRs (%d fetched, %d reused, %d missing) in %d s",
             repo, "reviews", stats["records"], stats["pages"], stats["prs"],
             stats["fetched"], stats["reused"], stats["missing"], stats["seconds"])
    return stats


@task(task_run_name="extract-{repo}", cache_policy=NO_CACHE)
def extract_repo(repo: str, client: GitHubClient, watermarks: dict, pr_source: str = "rest") -> dict:
    """All four endpoints for one repo. Reviews run last because they need the landed PR pages.
    With pr_source="graphql", PRs and reviews come from GraphQL instead (REST only for PRs
    with more than 100 reviews)."""
    base = f"{GITHUB_API}/repos/{repo}"
    commits_since = watermarks.get((repo, "commits"), WINDOW_START_TS)
    pulls_cutoff = watermarks.get((repo, "pulls"), WINDOW_START_TS)
    stats = {
        "commits": fetch_listing(client, repo, "commits",
                                 f"{base}/commits?since={iso(commits_since)}&per_page={PER_PAGE}"),
        # Releases have no date filter and are small enough to fetch in full every run.
        "releases": fetch_listing(client, repo, "releases", f"{base}/releases?per_page={PER_PAGE}"),
    }
    if pr_source == "graphql":
        stats["graphql"] = fetch_pulls_graphql(client, repo, pulls_cutoff)
        stats["reviews"] = fetch_reviews(client, repo, stats["graphql"].pop("overflow"))
    else:
        stats["pulls"] = fetch_listing(client, repo, "pulls",
                                       f"{base}/pulls?state=all&sort=updated&direction=desc&per_page={PER_PAGE}",
                                       pr_cutoff=pulls_cutoff)
        stats["reviews"] = fetch_reviews(client, repo, prs_needing_reviews(repo, pulls_cutoff))
    return stats
