"""Open-source package health: one Prefect flow, six tasks (DS 3022 Data Project 2, package set A).

    python main.py            run the whole flow (Tasks 1-6)
    python main.py --graphql  same, but fetch PRs and reviews through the GraphQL API
    python main.py --reload   re-run Task 2 on the files already in data/raw/ (idempotency check)
    python main.py --deploy   register the monthly deployment on the Prefect server (DEPLOYMENTS.md)
"""

import argparse
import json
import os
import time
import warnings
from collections import Counter
from typing import Literal

import pandas as pd
from prefect import flow, task, unmapped
from prefect.cache_policies import NO_CACHE
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipeline import extract, load, model, plots, validate
from pipeline.client import GitHubClient, resolve_token
from pipeline.config import (BASE_DIR, DATA_DIR, DB_PATH, DBT_TARGET_DIR, LOADED_MARKER, LOG_DIR, REPOS,
                             WINDOW_START)
from pipeline.logs import TaskLogger

# duckdb 2.0's .df() still calls DataFrame.drop(inplace=True), which pandas 3.1 deprecates.
warnings.filterwarnings("ignore", message="The inplace keyword in DataFrame.drop is deprecated")

GIT_URL = "https://github.com/whauf/projectforproject2.git"
WORK_POOL = "uvasds-k8s-pool"
PAT_BLOCK = "tgc4mz-pat"  # Prefect Secret block holding the GitHub PAT on the SDS server


@task(name="task1-extract", cache_policy=NO_CACHE)
def extract_github(repos: list[str], github_token_block: str | None = None, pr_source: str = "rest") -> dict:
    """Task 1: land every page of commits, pulls, PR reviews, and releases under data/raw/.

    Talks only to GitHub and writes only files (it reads _load_state, never writes DuckDB).
    The repos are fetched in parallel by a mapped task that shares one rate limiter.
    pr_source="graphql" fetches PRs and reviews through GraphQL instead of REST.
    """
    log = TaskLogger("task1_extract")
    log.info("===== Task 1: extract %s (window %s to now, PRs and reviews via %s) =====",
             ", ".join(repos), WINDOW_START, "GraphQL" if pr_source == "graphql" else "REST")
    started = time.monotonic()
    mode, watermarks = extract.plan_run(log)
    for (repo, endpoint), mark in sorted(watermarks.items()):
        log.info("watermark  %-24s %-8s %s", repo, endpoint, extract.iso(mark))

    client = GitHubClient(resolve_token(github_token_block))
    try:
        futures = extract.extract_repo.map(repos, client=unmapped(client), watermarks=unmapped(watermarks),
                                           pr_source=unmapped(pr_source))
        stats = {repo: future.result() for repo, future in zip(repos, futures)}
    except BaseException as exc:
        client.stop.set()  # Ctrl-C or a failed repo: every repo thread stops after its current request
        log.warning("Task 1 stopped by %s after %d requests this run; the pages already in data/raw/ "
                    "stay there for the next run to reuse", type(exc).__name__, client.odometer.total)
        raise
    finally:
        client.close()

    counts = pd.DataFrame([{"repo": repo, "endpoint": endpoint, **numbers}
                           for repo, by_endpoint in stats.items() for endpoint, numbers in by_endpoint.items()])
    log.table("Records per repo and endpoint (this run):",
              counts[["repo", "endpoint", "records", "pages", "fetched", "reused", "seconds"]])
    seconds = time.monotonic() - started
    log.info("API odometer: %d requests this run %s, GraphQL points spent: %d | rate-limit sleeps: %d (%.0f s) "
             "| Task 1 took %.0f s", client.odometer.total, json.dumps(client.odometer.by_repo()),
             client.odometer.graphql_points, client.limiter.sleeps + client.graphql_limiter.sleeps,
             client.limiter.slept_seconds + client.graphql_limiter.slept_seconds, seconds)
    return {"mode": mode, "requests": client.odometer.total, "seconds": round(seconds)}


@task(name="task2-load", cache_policy=NO_CACHE)
def load_to_duckdb() -> dict:
    """Task 2: upsert every landed page into the raw_* tables, refresh _load_state, then create
    data/raw/_LOADED. Reads only files and writes only DuckDB (no API calls)."""
    log = TaskLogger("task2_load")
    log.info("===== Task 2: load data/raw/ into %s =====", DB_PATH.name)
    con = load.connect()
    try:
        load.ensure_tables(con)
        rows_after = {}
        for endpoint in load.RAW_TABLES:
            frame, files = load.read_landed(endpoint)
            r = load.upsert(con, endpoint, frame)
            log.info("%-17s %5d files, %6d records, %6d unique keys | rows before %6d -> after %6d "
                     "(%d inserted, %d replaced)", r["table"], files, r["records"], r["unique_keys"],
                     r["rows_before"], r["rows_after"], r["inserted"], r["updated"])
            rows_after[r["table"]] = r["rows_after"]
        state = load.record_load_state(con)
    finally:
        con.close()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOADED_MARKER.touch()
    log.table("_load_state after this load:", state)
    log.info("Every landed file is loaded: created data/raw/_LOADED")
    return rows_after


@task(name="task3-dbt-build", cache_policy=NO_CACHE)
def dbt_build() -> dict:
    """Task 3: `dbt build` runs every staging, fact, and mart model and every test.
    This is the only place dbt runs; any failed model or test fails the task (and the flow)."""
    log = TaskLogger("task3_dbt")
    log.info("===== Task 3: dbt build =====")
    os.environ["DBT_DUCKDB_PATH"] = str(DB_PATH)
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir=BASE_DIR, profiles_dir=BASE_DIR, target_path=DBT_TARGET_DIR),
        raise_on_failure=False,
    )
    result = runner.invoke(["build", "--vars", json.dumps({"window_start": WINDOW_START}),
                            "--log-path", str(LOG_DIR)])
    statuses = Counter()
    for node_result in result.result.results:
        status = str(node_result.status)
        statuses[status] += 1
        failing = f"  ({node_result.failures} failing rows)" if node_result.failures else ""
        log.info("%-7s %-5s %s%s", status.upper(), node_result.node.resource_type, node_result.node.name, failing)
    if not result.success:
        raise RuntimeError(f"dbt build failed: {dict(statuses)}. See logs/task3_dbt.log and logs/dbt.log")
    log.info("dbt build passed: %s (dbt's own log: logs/dbt.log)", dict(statuses))
    return dict(statuses)


@task(name="task4-validate", cache_policy=NO_CACHE)
def validate_pipeline() -> dict:
    """Task 4 (Python, no dbt): end-to-end completeness checks, outlier scans, descriptive statistics.
    Raises if anything was lost between data/raw/, the raw tables, and staging."""
    log = TaskLogger("task4_validate")
    log.info("===== Task 4: validation =====")
    con = load.connect()
    try:
        checks = validate.run_completeness_checks(con, log)
        fences, slow_prs = validate.merge_time_outliers(con)
        week_fences, busy_weeks = validate.weekly_commit_outliers(con)
        stats = validate.describe(con)
    finally:
        con.close()
    log.table(f"Outlier scan 1, time to merge. Cutoff per repo: log10(1 + hours) > Q3 + {validate.MERGE_K:g} x IQR",
              fences)
    log.table("Flagged PRs. KEPT: real slow merges; the scored metrics are medians and shares, which they barely "
              "move", slow_prs)
    log.table(f"Outlier scan 2, commits per complete week. Cutoff per repo: Q3 + {validate.WEEK_K:g} x IQR "
              "(commit trend shown without and with the cap)", week_fences.round(3))
    log.table("Flagged weeks. CAPPED at the fence for the scored commit trend only; raw counts are kept everywhere "
              "else", busy_weeks)
    log.table("Descriptive statistics per repo (hours; medians and 90th percentiles from fct_pull_requests)",
              stats)
    return {**checks, "slow_prs_flagged": len(slow_prs), "busy_weeks_flagged": len(busy_weeks)}


@task(name="task5-model", cache_policy=NO_CACHE)
def model_and_rank() -> list[dict]:
    """Task 5: standardize the mart_health_inputs metrics, combine them with the dimension
    weights, rank the repos, and write the result back to DuckDB."""
    log = TaskLogger("task5_model")
    log.info("===== Task 5: health model and ranking =====")
    con = load.connect()
    try:
        inputs = con.execute("select * from mart_health_inputs order by repo").df()
        metric_scores, ranking = model.score(inputs)
        model.save(con, metric_scores, ranking)
    finally:
        con.close()
    log.table("Metric values and 0-100 scores (min-max across the repos, direction-adjusted):",
              metric_scores.round({"value": 3, "score": 1, "weight": 3}))
    log.table("Ranking (composite health score and per-dimension subscores, 0-100):", ranking.round(1))
    log.info("Wrote health_scores and health_metric_scores to %s", DB_PATH.name)
    return ranking.to_dict("records")


@task(name="task6-plot", cache_policy=NO_CACHE)
def make_plots() -> list[str]:
    """Task 6: save the dimension profile, monthly trends, and relationship plots to plots/."""
    log = TaskLogger("task6_plot")
    log.info("===== Task 6: plots =====")
    con = load.connect()
    try:
        monthly = con.execute("select * from mart_repo_monthly order by repo, month").df()
        ranking = con.execute("select * from health_scores order by rank").df()
    finally:
        con.close()
    ranking = ranking.rename(columns={d.lower().replace(" ", "_"): d for d in model.DIMENSION_WEIGHTS})
    paths = [
        plots.dimension_profile(ranking, model.DIMENSION_WEIGHTS),
        plots.monthly_trends(monthly, ranking),
        plots.review_speed_vs_merge_ratio(monthly, ranking),
    ]
    for path in paths:
        log.info("Saved %s", path.relative_to(path.parent.parent))
    return [str(p) for p in paths]


@flow(name="tgc4mz-package-health-flow", log_prints=True)
def package_health_flow(repos: list[str] = REPOS, github_token_block: str | None = None,
                        pr_source: Literal["rest", "graphql"] = "rest") -> dict:
    """Extract -> load -> dbt build -> validate -> model -> plot, in that order.
    pr_source picks how Task 1 fetches PRs and reviews: REST (default) or GraphQL."""
    started = time.monotonic()
    extracted = extract_github(repos, github_token_block, pr_source)
    load_to_duckdb()
    dbt_build()
    validate_pipeline()
    ranking = model_and_rank()
    make_plots()
    log = TaskLogger("flow")
    log.info("Flow finished in %.1f min | %s run | API odometer: %d GitHub requests | ranking: %s",
             (time.monotonic() - started) / 60, extracted["mode"], extracted["requests"],
             ", ".join(f"#{r['rank']} {r['repo']} ({r['health_score']:.1f})" for r in ranking))
    return {"mode": extracted["mode"], "api_requests": extracted["requests"], "ranking": ranking}


def deploy() -> None:
    """Register the monthly deployment (midnight Eastern on the 28th) on the SDS Prefect server.
    Worker pods start from a clean checkout, so every scheduled run is a full one: GraphQL mode
    keeps it to minutes instead of the hour-plus REST run."""
    from prefect.blocks.system import Secret
    from prefect.runner.storage import GitRepository
    from prefect.schedules import Cron

    requirements = (BASE_DIR / "requirements.txt").read_text().splitlines()
    packages = " ".join(line for line in requirements if line and not line.startswith("#"))
    package_health_flow.from_source(
        source=GitRepository(url=GIT_URL, branch="main", credentials={"access_token": Secret.load(PAT_BLOCK)}),
        entrypoint="main.py:package_health_flow",
    ).deploy(
        name="tgc4mz-deployment",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        parameters={"github_token_block": PAT_BLOCK, "pr_source": "graphql"},
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
            # Prefect's worker images pip-install these at start-up (dbt, pandas, matplotlib, ...).
            "env": {"EXTRA_PIP_PACKAGES": packages},
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Package-health pipeline (Prefect flow).")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--reload", action="store_true",
                      help="re-run Task 2 on the files already in data/raw/ to show idempotent loads")
    mode.add_argument("--deploy", action="store_true", help="register the monthly Prefect deployment")
    parser.add_argument("--graphql", action="store_true", help="fetch PRs and reviews with GraphQL instead of REST")
    args = parser.parse_args()
    if args.reload:
        load_to_duckdb()
    elif args.deploy:
        deploy()
    else:
        package_health_flow(pr_source="graphql" if args.graphql else "rest")
