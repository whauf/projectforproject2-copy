"""Task 4 helpers (Python only, no dbt): end-to-end completeness checks, outlier scans, and
descriptive statistics, read from the landed files and the tables Task 3 built."""

import json

import duckdb
import pandas as pd

from .load import RAW_TABLES, landed_pages, page_records

# Staging model for each endpoint, and the raw rows that model drops on purpose.
STAGING = {
    "commits": ("stg_commits", "false"),
    "pulls": ("stg_pull_requests", "false"),
    "reviews": ("stg_pr_reviews", "payload ->> '$.state' = 'PENDING'"),
    "releases": ("stg_releases", "coalesce((payload ->> '$.draft')::boolean, false)"),
}

# Outlier cutoffs (README, Task 4), both computed per repo:
#   time to merge  - Tukey's fence on log10(1 + hours): Q3 + 1.5 x IQR. Durations are roughly
#                    log-normal, so on the raw scale a 3 x IQR fence would flag the whole right tail.
#   weekly commits - Tukey's "far out" fence on the counts: Q3 + 3 x IQR of the complete weeks.
MERGE_K = 1.5
WEEK_K = 3.0


class ValidationError(RuntimeError):
    """A completeness check failed, so the flow must stop."""


def landed_keys() -> pd.DataFrame:
    """Distinct (endpoint, repo, id) across every landed page in data/raw/."""
    rows = []
    for endpoint, spec in RAW_TABLES.items():
        for folder, path in landed_pages(endpoint):
            page = json.loads(path.read_text(encoding="utf-8"))
            rows.extend((endpoint, page["repo"], str(spec.key(rec))) for rec in page_records(folder, endpoint, page))
    return pd.DataFrame(rows, columns=["endpoint", "repo", "id"]).drop_duplicates()


def landed_vs_raw(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """For each repo and endpoint: distinct landed records and how many are in the raw table."""
    con.register("landed", landed_keys())
    parts = [
        con.execute(f"""
            select ? as endpoint, l.repo, count(*) as landed_records, count(r.id) as in_raw_table
            from landed as l
            left join {spec.name} as r on r.repo = l.repo and r.id::varchar = l.id
            where l.endpoint = ?
            group by l.repo
            order by l.repo
        """, [endpoint, endpoint]).df()
        for endpoint, spec in RAW_TABLES.items()
    ]
    con.unregister("landed")
    result = pd.concat(parts, ignore_index=True)
    result["missing"] = result.landed_records - result.in_raw_table
    return result


def raw_vs_staging(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Raw row counts against staging row counts, net of the rows staging drops by design."""
    parts = []
    for endpoint, spec in RAW_TABLES.items():
        model, dropped = STAGING[endpoint]
        parts.append(con.execute(f"""
            with raw as (
                select repo, count(*) as raw_rows, count(*) filter (where {dropped}) as dropped_by_design
                from {spec.name}
                group by repo
            ),
            stg as (
                select repo, count(*) as staging_rows from {model} group by repo
            )
            select ? as endpoint, raw.repo, raw_rows, dropped_by_design, coalesce(staging_rows, 0) as staging_rows
            from raw
            left join stg on stg.repo = raw.repo
            order by raw.repo
        """, [endpoint]).df())
    result = pd.concat(parts, ignore_index=True)
    result["mismatch"] = result.raw_rows - result.dropped_by_design - result.staging_rows
    return result


def months_without_commits(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, int]:
    """Complete months with zero commits (the current, partial month is exempt)."""
    gaps = con.execute("""
        select repo, strftime(month, '%Y-%m') as month
        from mart_repo_monthly
        where not is_partial_month and commits = 0
        order by repo, month
    """).df()
    months = con.execute("select count(distinct month) from mart_repo_monthly where not is_partial_month").fetchall()[0][0]
    return gaps, months


def run_completeness_checks(con: duckdb.DuckDBPyConnection, log) -> dict:
    """Raise ValidationError if anything was lost between data/raw/, raw tables, and staging."""
    failures = []

    landed = landed_vs_raw(con)
    log.table("Check 1: every distinct (repo, id) in data/raw/ is in its raw table", landed)
    if (landed.missing > 0).any():
        failures.append(f"{int(landed.missing.sum())} landed records are missing from the raw tables")

    staging = raw_vs_staging(con)
    log.table("Check 2: raw rows - rows dropped by design (PENDING reviews, draft releases) = staging rows", staging)
    if (staging.mismatch != 0).any():
        failures.append("raw and staging row counts disagree")

    gaps, months = months_without_commits(con)
    if gaps.empty:
        log.info("Check 3: every repo has commits in all %d complete months of the window", months)
    else:
        log.table("Check 3 FAILED: complete months with zero commits", gaps)
        failures.append(f"{len(gaps)} repo-months without commits")

    if failures:
        raise ValidationError("Completeness checks failed: " + "; ".join(failures))
    log.info("All completeness checks passed")
    return {"landed_records": int(landed.landed_records.sum()), "complete_months": months}


def merge_time_outliers(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Merged PRs whose time to merge is beyond Tukey's fence on the log scale for their repo."""
    fences = con.execute(f"""
        with q as (
            select
                repo,
                count(*) as merged_prs,
                quantile_cont(log10(1 + hours_to_merge), 0.25) as q1,
                quantile_cont(log10(1 + hours_to_merge), 0.75) as q3
            from fct_pull_requests
            where is_merged
            group by repo
        )
        select
            repo,
            merged_prs,
            round(power(10, q1) - 1, 1) as q1_hours,
            round(power(10, q3) - 1, 1) as q3_hours,
            round(power(10, q3 + {MERGE_K} * (q3 - q1)) - 1, 1) as fence_hours,
            round((power(10, q3 + {MERGE_K} * (q3 - q1)) - 1) / 24, 1) as fence_days
        from q
        order by repo
    """).df()
    con.register("merge_fences", fences)
    flagged = con.execute("""
        select f.repo, f.pr_number, f.author_login,
               round(f.hours_to_merge / 24, 1) as days_to_merge,
               f.created_at::date as created, f.merged_at::date as merged
        from fct_pull_requests as f
        inner join merge_fences as m on m.repo = f.repo
        where f.is_merged and f.hours_to_merge > m.fence_hours
        order by f.repo, f.hours_to_merge desc
    """).df()
    con.unregister("merge_fences")
    fences["flagged"] = fences.repo.map(flagged.repo.value_counts()).fillna(0).astype(int)
    fences["pct_flagged"] = (100 * fences.flagged / fences.merged_prs).round(1)
    return fences, flagged


def weekly_commit_outliers(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Complete weeks beyond Q3 + 3 x IQR of the repo's weekly commits.

    dbt (mart_repo_weekly) caps these weeks for the scored commit trend. The fence is recomputed
    here independently, and the task fails if dbt flagged different weeks. The fence table also
    shows the commit trend with and without the cap.
    """
    fences = con.execute(f"""
        select
            repo,
            count(*) as weeks,
            quantile_cont(commits, 0.5) as median_commits,
            quantile_cont(commits, 0.25) as q1,
            quantile_cont(commits, 0.75) as q3,
            quantile_cont(commits, 0.75) + {WEEK_K} * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25))
                as fence,
            regr_slope(commits, week_index) * 52 / nullif(avg(commits), 0) as growth_per_year_raw,
            regr_slope(commits_capped, week_index) * 52 / nullif(avg(commits_capped), 0) as growth_per_year_capped
        from mart_repo_weekly
        where not is_partial_week
        group by repo
        order by repo
    """).df()
    con.register("week_fences", fences)
    flagged = con.execute("""
        select w.repo, w.week, w.commits, w.bot_commits, w.commit_authors, w.commits_capped,
               round(w.commits / nullif(f.median_commits, 0), 1) as x_median_week,
               w.commits > f.fence as over_fence, w.is_commit_outlier as capped_by_dbt
        from mart_repo_weekly as w
        inner join week_fences as f on f.repo = w.repo
        where not w.is_partial_week and (w.commits > f.fence or w.is_commit_outlier)
        order by w.repo, w.week
    """).df()
    con.unregister("week_fences")
    if (flagged.over_fence != flagged.capped_by_dbt).any():
        raise ValidationError("dbt capped different weeks than the Task 4 fence flags")
    fences["flagged"] = fences.repo.map(flagged.repo.value_counts()).fillna(0).astype(int)
    return fences, flagged.drop(columns=["over_fence", "capped_by_dbt"])


def describe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Per repo: median and P90 time to first review and to merge, commits per week, releases per year."""
    return con.execute("""
        with durations as (
            select
                repo,
                count(*) as prs,
                count(*) filter (where is_merged) as merged,
                count(*) filter (where is_open) as still_open,
                median(hours_to_first_review) as median_first_review_h,
                quantile_cont(hours_to_first_review, 0.9) as p90_first_review_h,
                median(hours_to_merge) as median_merge_h,
                quantile_cont(hours_to_merge, 0.9) as p90_merge_h
            from fct_pull_requests
            group by repo
        ),
        weekly as (
            select repo, median(commits) as median_commits_per_week
            from mart_repo_weekly
            where not is_partial_week
            group by repo
        )
        select d.*, h.commits_per_week as mean_commits_per_week, w.median_commits_per_week, h.releases_per_year
        from durations as d
        inner join mart_health_inputs as h on h.repo = d.repo
        inner join weekly as w on w.repo = d.repo
        order by d.repo
    """).df().round(1)
