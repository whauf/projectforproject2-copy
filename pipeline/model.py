"""Task 5 helpers: standardize the metrics in mart_health_inputs, weight them, and rank the repos."""

from dataclasses import dataclass

import duckdb
import pandas as pd


@dataclass(frozen=True)
class Metric:
    column: str  # column in mart_health_inputs
    dimension: str
    higher_is_better: bool
    definition: str


METRICS = [
    Metric("commit_growth_per_year", "Activity", True,
           "OLS slope of monthly commits over the complete months, x 12, / mean monthly commits"),
    Metric("median_hours_to_first_review", "Responsiveness", False,
           "Median hours from PR creation to the first review by someone other than the author or a bot"),
    Metric("pct_reviewed_within_7d", "Responsiveness", True,
           "Share of PRs created at least 7 days before run time whose first review came within 7 days"),
    Metric("merge_rate", "Throughput", True,
           "Merged / (merged + closed unmerged) over PRs created in the window"),
    Metric("median_hours_to_merge", "Throughput", False,
           "Median hours from creation to merge over merged PRs"),
    Metric("pct_open_stale", "Backlog", False,
           "Share of still-open PRs with no update in 180+ days"),
    Metric("releases_per_year", "Release cadence", True,
           "Published releases in the window per 365.25 days"),
    Metric("top5_author_share", "Contributor base", False,
           "Share of merged PRs written by the five most prolific human authors"),
    Metric("contributor_growth_per_year", "Contributor base", True,
           "OLS slope of monthly active PR authors, x 12, / mean monthly active authors"),
]

# Dimension weights (sum to 1). The README explains the reasoning behind each one.
DIMENSION_WEIGHTS = {
    "Responsiveness": 0.25,
    "Throughput": 0.20,
    "Contributor base": 0.20,
    "Activity": 0.15,
    "Backlog": 0.10,
    "Release cadence": 0.10,
}


def minmax(values: pd.Series, higher_is_better: bool) -> pd.Series:
    """Rescale to 0-100: 100 = healthiest of the repos on this metric, 0 = least healthy."""
    low, high = values.min(), values.max()
    if pd.isna(low) or high == low:
        return pd.Series(50.0, index=values.index)
    scaled = 100 * (values - low) / (high - low)
    return scaled if higher_is_better else 100 - scaled


def score(inputs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (one row per repo x metric with its 0-100 score, the ranked table with subscores)."""
    inputs = inputs.set_index("repo")
    per_dimension = pd.Series([m.dimension for m in METRICS]).value_counts()
    rows = []
    for m in METRICS:
        scaled = minmax(inputs[m.column], m.higher_is_better).fillna(50.0)  # a missing value counts as neutral
        rows += [{
            "repo": repo,
            "dimension": m.dimension,
            "metric": m.column,
            "direction": "higher is healthier" if m.higher_is_better else "lower is healthier",
            "value": inputs.at[repo, m.column],
            "score": scaled[repo],
            "weight": DIMENSION_WEIGHTS[m.dimension] / per_dimension[m.dimension],
        } for repo in inputs.index]
    metric_scores = pd.DataFrame(rows)

    subscores = metric_scores.pivot_table(index="repo", columns="dimension", values="score", aggfunc="mean")
    ranking = subscores[list(DIMENSION_WEIGHTS)].copy()
    ranking["health_score"] = sum(ranking[d] * w for d, w in DIMENSION_WEIGHTS.items())
    ranking = ranking.sort_values("health_score", ascending=False).reset_index()
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    return metric_scores, ranking


def save(con: duckdb.DuckDBPyConnection, metric_scores: pd.DataFrame, ranking: pd.DataFrame) -> None:
    """Write the ranking (health_scores) and the per-metric scores (health_metric_scores) to DuckDB."""
    con.register("ranking_df", ranking.rename(columns=lambda c: c.lower().replace(" ", "_")))
    con.execute("""
        create or replace table health_scores as
        select r.*, h.run_time from ranking_df as r join mart_health_inputs as h using (repo)
        order by rank
    """)
    con.register("metric_df", metric_scores)
    con.execute("create or replace table health_metric_scores as select * from metric_df order by metric, repo")
    con.unregister("ranking_df")
    con.unregister("metric_df")
