"""Task 6 helpers: the three PNG plots saved to plots/. Each repo keeps one color everywhere."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: render straight to files

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from .config import PLOTS_DIR, REPO_COLORS

SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 9.5, "axes.titlesize": 10.5, "axes.titleweight": "bold", "axes.titlecolor": INK,
    "axes.labelcolor": INK_2, "axes.labelsize": 9, "axes.edgecolor": AXIS, "axes.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.7, "grid.linestyle": "-",
    "xtick.color": AXIS, "ytick.color": AXIS, "xtick.labelcolor": INK_2, "ytick.labelcolor": INK_2,
    "legend.frameon": False, "legend.fontsize": 9, "legend.title_fontsize": 9,
    "lines.linewidth": 2, "lines.solid_capstyle": "round", "lines.solid_joinstyle": "round",
})


def _color(repo: str) -> str:
    return REPO_COLORS.get(repo, MUTED)


def _rank_label(row) -> str:
    return f"#{row['rank']}  {row['repo']}  (score {row['health_score']:.0f})"


def _save(fig, name: str, legends: tuple = ()) -> Path:
    """Save as PNG; `legends` placed outside the axes are passed so the tight crop keeps them."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = PLOTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", bbox_extra_artists=list(legends) or None)
    plt.close(fig)
    return path


def dimension_profile(ranking: pd.DataFrame, weights: dict[str, float]) -> Path:
    """Small multiples: one panel per scored dimension plus the composite; rank 1 sits on top."""
    order = ranking.sort_values("rank")
    y = np.arange(len(order))[::-1]
    fig, axes = plt.subplots(2, 4, figsize=(14, 6.4), sharex=True)
    panels = [(d, f"{d}  ·  weight {w:.0%}") for d, w in weights.items()]
    panels.append(("health_score", "Composite health score"))
    for ax, (column, title) in zip(axes.flat, panels):
        values = order[column].to_numpy()
        ax.barh(y, values, height=0.56, color=[_color(r) for r in order.repo])
        for yi, value in zip(y, values):
            ax.text(value + 2, yi, f"{value:.0f}", va="center", color=INK_2, fontsize=9)
        ax.set_title(title, loc="left")
        ax.set_yticks(y, [f"#{rank} {repo.split('/')[1]}" for rank, repo in zip(order["rank"], order.repo)])
        ax.set_xlim(0, 115)
        ax.set_xticks([0, 25, 50, 75, 100])
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", length=0)
    for ax in axes[1]:
        ax.set_xlabel("subscore, 0 = least healthy of the three, 100 = healthiest")
    legend_ax = axes.flat[-1]
    legend_ax.axis("off")
    legend_ax.legend([Patch(color=_color(r)) for r in order.repo], [_rank_label(row) for _, row in order.iterrows()],
                     loc="center left", title="Final ranking, healthiest first", alignment="left")
    fig.text(0.01, 0.995, "Package health by dimension, repos ordered from healthiest (#1) to least healthy",
             va="top", fontsize=13, fontweight="bold", color=INK)
    fig.text(0.01, 0.948, "Each metric is min-max scaled across the three repos; a dimension's subscore is the mean of "
             "its metrics; the composite is the weighted sum of the dimension subscores.", va="top", color=INK_2,
             fontsize=9.5)
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    return _save(fig, "dimension_profile.png")


TREND_PANELS = [
    ("commits", "Commits", "commits per month", False),
    ("active_contributors", "Active PR authors (non-bot)", "authors per month", False),
    ("prs_opened", "PRs opened", "PRs per month", False),
    ("prs_merged", "PRs merged", "PRs per month", False),
    ("prs_closed_unmerged", "PRs closed without merging", "PRs per month", False),
    ("merge_ratio", "Merge ratio", "merged / (merged + closed unmerged)", False),
    ("median_first_review_hours", "Median time to first review", "hours (log scale)", True),
    ("median_merge_hours", "Median time to merge", "hours (log scale)", True),
]


def monthly_trends(monthly: pd.DataFrame, ranking: pd.DataFrame) -> Path:
    """Small multiples from mart_repo_monthly: one panel per metric, one line per repo."""
    monthly = monthly.assign(month=pd.to_datetime(monthly.month))
    repos = ranking.sort_values("rank").repo.tolist()
    fig, axes = plt.subplots(2, 4, figsize=(16, 7.2), sharex=True)
    for ax, (column, title, unit, log_scale) in zip(axes.flat, TREND_PANELS):
        for repo in repos:
            rows = monthly[monthly.repo == repo].sort_values("month")
            done, partial = rows[~rows.is_partial_month], rows[rows.is_partial_month]
            ax.plot(done.month, done[column], color=_color(repo), linewidth=1.8)
            if not partial.empty and not done.empty:  # the current month is incomplete: dotted + hollow dot
                ax.plot([done.month.iloc[-1], partial.month.iloc[0]], [done[column].iloc[-1], partial[column].iloc[0]],
                        color=_color(repo), linewidth=1.4, linestyle=":")
                ax.scatter(partial.month, partial[column], s=30, facecolor=SURFACE, edgecolor=_color(repo),
                           linewidth=1.6, zorder=3)
        ax.set_title(title, loc="left")
        ax.set_ylabel(unit)
        if log_scale:
            ax.set_yscale("log")
        elif column == "merge_ratio":
            ax.set_ylim(0, 1.02)
        else:
            ax.set_ylim(bottom=0)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    labels = {row.repo: _rank_label(row) for _, row in ranking.iterrows()}
    handles = [Line2D([], [], color=_color(r), linewidth=2) for r in repos]
    handles.append(Line2D([], [], color=MUTED, linewidth=1.4, linestyle=":", marker="o",
                          markerfacecolor=SURFACE, markeredgecolor=MUTED))
    fig.legend(handles, [labels[r] for r in repos] + ["current month (partial)"],
               loc="upper left", bbox_to_anchor=(0.01, 0.955), ncol=4)
    start, end = monthly.month.min(), monthly.month.max()
    fig.suptitle(f"Monthly activity, {start:%b %Y} to {end:%b %Y}: one panel per metric, one line per repo",
                 x=0.01, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    return _save(fig, "monthly_trends.png")


def review_speed_vs_merge_ratio(monthly: pd.DataFrame, ranking: pd.DataFrame) -> Path:
    """Bubble chart, one bubble per complete repo-month: x = median hours to first review,
    y = merge ratio, area = active PR authors, color = repo."""
    data = monthly[~monthly.is_partial_month].dropna(subset=["median_first_review_hours", "merge_ratio"])
    area = lambda authors: 10 + 9 * authors  # noqa: E731 - marker area in pt^2
    fig, ax = plt.subplots(figsize=(12, 6.6))
    order = ranking.sort_values("rank")
    for _, row in order.iterrows():
        rows = data[data.repo == row.repo]
        ax.scatter(rows.median_first_review_hours, rows.merge_ratio, s=area(rows.active_contributors),
                   color=_color(row.repo), alpha=0.6, edgecolor=SURFACE, linewidth=1.2)
        # repo-level medians across months, drawn as a ringed marker for reference
        ax.scatter(rows.median_first_review_hours.median(), rows.merge_ratio.median(), s=150, marker="D",
                   color=_color(row.repo), edgecolor=INK, linewidth=1.2, zorder=4)
    if (data.median_first_review_hours > 0).any():
        ax.set_xscale("log")
    ax.set_xlabel("Median hours to first review of PRs opened that month (log scale)")
    ax.set_ylabel("Merge ratio that month: merged / (merged + closed unmerged)")
    ax.set_ylim(min(0.3, data.merge_ratio.min() - 0.05), 1.03)
    repo_keys = [Line2D([], [], linestyle="none", marker="o", markersize=10, markerfacecolor=_color(r),
                        markeredgecolor=SURFACE, alpha=0.8) for r in order.repo]
    repo_legend = ax.legend(repo_keys, [_rank_label(row) for _, row in order.iterrows()], loc="upper left",
                            bbox_to_anchor=(1.02, 1.0), title="Repo (rank, composite score)", alignment="left")
    ax.add_artist(repo_legend)
    sizes = [5, 20, 50]
    size_keys = [Line2D([], [], linestyle="none", marker="o", markersize=np.sqrt(area(n)), markerfacecolor=MUTED,
                        markeredgecolor=SURFACE, alpha=0.6) for n in sizes]
    size_keys.append(Line2D([], [], linestyle="none", marker="D", markerfacecolor=MUTED, markeredgecolor=INK,
                            markersize=8))
    size_legend = ax.legend(size_keys, [f"{n} active authors" for n in sizes] + ["median of the repo's months"],
                            loc="upper left", bbox_to_anchor=(1.02, 0.66), title="Bubble area", labelspacing=1.1,
                            alignment="left")
    ax.set_title("Review speed vs. merge ratio, one bubble per repo-month (complete months only)",
                 loc="left", fontsize=12.5)
    fig.tight_layout(rect=(0, 0, 0.76, 1))  # leave the right side for the two legends
    return _save(fig, "review_speed_vs_merge_ratio.png", legends=(repo_legend, size_legend))
