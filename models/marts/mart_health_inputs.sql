-- One row per repo with every metric scored in Task 5. Each column is a rate, share, median,
-- or slope rather than a raw count, so the largest project cannot win on volume alone.
with run as (
    select {{ run_time() }} as run_time
),

window_length as (
    select date_diff('second', {{ window_start() }}, run_time) / 86400.0 as days
    from run
),

pr_metrics as (
    select
        repo,
        median(hours_to_first_review) as median_hours_to_first_review,
        avg(reviewed_within_7d::int) filter (where is_older_than_7d) as pct_reviewed_within_7d,
        count(*) filter (where is_merged)
            / nullif(count(*) filter (where is_merged or is_closed_unmerged), 0) as merge_rate,
        median(hours_to_merge) as median_hours_to_merge,
        avg(is_stale::int) filter (where is_open) as pct_open_stale
    from {{ ref('fct_pull_requests') }}
    group by repo
),

commit_metrics as (
    select
        repo,
        count(*) / ((select days from window_length) / 7) as commits_per_week
    from {{ ref('stg_commits') }}
    where committed_at >= {{ window_start() }}
    group by repo
),

-- Trends: OLS slope over the complete months, divided by the repo's own monthly mean and
-- scaled to a year, so +0.10 means "growing by 10% of a typical month per year".
monthly_trends as (
    select
        repo,
        regr_slope(commits, month_index) as commit_trend,
        regr_slope(active_contributors, month_index) * 12
            / nullif(avg(active_contributors), 0) as contributor_growth_per_year
    from {{ ref('mart_repo_monthly') }}
    where not is_partial_month
    group by repo
),

-- The scored commit trend uses weekly counts capped at the outlier fence, so a few burst
-- weeks cannot decide a 34-month slope (Task 4 lists the capped weeks).
weekly_trends as (
    select
        repo,
        regr_slope(commits_capped, week_index) * 52 / nullif(avg(commits_capped), 0) as commit_growth_per_year
    from {{ ref('mart_repo_weekly') }}
    where not is_partial_week
    group by repo
),

release_metrics as (
    select
        repo,
        count(*) / ((select days from window_length) / 365.25) as releases_per_year
    from {{ ref('fct_releases') }}
    group by repo
),

-- How concentrated merged PRs are among human authors.
author_merges as (
    select repo, author_login, count(*) as merged_prs
    from {{ ref('fct_pull_requests') }}
    where is_merged and not is_bot_author and author_login is not null
    group by all
),

ranked_authors as (
    select
        repo,
        merged_prs,
        row_number() over (partition by repo order by merged_prs desc, author_login) as author_rank,
        sum(merged_prs) over (partition by repo) as total_merged
    from author_merges
),

contributor_metrics as (
    select
        repo,
        sum(merged_prs) filter (where author_rank <= 5) / any_value(total_merged) as top5_author_share
    from ranked_authors
    group by repo
)

select
    p.repo,
    -- Activity
    c.commits_per_week,
    t.commit_trend,
    wt.commit_growth_per_year,
    -- Responsiveness
    p.median_hours_to_first_review,
    p.pct_reviewed_within_7d,
    -- Throughput
    p.merge_rate,
    p.median_hours_to_merge,
    -- Backlog
    coalesce(p.pct_open_stale, 0) as pct_open_stale,
    -- Release cadence
    coalesce(r.releases_per_year, 0) as releases_per_year,
    -- Contributor base
    a.top5_author_share,
    t.contributor_growth_per_year,
    (select run_time from run) as run_time
from pr_metrics as p
left join commit_metrics as c on c.repo = p.repo
left join monthly_trends as t on t.repo = p.repo
left join weekly_trends as wt on wt.repo = p.repo
left join release_metrics as r on r.repo = p.repo
left join contributor_metrics as a on a.repo = p.repo
