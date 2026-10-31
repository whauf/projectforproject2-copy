-- Repo x month from the window start through the run month; months with no activity show 0.
-- Each event counts in the month it happened: opened by created_at, merged by merged_at,
-- closed-unmerged by closed_at, commits by committed_at.
with run as (
    select {{ run_time() }} as run_time
),

repos as (
    select distinct repo from {{ ref('stg_pull_requests') }}
    union
    select distinct repo from {{ ref('stg_commits') }}
),

months as (
    select cast(month_start as date) as month
    from (
        select unnest(generate_series(
            cast(date_trunc('month', {{ window_start() }}) as date),
            cast(date_trunc('month', (select run_time from run)) as date),
            interval 1 month
        )) as month_start
    )
),

opened as (
    select
        repo,
        cast(date_trunc('month', created_at) as date) as month,
        count(*) as prs_opened,
        median(hours_to_first_review) as median_first_review_hours,
        count(distinct author_login) filter (where not is_bot_author) as active_contributors
    from {{ ref('fct_pull_requests') }}
    group by all
),

merged as (
    select
        repo,
        cast(date_trunc('month', merged_at) as date) as month,
        count(*) as prs_merged,
        median(hours_to_merge) as median_merge_hours
    from {{ ref('fct_pull_requests') }}
    where is_merged
    group by all
),

closed_unmerged as (
    select
        repo,
        cast(date_trunc('month', closed_at) as date) as month,
        count(*) as prs_closed_unmerged
    from {{ ref('fct_pull_requests') }}
    where is_closed_unmerged
    group by all
),

commits as (
    select
        repo,
        cast(date_trunc('month', committed_at) as date) as month,
        count(*) as commits,
        count(*) filter (where is_bot) as bot_commits
    from {{ ref('stg_commits') }}
    where committed_at >= {{ window_start() }}
    group by all
),

releases as (
    select
        repo,
        cast(date_trunc('month', published_at) as date) as month,
        count(*) as releases
    from {{ ref('fct_releases') }}
    group by all
)

select
    r.repo,
    m.month,
    date_diff('month', cast(date_trunc('month', {{ window_start() }}) as date), m.month) as month_index,
    coalesce(o.prs_opened, 0) as prs_opened,
    coalesce(mg.prs_merged, 0) as prs_merged,
    coalesce(cu.prs_closed_unmerged, 0) as prs_closed_unmerged,
    coalesce(mg.prs_merged, 0)
        / nullif(coalesce(mg.prs_merged, 0) + coalesce(cu.prs_closed_unmerged, 0), 0) as merge_ratio,
    o.median_first_review_hours,
    mg.median_merge_hours,
    coalesce(c.commits, 0) as commits,
    coalesce(c.bot_commits, 0) as bot_commits,
    coalesce(o.active_contributors, 0) as active_contributors,
    coalesce(rl.releases, 0) as releases,
    m.month = cast(date_trunc('month', (select run_time from run)) as date) as is_partial_month
from repos as r
cross join months as m
left join opened as o on o.repo = r.repo and o.month = m.month
left join merged as mg on mg.repo = r.repo and mg.month = m.month
left join closed_unmerged as cu on cu.repo = r.repo and cu.month = m.month
left join commits as c on c.repo = r.repo and c.month = m.month
left join releases as rl on rl.repo = r.repo and rl.month = m.month
