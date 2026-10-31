-- Repo x week (weeks start on Monday) of commit counts. Weeks far above a repo's typical
-- range (Tukey's "far out" fence, Q3 + 3 x IQR of its complete weeks) are flagged, and
-- commits_capped holds the count capped at that fence for the activity trend (see README).
with run as (
    select {{ run_time() }} as run_time
),

repos as (
    select distinct repo from {{ ref('stg_commits') }}
),

weeks as (
    select cast(week_start as date) as week
    from (
        select unnest(generate_series(
            cast(date_trunc('week', {{ window_start() }}) as date),
            cast(date_trunc('week', (select run_time from run)) as date),
            interval 1 week
        )) as week_start
    )
),

commits as (
    select
        repo,
        cast(date_trunc('week', committed_at) as date) as week,
        count(*) as commits,
        count(*) filter (where is_bot) as bot_commits,
        count(distinct author_login) as commit_authors
    from {{ ref('stg_commits') }}
    where committed_at >= {{ window_start() }}
    group by all
),

weekly as (
    select
        r.repo,
        w.week,
        date_diff('week', cast(date_trunc('week', {{ window_start() }}) as date), w.week) as week_index,
        coalesce(c.commits, 0) as commits,
        coalesce(c.bot_commits, 0) as bot_commits,
        coalesce(c.commit_authors, 0) as commit_authors,
        w.week = cast(date_trunc('week', (select run_time from run)) as date) as is_partial_week
    from repos as r
    cross join weeks as w
    left join commits as c on c.repo = r.repo and c.week = w.week
),

fences as (
    select
        repo,
        quantile_cont(commits, 0.75)
            + 3 * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) as commit_fence
    from weekly
    where not is_partial_week
    group by repo
)

select
    w.*,
    f.commit_fence,
    not w.is_partial_week and w.commits > f.commit_fence as is_commit_outlier,
    least(w.commits, f.commit_fence) as commits_capped
from weekly as w
inner join fences as f on f.repo = w.repo
