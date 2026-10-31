-- One row per PR created on or after the window start, with precalculated durations and flags.
-- PRs still open at run time stay in the table with is_open = true; their merge and close
-- durations are NULL (never a fake close date), so medians of those durations skip them.
with prs as (
    select *
    from {{ ref('stg_pull_requests') }}
    where created_at >= {{ window_start() }}
),

-- Reviews that count: not by the PR's own author and not by a bot.
qualifying_reviews as (
    select r.pr_key, r.submitted_at, r.state
    from {{ ref('stg_pr_reviews') }} as r
    inner join prs as p on r.pr_key = p.pr_key
    where not r.is_bot
      and r.reviewer_login is distinct from p.author_login
),

review_summary as (
    select
        pr_key,
        min(submitted_at) as first_review_at,
        bool_or(state = 'APPROVED') as has_approving_review,
        count(*) as review_count
    from qualifying_reviews
    group by pr_key
),

run as (
    select {{ run_time() }} as run_time
)

select
    p.repo,
    p.pr_number,
    p.pr_key,
    p.author_login,
    p.is_bot as is_bot_author,
    p.state,
    case
        when p.merged_at is not null then 'merged'
        when p.state = 'closed' then 'closed_unmerged'
        else 'open'
    end as pr_status,
    p.created_at,
    p.updated_at,
    p.closed_at,
    p.merged_at,
    p.merged_at is not null as is_merged,
    p.state = 'closed' and p.merged_at is null as is_closed_unmerged,
    p.state = 'open' as is_open,
    rs.first_review_at,
    date_diff('second', p.created_at, rs.first_review_at) / 3600.0 as hours_to_first_review,
    coalesce(date_diff('second', p.created_at, rs.first_review_at) / 3600.0 <= 168, false) as reviewed_within_7d,
    date_diff('second', p.created_at, p.merged_at) / 3600.0 as hours_to_merge,
    case when p.state = 'closed' then date_diff('second', p.created_at, p.closed_at) / 3600.0 end as hours_to_close,
    p.state = 'open' and p.updated_at < run.run_time - interval 180 day as is_stale,
    coalesce(rs.has_approving_review, false) as has_approving_review,
    coalesce(rs.review_count, 0) as review_count,
    p.created_at <= run.run_time - interval 7 day as is_older_than_7d,
    run.run_time
from prs as p
left join review_summary as rs on p.pr_key = rs.pr_key
cross join run
