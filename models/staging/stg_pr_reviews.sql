-- One row per submitted review (latest fetch wins). PENDING reviews are dropped: they have no submitted_at.
with latest as (
    select *
    from {{ source('raw', 'raw_pr_reviews') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
),

parsed as (
    select
        *,
        -- REST: pull_request_url ends in the PR number (.../repos/{owner}/{repo}/pulls/{number});
        -- GraphQL mode: the review carries pull_request.number instead.
        coalesce(
            regexp_extract(payload ->> '$.pull_request_url', '(\d+)$', 1),
            payload ->> '$.pull_request.number'
        ) as pr_number_text
    from latest
    where payload ->> '$.state' <> 'PENDING'
)

select
    repo,
    id as review_id,
    repo || ':' || id::varchar as review_key,
    pr_number_text::bigint as pr_number,
    repo || '#' || pr_number_text as pr_key,
    payload ->> '$.user.login' as reviewer_login,
    payload ->> '$.user.type' as reviewer_type,
    {{ is_bot("payload ->> '$.user.login'", "payload ->> '$.user.type'") }} as is_bot,
    payload ->> '$.state' as state,
    (payload ->> '$.submitted_at')::timestamptz as submitted_at,
    fetched_at
from parsed
