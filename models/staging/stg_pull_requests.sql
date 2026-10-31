-- One row per pull request (latest fetch wins), with typed timestamps and the bot flag.
with latest as (
    select *
    from {{ source('raw', 'raw_pull_requests') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id as pr_number,
    repo || '#' || id::varchar as pr_key,
    payload ->> '$.user.login' as author_login,
    payload ->> '$.user.type' as author_type,
    {{ is_bot("payload ->> '$.user.login'", "payload ->> '$.user.type'") }} as is_bot,
    -- REST reports open/closed; GraphQL reports OPEN/CLOSED/MERGED (merged = closed in REST terms).
    case lower(payload ->> '$.state') when 'merged' then 'closed' else lower(payload ->> '$.state') end as state,
    coalesce((payload ->> '$.draft')::boolean, false) as is_draft,
    payload ->> '$.author_association' as author_association,
    (payload ->> '$.created_at')::timestamptz as created_at,
    (payload ->> '$.updated_at')::timestamptz as updated_at,
    (payload ->> '$.closed_at')::timestamptz as closed_at,
    (payload ->> '$.merged_at')::timestamptz as merged_at,
    fetched_at
from latest
