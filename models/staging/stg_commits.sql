-- One row per commit (latest fetch wins). committed_at is the author date: when the work was done.
with latest as (
    select *
    from {{ source('raw', 'raw_commits') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id as sha,
    repo || ':' || id as commit_key,
    payload ->> '$.author.login' as author_login,
    payload ->> '$.author.type' as author_type,
    {{ is_bot("payload ->> '$.author.login'", "payload ->> '$.author.type'") }} as is_bot,
    (payload ->> '$.commit.author.date')::timestamptz as committed_at,
    (payload ->> '$.commit.committer.date')::timestamptz as committer_date,
    json_array_length(payload -> '$.parents') > 1 as is_merge_commit,
    fetched_at
from latest
