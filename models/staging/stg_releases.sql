-- One row per published release (latest fetch wins). Draft releases are dropped.
with latest as (
    select *
    from {{ source('raw', 'raw_releases') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id as release_id,
    repo || ':' || id::varchar as release_key,
    payload ->> '$.tag_name' as tag_name,
    payload ->> '$.name' as release_name,
    (payload ->> '$.created_at')::timestamptz as created_at,
    (payload ->> '$.published_at')::timestamptz as published_at,
    coalesce((payload ->> '$.prerelease')::boolean, false) as prerelease,
    fetched_at
from latest
where not coalesce((payload ->> '$.draft')::boolean, false)
