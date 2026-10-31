-- One row per published release on or after the window start. The lag runs over every release
-- before the window filter, so the first release in the window still measures its gap to the
-- release before it.
with all_releases as (
    select
        *,
        date_diff(
            'day',
            lag(published_at) over (partition by repo order by published_at),
            published_at
        ) as days_since_previous
    from {{ ref('stg_releases') }}
    where published_at is not null
)

select
    repo,
    release_id,
    release_key,
    tag_name,
    release_name,
    published_at,
    prerelease as is_prerelease,
    days_since_previous
from all_releases
where published_at >= {{ window_start() }}
