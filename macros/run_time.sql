{#
    "Run time" is the newest fetched_at across the raw tables, so rebuilding the models
    without new data reproduces the same results.
#}
{% macro run_time() -%}
    (select max(fetched_at) from (
        select fetched_at from {{ source('raw', 'raw_commits') }}
        union all select fetched_at from {{ source('raw', 'raw_pull_requests') }}
        union all select fetched_at from {{ source('raw', 'raw_pr_reviews') }}
        union all select fetched_at from {{ source('raw', 'raw_releases') }}
    ))::timestamptz
{%- endmacro %}

{% macro window_start() -%}
    '{{ var("window_start") }}'::timestamptz
{%- endmacro %}
