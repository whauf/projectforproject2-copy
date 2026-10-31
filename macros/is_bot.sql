{#
    Bot rule, applied to PR authors, reviewers, and commit authors:
        is_bot = user.type = 'Bot' OR user.login LIKE '%[bot]'
    A missing account (e.g. a commit whose email is not linked to a GitHub user) is not a bot.
#}
{% macro is_bot(login, account_type) -%}
    coalesce({{ account_type }} = 'Bot' or {{ login }} like '%[bot]', false)
{%- endmacro %}
