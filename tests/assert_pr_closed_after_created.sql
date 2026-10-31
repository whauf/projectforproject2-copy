-- No pull request may be closed or merged before it was created.
-- Returns the offending PRs; the test passes when this returns no rows.
select repo, pr_number, created_at, closed_at, merged_at
from {{ ref('stg_pull_requests') }}
where closed_at < created_at
   or merged_at < created_at
