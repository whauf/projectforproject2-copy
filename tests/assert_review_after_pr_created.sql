-- No review may be submitted before its pull request was created.
-- Returns the offending reviews; the test passes when this returns no rows.
select r.repo, r.pr_number, r.review_id, r.submitted_at, p.created_at as pr_created_at
from {{ ref('stg_pr_reviews') }} as r
inner join {{ ref('stg_pull_requests') }} as p on r.pr_key = p.pr_key
where r.submitted_at < p.created_at
