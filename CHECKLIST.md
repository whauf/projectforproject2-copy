# Submission Checklist

Due: Nov 02, 2026. See the [assignment instructions](INSTRUCTIONS.md) for full grading criteria and the [README](README.md) for this project's write-up.

## Submit in Canvas

- [ ] Package set (A, B, or C): `A` (numpy/numpy, networkx/networkx, statsmodels/statsmodels)
- [ ] Repository URL: `https://github.com/whauf/projectforproject2`
- [ ] Set up your oral exam time slot 

## Repository setup

- [ ] Created from the template (not forked) and set to **Private** (to do: the repo is still public)
- [ ] `nmagee`, `elaineliu05`, and `tcusick8` added as collaborators (to do)
- [x] Dependencies pinned in `requirements.txt`, `Pipfile`, or `pyproject.toml`
- [x] `.gitignore` excludes all appropriate files and directories.
- [x] No database files, logs, raw API dumps, caches, or binaries committed
- [x] No GitHub token anywhere in the repository or its history

## The flow (`main.py`)

- [x] One `@flow` runs all six tasks in order and completes from a clean clone with only a GitHub token set
- [x] Token read from an environment variable or Prefect Secret block
- [x] Each task logs to the Prefect logger and to its own log file; dbt's log is saved
- [x] **Task 1 · Extract:** commits, pull requests, PR reviews, and releases for all three repos, written to `data/raw/` one file per page
- [x] **Task 2 · Load:** one `raw_*` table per endpoint, upserted on `(repo, id)`, plus `_load_state` and the `_LOADED` marker
- [x] **Task 3 · dbt:** `dbt build` runs inside the flow and fails it on any model or test failure
- [x] **Task 4 · Validation (Python, no dbt):** completeness checks, outlier detection, and descriptive statistics printed and logged
- [x] **Task 5 · Model:** ranked table with score, rank, and per-dimension subscores printed, logged, and written to the database
- [x] **Task 6 · Plot:** at least three PNGs saved to `plots/`

## dbt project

- [x] Staging models for all four raw tables, with `is_bot` flag
- [x] `fct_pull_requests`, `fct_releases`, `mart_repo_monthly`, `mart_health_inputs`
- [x] Raw tables declared as sources; `schema.yml` describes every mart and column
- [x] Generic tests: `unique`/`not_null` on primary keys, `relationships` from reviews to PRs, `accepted_values` on state columns
- [x] Singular tests: no PR closed or merged before creation; no review before its PR was created

## Plots (committed in `plots/`)

- [x] Dimension profile showing all three repos, ordered by final rank with rank and score labeled
- [x] Monthly trends: small multiples from `mart_repo_monthly`, one line per repo
- [x] Relationship plot encoding at least three variables
- [x] Titles, labeled axes with units, and a consistent color per repo across all plots

## Your README

- [x] Setup steps, run command, and expected runtime
- [x] Log excerpts from an interrupted run and its resumed run
- [x] Row counts from running Task 2 twice on the same files
- [x] Request counts for a full run and an incremental run
- [x] The bot rule you used
- [x] How medians treat PRs still open at run time
- [x] Outlier cutoffs, the records flagged, and whether you kept, capped, or dropped them, and why
- [x] Each health metric's definition and direction (higher or lower is healthier); at least four metrics across three dimensions
- [x] Your weights and the reasoning behind them
- [x] A one-paragraph interpretation of each plot

## Extended items (if attempted)

- [x] **GraphQL:** PR and review ingestion uses GraphQL with cursor pagination; README logs request counts and runtime for REST vs. GraphQL
- [x] **Concurrent fetching:** repos are fetched in parallel under a shared rate limiter; README says where this happens in the code
- [ ] **Scheduled deployment:** monthly Prefect deployment (the 28th); `DEPLOYMENTS.md` shows the deployment command and the log of one manually triggered run (code and command ready in `main.py --deploy`; registering it and the manual run need the SDS server login)
- [x] **API odometer:** total requests per flow run is logged, and your count is: 8,120 requests for a full REST run, 13 for an incremental run (298 for a full GraphQL run)

## After submission

- [ ] Attend your 5-minute oral interview, ready to explain any part of your code
