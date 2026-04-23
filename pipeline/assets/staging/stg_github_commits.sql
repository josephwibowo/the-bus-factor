/* @bruin

name: stg.github_commits
type: duckdb.sql

description: |
  Typed per-repo commit activity. Repository URLs are lower-cased so they
  join cleanly against other staging assets.

  ``last_commit_date`` may be null for real repos where the GitHub API
  returned no commits on the default branch (empty repo, archived
  fork, rate-limited 403, or missing default branch). Per the
  missing-data-reduces-confidence rule in AGENTS.md, the not-null check
  is intentionally omitted here; ``int.fragility_inputs`` maps null
  ``last_commit_date`` to a neutral activity signal and the coverage
  gap is reported in ``mart.source_health``.

materialization:
  type: table

depends:
  - raw.github_commits

tags:
  - dialect:duckdb
  - layer:staging
  - source:github

columns:
  - name: repo_url
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: last_commit_date
    type: date
  - name: commits_last_365d
    type: bigint
    checks:
      - name: non_negative

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    last_commit_date,
    commits_last_365d
FROM raw.github_commits
