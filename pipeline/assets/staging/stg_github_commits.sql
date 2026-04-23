/* @bruin

name: stg.github_commits
type: duckdb.sql

description: |
  Typed per-repo commit activity. Repository URLs are lower-cased so they
  join cleanly against other staging assets.

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
    checks:
      - name: not_null
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
