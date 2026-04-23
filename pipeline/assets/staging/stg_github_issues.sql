/* @bruin

name: stg.github_issues
type: duckdb.sql

description: |
  Typed per-repo issue responsiveness (180d window). Repositories with fewer
  than the configured minimum eligible issues contribute zero fragility for
  this signal downstream.

materialization:
  type: table

depends:
  - raw.github_issues

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
  - name: issues_opened_last_180d
    type: bigint
    checks:
      - name: non_negative
  - name: median_time_to_first_response_days
    type: double
    checks:
      - name: non_negative

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    issues_opened_last_180d,
    median_time_to_first_response_days
FROM raw.github_issues
