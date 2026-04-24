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

WITH source AS (
    SELECT * FROM raw.github_issues
    UNION ALL BY NAME
    SELECT
        CAST(NULL AS VARCHAR) AS repo_url,
        CAST(NULL AS BIGINT) AS issues_opened_last_180d,
        CAST(NULL AS DOUBLE) AS median_time_to_first_response_days,
        CAST(NULL AS TIMESTAMP) AS ingested_at
    WHERE FALSE
)

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    COALESCE(issues_opened_last_180d, 0) AS issues_opened_last_180d,
    median_time_to_first_response_days
FROM source
