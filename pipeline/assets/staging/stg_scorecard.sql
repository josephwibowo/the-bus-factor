/* @bruin

name: stg.scorecard
type: duckdb.sql

description: |
  Typed OpenSSF Scorecard aggregate per repository URL.

materialization:
  type: table

depends:
  - raw.scorecard

tags:
  - dialect:duckdb
  - layer:staging
  - source:scorecard

columns:
  - name: repo_url
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: aggregate_score
    type: double
    checks:
      - name: non_negative
  - name: check_count
    type: bigint
    checks:
      - name: non_negative
  - name: scorecard_date
    type: date
    checks:
      - name: not_null

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    aggregate_score,
    check_count,
    scorecard_date
FROM raw.scorecard
