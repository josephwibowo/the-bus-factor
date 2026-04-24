/* @bruin

name: stg.scorecard
type: bq.sql

description: |
  Typed OpenSSF Scorecard aggregate per repository URL.

materialization:
  type: table

depends:
  - raw.scorecard

tags:
  - dialect:bigquery
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

custom_checks:
  - name: scored_rows_have_scorecard_date
    description: A populated Scorecard aggregate should include the upstream Scorecard run date.
    value: 0
    query: |
      SELECT COUNT(*) FROM stg.scorecard
      WHERE aggregate_score IS NOT NULL AND scorecard_date IS NULL

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    aggregate_score,
    check_count,
    scorecard_date
FROM raw.scorecard
