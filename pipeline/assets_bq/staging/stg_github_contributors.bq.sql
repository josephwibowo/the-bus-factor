/* @bruin

name: stg.github_contributors
type: bq.sql

description: |
  Typed per-repo all-time contributor concentration. Top-1 contributor
  share drives the all_time_contribution_concentration fragility input.

materialization:
  type: table

depends:
  - raw.github_contributors

tags:
  - dialect:bigquery
  - layer:staging
  - source:github

columns:
  - name: repo_url
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: top_contributor_share_all_time
    type: double
    checks:
      - name: non_negative
  - name: contributors_all_time
    type: bigint
    checks:
      - name: non_negative

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    top_contributor_share_all_time,
    contributors_all_time
FROM raw.github_contributors
