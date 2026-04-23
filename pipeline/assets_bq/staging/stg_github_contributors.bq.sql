/* @bruin

name: stg.github_contributors
type: bq.sql

description: |
  Typed per-repo contributor concentration over the last 365 days. Top-1
  contributor share drives the contributor_bus_factor fragility input.

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
  - name: top_contributor_share_365d
    type: double
    checks:
      - name: non_negative
  - name: contributors_last_365d
    type: bigint
    checks:
      - name: non_negative

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    top_contributor_share_365d,
    contributors_last_365d
FROM raw.github_contributors
