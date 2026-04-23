/* @bruin

name: stg.github_releases
type: bq.sql

description: |
  Typed per-repo release counts over the last 365 days and the prior 365
  days window. Powers release_cadence_decay fragility input.

materialization:
  type: table

depends:
  - raw.github_releases

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
  - name: releases_last_365d
    type: bigint
    checks:
      - name: non_negative
  - name: releases_prior_365d
    type: bigint
    checks:
      - name: non_negative
  - name: latest_release_tag
    type: varchar
  - name: latest_release_date
    type: date

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    releases_last_365d,
    releases_prior_365d,
    latest_release_tag,
    CAST(latest_release_date AS DATE) AS latest_release_date
FROM raw.github_releases
