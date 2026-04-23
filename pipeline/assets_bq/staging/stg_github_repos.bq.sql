/* @bruin

name: stg.github_repos
type: bq.sql

description: |
  Typed GitHub repository metadata keyed by canonical repo URL.
  Counts manifest files per repo so the mapping stage can enforce the
  monorepo-sibling cap on mapping confidence.

materialization:
  type: table

depends:
  - raw.github_repos

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
  - name: repo_id
    type: bigint
    checks:
      - name: not_null
  - name: default_branch
    type: varchar
    checks:
      - name: not_null
  - name: is_archived
    type: boolean
    checks:
      - name: not_null
  - name: is_disabled
    type: boolean
    checks:
      - name: not_null
  - name: stars
    type: bigint
    checks:
      - name: non_negative
  - name: primary_language
    type: varchar
  - name: manifest_paths
    type: varchar
  - name: manifest_count
    type: integer
    description: Number of ecosystem manifest files found in the repository.
    checks:
      - name: non_negative
  - name: owner_login
    type: varchar
    checks:
      - name: not_null

@bruin */

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    repo_id,
    default_branch,
    is_archived,
    is_disabled,
    stars,
    primary_language,
    manifest_paths,
    CASE
        WHEN NULLIF(TRIM(manifest_paths), '') IS NULL THEN 0
        ELSE 1 + LENGTH(manifest_paths) - LENGTH(REPLACE(manifest_paths, ';', ''))
    END AS manifest_count,
    LOWER(TRIM(owner_login)) AS owner_login
FROM raw.github_repos
