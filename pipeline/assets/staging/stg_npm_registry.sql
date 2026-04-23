/* @bruin

name: stg.npm_registry
type: duckdb.sql

description: |
  Typed, cleaned npm registry snapshot. Normalizes repository URLs, flags
  stub-types names, and carries forward fields used by scoring and mapping.

materialization:
  type: table

depends:
  - raw.npm_registry

tags:
  - dialect:duckdb
  - layer:staging
  - source:npm

columns:
  - name: ecosystem
    type: varchar
    description: Always 'npm' for this asset.
    checks:
      - name: not_null
      - name: accepted_values
        value: [npm]
  - name: package_name
    type: varchar
    description: Canonical npm package name.
    checks:
      - name: not_null
      - name: unique
  - name: latest_version
    type: varchar
    checks:
      - name: not_null
  - name: first_release_date
    type: date
    checks:
      - name: not_null
  - name: latest_release_date
    type: date
    checks:
      - name: not_null
  - name: homepage_url_clean
    type: varchar
  - name: repository_url_clean
    type: varchar
    description: Lowercased https-canonicalized repository URL; null when unmappable.
  - name: is_deprecated
    type: boolean
    checks:
      - name: not_null
  - name: is_archived
    type: boolean
    checks:
      - name: not_null
  - name: publisher
    type: varchar
  - name: downloads_90d
    type: bigint
    checks:
      - name: non_negative
  - name: is_stub_types
    type: boolean
    description: True when package name matches the npm stub-types naming rule (@types/*).
    checks:
      - name: not_null

@bruin */

SELECT
    'npm' AS ecosystem,
    LOWER(TRIM(package_name)) AS package_name,
    latest_version,
    first_release_date,
    latest_release_date,
    NULLIF(TRIM(homepage_url), '') AS homepage_url_clean,
    CASE
        WHEN NULLIF(TRIM(repository_url), '') IS NULL THEN NULL
        ELSE LOWER(TRIM(repository_url))
    END AS repository_url_clean,
    is_deprecated,
    is_archived,
    publisher,
    downloads_90d,
    CASE
        WHEN LOWER(TRIM(package_name)) LIKE '@types/%' THEN TRUE
        ELSE FALSE
    END AS is_stub_types
FROM raw.npm_registry
