/* @bruin

name: stg.deps_dev
type: duckdb.sql

description: |
  Typed deps.dev signals keyed by (ecosystem, package_name). Normalizes the
  source_repo URL and preserves null when deps.dev did not mint a mapping.

materialization:
  type: table

depends:
  - raw.deps_dev

tags:
  - dialect:duckdb
  - layer:staging
  - source:deps_dev

columns:
  - name: ecosystem
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [npm, pypi]
  - name: package_name
    type: varchar
    checks:
      - name: not_null
  - name: dependent_count
    type: bigint
    checks:
      - name: non_negative
  - name: source_repo_url_clean
    type: varchar
    description: Lowercased source repository URL reported by deps.dev, null when missing.
  - name: transitive_vuln_count
    type: bigint
    checks:
      - name: non_negative

@bruin */

SELECT
    LOWER(TRIM(ecosystem)) AS ecosystem,
    CASE
        WHEN LOWER(TRIM(ecosystem)) = 'pypi'
            THEN LOWER(REGEXP_REPLACE(TRIM(package_name), '[-_.]+', '-', 'g'))
        ELSE LOWER(TRIM(package_name))
    END AS package_name,
    dependent_count,
    CASE
        WHEN NULLIF(TRIM(source_repo_url), '') IS NULL THEN NULL
        ELSE LOWER(TRIM(source_repo_url))
    END AS source_repo_url_clean,
    transitive_vuln_count
FROM raw.deps_dev
