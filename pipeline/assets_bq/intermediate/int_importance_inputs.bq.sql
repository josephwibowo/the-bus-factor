/* @bruin

name: int.importance_inputs
type: bq.sql

description: |
  Per-package log-scaled inputs for the three importance components.
  Ecosystem percentile ranks are deferred to mart.package_scores so that
  they are computed over the eligible universe only (excluding stub types,
  archived, too-new, and unmappable packages).

materialization:
  type: table

depends:
  - stg.npm_registry
  - stg.pypi_registry
  - stg.deps_dev
  - stg.osv

tags:
  - dialect:bigquery
  - layer:intermediate

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
  - name: downloads_90d
    type: bigint
    checks:
      - name: non_negative
  - name: dependent_count
    type: bigint
    checks:
      - name: non_negative
  - name: security_exposure_count
    type: bigint
    description: Sum of direct OSV count and transitive vuln count from deps.dev.
    checks:
      - name: non_negative
  - name: log_download_volume
    type: double
    checks:
      - name: non_negative
  - name: log_dependency_reach
    type: double
    checks:
      - name: non_negative
  - name: log_security_exposure
    type: double
    checks:
      - name: non_negative

custom_checks:
  - name: unknown_dependency_reach_stays_null
    description: Missing dependency reach must remain null rather than being coerced to zero.
    value: 0
    query: |
      SELECT COUNT(*)
      FROM int.importance_inputs ii
      INNER JOIN stg.deps_dev d
          ON ii.ecosystem = d.ecosystem AND ii.package_name = d.package_name
      WHERE d.dependent_count IS NULL
        AND (ii.dependent_count IS NOT NULL OR ii.log_dependency_reach IS NOT NULL)

@bruin */

WITH packages AS (
    SELECT ecosystem, package_name, downloads_90d
    FROM stg.npm_registry
    UNION ALL
    SELECT ecosystem, package_name, downloads_90d
    FROM stg.pypi_registry
),
deps AS (
    SELECT ecosystem, package_name, dependent_count, transitive_vuln_count
    FROM stg.deps_dev
),
osv AS (
    SELECT ecosystem, package_name, direct_vuln_count
    FROM stg.osv
)
SELECT
    p.ecosystem,
    p.package_name,
    p.downloads_90d,
    d.dependent_count AS dependent_count,
    COALESCE(d.transitive_vuln_count, 0) + COALESCE(o.direct_vuln_count, 0)
        AS security_exposure_count,
    LN(1.0 + GREATEST(p.downloads_90d, 0)) AS log_download_volume,
    CASE
        WHEN d.dependent_count IS NULL THEN NULL
        ELSE LN(1.0 + GREATEST(d.dependent_count, 0))
    END AS log_dependency_reach,
    LN(1.0
        + COALESCE(d.transitive_vuln_count, 0)
        + COALESCE(o.direct_vuln_count, 0)
    ) AS log_security_exposure
FROM packages p
LEFT JOIN deps d
    ON p.ecosystem = d.ecosystem AND p.package_name = d.package_name
LEFT JOIN osv o
    ON p.ecosystem = o.ecosystem AND p.package_name = o.package_name
