/* @bruin

name: mart.packages_current
type: duckdb.sql

description: |
  One row per tracked package for the current snapshot. Combines registry
  metadata, mapping confidence, eligibility decision, and repo metadata.
  This mart is the canonical source for the `/package/[slug]` detail page
  and for any downstream export that needs non-score context.

materialization:
  type: table

depends:
  - int.snapshot
  - stg.npm_registry
  - stg.pypi_registry
  - stg.github_repos
  - int.repo_mapping
  - int.eligibility

tags:
  - dialect:duckdb
  - layer:mart
  - domain:packages

columns:
  - name: ecosystem
    type: varchar
    description: One of npm / pypi.
    checks:
      - name: not_null
      - name: accepted_values
        value: [npm, pypi]
  - name: package_name
    type: varchar
    checks:
      - name: not_null
  - name: slug
    type: varchar
    description: URL-safe identifier used by the web tier (ecosystem-prefixed).
    checks:
      - name: not_null
      - name: unique
  - name: snapshot_week
    type: date
    checks:
      - name: not_null
  - name: methodology_version
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
  - name: publisher
    type: varchar
  - name: homepage_url
    type: varchar
  - name: repository_url
    type: varchar
  - name: is_deprecated
    type: boolean
    checks:
      - name: not_null
  - name: is_archived
    type: boolean
    description: True when either the registry flags it or GitHub reports archived/disabled.
    checks:
      - name: not_null
  - name: is_stub_types
    type: boolean
    checks:
      - name: not_null
  - name: mapping_points
    type: integer
    checks:
      - name: non_negative
  - name: mapping_bucket
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [high, medium, low]
  - name: mapping_rationale
    type: varchar
  - name: is_eligible
    type: boolean
    checks:
      - name: not_null
  - name: exclusion_reason
    type: varchar
    checks:
      - name: accepted_values
        value: [stub_types, too_new, archived_deprecated, unmappable]
  - name: is_reduced_confidence_age
    type: boolean
    checks:
      - name: not_null
  - name: age_months
    type: double
  - name: stars
    type: bigint
  - name: primary_language
    type: varchar
  - name: owner_login
    type: varchar

custom_checks:
  - name: no_duplicate_package_keys
    description: (ecosystem, package_name) must uniquely identify a row.
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT ecosystem || '|' || package_name)
      FROM mart.packages_current
  - name: snapshot_week_present_on_every_row
    description: Every row must carry the snapshot week anchor.
    query: SELECT COUNT(*) FROM mart.packages_current WHERE snapshot_week IS NULL
  - name: methodology_version_present_on_every_row
    description: Every row must carry the methodology version.
    query: |
      SELECT COUNT(*) FROM mart.packages_current
      WHERE methodology_version IS NULL OR methodology_version = ''

@bruin */

WITH registry AS (
    SELECT
        ecosystem,
        package_name,
        latest_version,
        first_release_date,
        latest_release_date,
        homepage_url_clean AS homepage_url,
        repository_url_clean AS repository_url,
        publisher,
        is_deprecated,
        is_archived AS registry_is_archived,
        is_stub_types,
        downloads_90d
    FROM stg.npm_registry
    UNION ALL
    SELECT
        ecosystem,
        package_name,
        latest_version,
        first_release_date,
        latest_release_date,
        homepage_url_clean AS homepage_url,
        repository_url_clean AS repository_url,
        publisher,
        is_deprecated,
        is_archived AS registry_is_archived,
        is_stub_types,
        downloads_90d
    FROM stg.pypi_registry
),
snap AS (SELECT snapshot_week, methodology_version FROM int.snapshot)
SELECT
    r.ecosystem,
    r.package_name,
    r.ecosystem || '-' || REGEXP_REPLACE(
        REGEXP_REPLACE(r.package_name, '^@', ''),
        '/', '__', 'g'
    ) AS slug,
    s.snapshot_week,
    s.methodology_version,
    r.first_release_date,
    r.latest_release_date,
    r.publisher,
    r.homepage_url,
    r.repository_url,
    r.is_deprecated,
    COALESCE(r.registry_is_archived, FALSE)
        OR COALESCE(gh.is_archived, FALSE)
        OR COALESCE(gh.is_disabled, FALSE) AS is_archived,
    r.is_stub_types,
    COALESCE(m.mapping_points, 0) AS mapping_points,
    COALESCE(m.mapping_bucket, 'low') AS mapping_bucket,
    m.mapping_rationale,
    e.is_eligible,
    e.exclusion_reason,
    e.is_reduced_confidence_age,
    e.age_months,
    gh.stars,
    gh.primary_language,
    gh.owner_login
FROM registry r
CROSS JOIN snap s
LEFT JOIN int.repo_mapping m
    ON r.ecosystem = m.ecosystem AND r.package_name = m.package_name
LEFT JOIN stg.github_repos gh
    ON m.repository_url = gh.repo_url
LEFT JOIN int.eligibility e
    ON r.ecosystem = e.ecosystem AND r.package_name = e.package_name
