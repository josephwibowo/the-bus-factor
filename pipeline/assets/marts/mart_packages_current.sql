/* @bruin

name: mart.packages_current
type: duckdb.sql
description: |
  One row per tracked package for the current snapshot. Combines registry
  metadata, mapping confidence, eligibility decision, and repo metadata.
  This mart is the canonical source for the `/package/[slug]` detail page
  and for any downstream export that needs non-score context.

  Data lineage:
    * registry/profile fields come from `stg.npm_registry` and `stg.pypi_registry`
    * repository attributes (`stars`, `primary_language`, `owner_login`) come
      from `stg.github_repos` through `int.repo_mapping`
    * eligibility flags and exclusion rationale come from `int.eligibility`
    * `snapshot_week` and `methodology_version` are run anchors from `int.snapshot`

  Operationally this is a weekly snapshot table rebuilt as a deterministic
  table each run. Row count scales with the tracked package universe for the
  run (bounded by configured npm/PyPI universe limits). The table is mutable
  across runs but append-like by week at the conceptual level; consumers
  should treat `(snapshot_week, ecosystem, package_name)` as the logical key.

  Nuances:
    * `is_archived` intentionally coalesces registry and GitHub archived/disabled
      signals, so it can differ from raw registry-only status.
    * `owner_login` is a repository owner identifier (user/org) and is not a
      maintainer identity assertion.
    * `age_months` is derived from release history and snapshot date; it is
      approximate because month length is normalized to 30.4375 days.
tags:
  - dialect:duckdb
  - layer:mart
  - domain:packages
  - data_type:dimension_table
  - sensitivity:public
  - cadence:weekly_snapshot
  - consumption:web_export_ai

materialization:
  type: table

depends:
  - int.snapshot
  - stg.npm_registry
  - stg.pypi_registry
  - stg.github_repos
  - int.repo_mapping
  - int.eligibility

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
    description: Canonical package identifier within its ecosystem (dimension key; high cardinality).
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
    description: Monday UTC snapshot anchor date for this pipeline run (timestamp dimension).
    checks:
      - name: not_null
  - name: methodology_version
    type: varchar
    description: Scoring/config version stamp used for reproducibility and auditability.
    checks:
      - name: not_null
  - name: first_release_date
    type: date
    description: Earliest known package release date from registry history (source timestamp).
    checks:
      - name: not_null
  - name: latest_release_date
    type: date
    description: Most recent known package release date from registry history (source timestamp).
    checks:
      - name: not_null
  - name: publisher
    type: varchar
    description: Registry-reported publisher/owner label; free-form text and not globally unique.
  - name: homepage_url
    type: varchar
    description: Canonicalized package homepage URL when present in registry metadata.
  - name: repository_url
    type: varchar
    description: Canonicalized GitHub repository URL used for cross-source joins; null when unmappable.
  - name: is_deprecated
    type: boolean
    description: Registry-level deprecation indicator (boolean status dimension).
    checks:
      - name: not_null
  - name: is_archived
    type: boolean
    description: True when either the registry flags it or GitHub reports archived/disabled.
    checks:
      - name: not_null
  - name: is_stub_types
    type: boolean
    description: True for type-stub packages (`@types/*`, `types-*`, `*-stubs`) excluded from ranking.
    checks:
      - name: not_null
  - name: mapping_points
    type: integer
    description: Additive repository-mapping confidence points from `int.repo_mapping` (metric; unit=points).
    checks:
      - name: non_negative
  - name: mapping_bucket
    type: varchar
    description: Mapping-confidence band (`high`, `medium`, `low`) used in confidence/eligibility logic.
    checks:
      - name: not_null
      - name: accepted_values
        value: [high, medium, low]
  - name: mapping_rationale
    type: varchar
    description: Human-readable list of mapping point contributors (derived diagnostic text).
  - name: is_eligible
    type: boolean
    description: True when the package passes exclusion gates and can be scored in `mart.package_scores`.
    checks:
      - name: not_null
  - name: exclusion_reason
    type: varchar
    description: Exclusion state when not eligible; null for eligible packages.
    checks:
      - name: accepted_values
        value: [stub_types, too_new, archived_deprecated, unmappable]
  - name: is_reduced_confidence_age
    type: boolean
    description: True when package age is 12-24 months and downstream confidence is reduced.
    checks:
      - name: not_null
  - name: age_months
    type: double
    description: Package age at snapshot time in months (derived metric; unit=months).
  - name: stars
    type: bigint
    description: GitHub stargazer count for mapped repository (engagement metric; nullable when unmapped).
  - name: primary_language
    type: varchar
    description: GitHub-reported primary repository language (dimension; nullable when unmapped).
  - name: owner_login
    type: varchar
    description: Lowercased GitHub repository owner login (identifier; user or organization).

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
  - name: release_dates_are_ordered
    description: First release date must be on or before latest release date.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.packages_current
      WHERE first_release_date > latest_release_date
  - name: slug_prefixed_by_ecosystem
    description: Slug must preserve the `ecosystem-` prefix contract used by web routing.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.packages_current
      WHERE slug NOT LIKE ecosystem || '-%'
  - name: eligibility_and_exclusion_reason_are_consistent
    description: Eligible rows must have null exclusion_reason; excluded rows must have one.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.packages_current
      WHERE (is_eligible = TRUE AND exclusion_reason IS NOT NULL)
         OR (is_eligible = FALSE AND exclusion_reason IS NULL)
  - name: age_months_non_negative_when_present
    description: Derived age in months should not be negative.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.packages_current
      WHERE age_months IS NOT NULL AND age_months < 0

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
        '/', '__'
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
