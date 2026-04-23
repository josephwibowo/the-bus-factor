/* @bruin

name: int.eligibility
type: bq.sql

description: |
  Per-package eligibility decision. Exclusion states are evaluated in this
  order so one "winner" is assigned per package:
    1. stub_types         (from staging is_stub_types flag)
    2. too_new            (first_release_date within eligibility.min_age_months)
    3. archived_deprecated (is_archived / is_deprecated on registry OR GitHub)
    4. unmappable         (repository mapping bucket = low)

  Packages with no exclusion reason are marked eligible; those in the 12-24
  month band receive reduced-confidence treatment downstream.

materialization:
  type: table

depends:
  - int.snapshot
  - stg.npm_registry
  - stg.pypi_registry
  - stg.github_repos
  - int.repo_mapping

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
  - name: snapshot_week
    type: date
    checks:
      - name: not_null
  - name: age_months
    type: double
    checks:
      - name: non_negative
  - name: is_eligible
    type: boolean
    checks:
      - name: not_null
  - name: exclusion_reason
    type: varchar
    description: Null when eligible; otherwise one of the configured exclusion states.
    checks:
      - name: accepted_values
        value: [stub_types, too_new, archived_deprecated, unmappable]
  - name: is_reduced_confidence_age
    type: boolean
    description: True when the package is 12-24 months old (drops confidence band).
    checks:
      - name: not_null

@bruin */

WITH registry AS (
    SELECT
        ecosystem,
        package_name,
        first_release_date,
        is_deprecated,
        is_archived AS registry_is_archived,
        repository_url_clean,
        is_stub_types
    FROM stg.npm_registry
    UNION ALL
    SELECT
        ecosystem,
        package_name,
        first_release_date,
        is_deprecated,
        is_archived AS registry_is_archived,
        repository_url_clean,
        is_stub_types
    FROM stg.pypi_registry
),
snap AS (
    SELECT snapshot_week FROM int.snapshot
),
enriched AS (
    SELECT
        r.ecosystem,
        r.package_name,
        s.snapshot_week,
        DATE_DIFF(s.snapshot_week, r.first_release_date, DAY) / 30.4375 AS age_months,
        r.is_stub_types,
        r.is_deprecated,
        r.registry_is_archived,
        gh.is_archived AS github_is_archived,
        gh.is_disabled AS github_is_disabled,
        m.mapping_bucket,
        r.repository_url_clean
    FROM registry r
    CROSS JOIN snap s
    LEFT JOIN int.repo_mapping m
        ON r.ecosystem = m.ecosystem AND r.package_name = m.package_name
    LEFT JOIN stg.github_repos gh
        ON m.repository_url = gh.repo_url
),
classified AS (
    SELECT
        *,
        CASE
            WHEN is_stub_types THEN 'stub_types'
            WHEN age_months < 12 THEN 'too_new'
            WHEN is_deprecated = TRUE THEN 'archived_deprecated'
            WHEN registry_is_archived = TRUE THEN 'archived_deprecated'
            WHEN github_is_archived = TRUE THEN 'archived_deprecated'
            WHEN github_is_disabled = TRUE THEN 'archived_deprecated'
            WHEN mapping_bucket = 'low' OR mapping_bucket IS NULL THEN 'unmappable'
            ELSE NULL
        END AS exclusion_reason
    FROM enriched
)
SELECT
    ecosystem,
    package_name,
    snapshot_week,
    age_months,
    (exclusion_reason IS NULL) AS is_eligible,
    exclusion_reason,
    CASE
        WHEN age_months >= 12 AND age_months < 24 THEN TRUE
        ELSE FALSE
    END AS is_reduced_confidence_age
FROM classified
