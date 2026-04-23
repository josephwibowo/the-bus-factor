/* @bruin

name: mart.coverage_summary
type: duckdb.sql

description: |
  Per-ecosystem totals used by the methodology page and the README status
  badge. Tracks tracked / eligible / flagged counts and exclusions split
  by reason.

materialization:
  type: table

depends:
  - int.snapshot
  - mart.packages_current
  - mart.package_scores

tags:
  - dialect:duckdb
  - layer:mart
  - domain:coverage

columns:
  - name: snapshot_week
    type: date
    checks:
      - name: not_null
  - name: methodology_version
    type: varchar
    checks:
      - name: not_null
  - name: ecosystem
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [npm, pypi]
  - name: tracked
    type: integer
    checks:
      - name: non_negative
  - name: eligible
    type: integer
    checks:
      - name: non_negative
  - name: flagged
    type: integer
    checks:
      - name: non_negative
  - name: excluded_unmappable
    type: integer
    checks:
      - name: non_negative
  - name: excluded_archived
    type: integer
    checks:
      - name: non_negative
  - name: excluded_too_new
    type: integer
    checks:
      - name: non_negative
  - name: excluded_stub_types
    type: integer
    checks:
      - name: non_negative

@bruin */

WITH packages AS (
    SELECT
        snapshot_week,
        methodology_version,
        ecosystem,
        is_eligible,
        exclusion_reason
    FROM mart.packages_current
),
scored AS (
    SELECT ecosystem, flagged FROM mart.package_scores
),
ecosystem_counts AS (
    SELECT
        p.snapshot_week,
        p.methodology_version,
        p.ecosystem,
        COUNT(*) AS tracked,
        COUNT(*) FILTER (WHERE p.is_eligible) AS eligible,
        COUNT(*) FILTER (WHERE p.exclusion_reason = 'unmappable') AS excluded_unmappable,
        COUNT(*) FILTER (WHERE p.exclusion_reason = 'archived_deprecated') AS excluded_archived,
        COUNT(*) FILTER (WHERE p.exclusion_reason = 'too_new') AS excluded_too_new,
        COUNT(*) FILTER (WHERE p.exclusion_reason = 'stub_types') AS excluded_stub_types
    FROM packages p
    GROUP BY 1, 2, 3
),
flagged_counts AS (
    SELECT ecosystem, COUNT(*) AS flagged FROM scored WHERE flagged GROUP BY 1
)
SELECT
    e.snapshot_week,
    e.methodology_version,
    e.ecosystem,
    CAST(e.tracked AS INTEGER) AS tracked,
    CAST(e.eligible AS INTEGER) AS eligible,
    CAST(COALESCE(f.flagged, 0) AS INTEGER) AS flagged,
    CAST(e.excluded_unmappable AS INTEGER) AS excluded_unmappable,
    CAST(e.excluded_archived AS INTEGER) AS excluded_archived,
    CAST(e.excluded_too_new AS INTEGER) AS excluded_too_new,
    CAST(e.excluded_stub_types AS INTEGER) AS excluded_stub_types
FROM ecosystem_counts e
LEFT JOIN flagged_counts f USING (ecosystem)
