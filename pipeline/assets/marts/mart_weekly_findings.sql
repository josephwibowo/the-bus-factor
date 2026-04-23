/* @bruin

name: mart.weekly_findings
type: duckdb.sql
description: |
  Top findings for the weekly share card and the `/weekly` page. Selects
  up to five flagged packages ordered by risk_score, interleaving npm and
  pypi so no ecosystem dominates. When zero packages are flagged we keep
  the mart empty and let the export layer substitute the fallback copy.
tags:
  - dialect:duckdb
  - layer:mart
  - domain:weekly

materialization:
  type: table

depends:
  - int.snapshot
  - mart.package_scores
  - mart.packages_current
  - mart.package_evidence

columns:
  - name: snapshot_week
    type: DATE
    checks:
      - name: not_null
  - name: rank
    type: INTEGER
    checks:
      - name: not_null
      - name: positive
  - name: ecosystem
    type: VARCHAR
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - npm
          - pypi
  - name: package_name
    type: VARCHAR
    checks:
      - name: not_null
  - name: slug
    type: VARCHAR
    checks:
      - name: not_null
  - name: severity_tier
    type: VARCHAR
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - High
          - Critical
  - name: risk_score
    type: DOUBLE
    checks:
      - name: non_negative
  - name: primary_finding
    type: VARCHAR
    checks:
      - name: not_null

custom_checks:
  - name: at_most_five_findings
    description: The weekly card honours the up-to-5 rule regardless of flagged volume.
    value: 0
    query: |
      SELECT CASE WHEN COUNT(*) > 5 THEN 1 ELSE 0 END
      FROM mart.weekly_findings
  - name: ranks_are_sequential
    description: Ranks are 1..N without gaps.
    value: 0
    query: |-
      SELECT COUNT(*) FROM (
          SELECT rank, ROW_NUMBER() OVER (ORDER BY rank) AS rn
          FROM mart.weekly_findings
      ) t
      WHERE rank != rn

@bruin */

WITH flagged AS (
    SELECT
        s.snapshot_week,
        s.ecosystem,
        s.package_name,
        s.severity_tier,
        s.risk_score
    FROM mart.package_scores s
    WHERE s.flagged
),
top_signal AS (
    SELECT
        e.ecosystem,
        e.package_name,
        e.signal_name,
        e.contribution,
        e.evidence,
        ROW_NUMBER() OVER (
            PARTITION BY e.ecosystem, e.package_name
            ORDER BY e.contribution DESC
        ) AS rn
    FROM mart.package_evidence e
),
interleaved AS (
    SELECT
        f.snapshot_week,
        f.ecosystem,
        f.package_name,
        f.severity_tier,
        f.risk_score,
        ts.evidence AS primary_finding,
        ROW_NUMBER() OVER (PARTITION BY f.ecosystem ORDER BY f.risk_score DESC)
            AS rank_in_ecosystem
    FROM flagged f
    LEFT JOIN top_signal ts
        ON f.ecosystem = ts.ecosystem
        AND f.package_name = ts.package_name
        AND ts.rn = 1
),
ordered AS (
    SELECT
        i.snapshot_week,
        i.ecosystem,
        i.package_name,
        i.severity_tier,
        i.risk_score,
        i.primary_finding,
        i.rank_in_ecosystem,
        pc.slug,
        ROW_NUMBER() OVER (
            ORDER BY i.rank_in_ecosystem, i.risk_score DESC, i.ecosystem
        ) AS rank
    FROM interleaved i
    LEFT JOIN mart.packages_current pc
        ON i.ecosystem = pc.ecosystem AND i.package_name = pc.package_name
)
SELECT
    snapshot_week,
    CAST(rank AS INTEGER) AS rank,
    ecosystem,
    package_name,
    slug,
    severity_tier,
    ROUND(risk_score, 4) AS risk_score,
    primary_finding
FROM ordered
WHERE rank <= 5
