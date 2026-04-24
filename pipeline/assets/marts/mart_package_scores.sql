/* @bruin

name: mart.package_scores
type: duckdb.sql
description: |
  One row per ELIGIBLE package with importance, fragility, and risk scores
  plus severity tier, confidence band, and the flagged decision. Percentile
  ranks are computed over the eligible subset within each ecosystem so
  excluded packages never skew the leaderboard.

  Flagged criteria (scoring.yml `flagged`):
    * risk_score >= 30
    * severity_tier in (High, Critical)
    * confidence in (medium, high)
    * >= 2 independent fragility signals >= 40, and at least one of them
      is NOT release_recency (release_recency is paired_signal_only)
    * importance percentile >= top_quantile gate (75 for quantile 0.25)
tags:
  - dialect:duckdb
  - layer:mart
  - domain:scores

materialization:
  type: table

depends:
  - int.snapshot
  - int.eligibility
  - int.importance_inputs
  - int.fragility_inputs
  - int.repo_mapping
  - stg.source_health

columns:
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
  - name: snapshot_week
    type: DATE
    checks:
      - name: not_null
  - name: methodology_version
    type: VARCHAR
    checks:
      - name: not_null
  - name: importance_score
    type: DOUBLE
    description: 0-100, weighted sum of three log-percentile components over the eligible ecosystem.
    checks:
      - name: non_negative
  - name: fragility_score
    type: DOUBLE
    checks:
      - name: non_negative
  - name: risk_score
    type: DOUBLE
    description: (importance_score * fragility_score) / 100, clamped 0-100.
    checks:
      - name: non_negative
  - name: severity_tier
    type: VARCHAR
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Stable
          - Watch
          - Elevated
          - High
          - Critical
  - name: flagged
    type: BOOLEAN
    checks:
      - name: not_null
  - name: confidence
    type: VARCHAR
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - low
          - medium
          - high
  - name: importance_rank_within_ecosystem
    type: INTEGER
    description: Dense rank by risk_score within each ecosystem (1 = highest risk).
    checks:
      - name: not_null
      - name: positive
  - name: importance_percentile_within_eligible
    type: DOUBLE
    description: 0-100, higher = more important within the ecosystem's eligible set.
    checks:
      - name: non_negative
  - name: signals_above_threshold
    type: INTEGER
    checks:
      - name: non_negative
  - name: non_paired_signals_above_threshold
    type: INTEGER
    checks:
      - name: non_negative

custom_checks:
  - name: no_flagged_low_confidence
    description: Flagged packages must have medium or high confidence (scoring.yml).
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE flagged AND confidence = 'low'
  - name: flagged_has_two_independent_signals
    description: Every flagged package must expose at least two fragility signals >= 40.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE flagged AND signals_above_threshold < {{ var.flagged_min_independent_fragility_signals }}
  - name: flagged_has_non_paired_signal
    description: release_recency alone cannot flag a package (paired_signal_only).
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE flagged AND non_paired_signals_above_threshold < 1
  - name: flagged_risk_at_or_above_min
    description: Flagged packages must meet the configured risk_score minimum.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE flagged AND risk_score < {{ var.flagged_risk_score_min }}
  - name: flagged_in_top_quantile
    description: Flagged packages must be in the top 25% importance within their ecosystem.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE flagged AND importance_percentile_within_eligible
          < (1.0 - {{ var.flagged_importance_top_quantile }}) * 100.0
  - name: scores_within_bounds
    description: importance/fragility/risk must stay in [0, 100].
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE importance_score < 0 OR importance_score > 100
         OR fragility_score < 0 OR fragility_score > 100
         OR risk_score < 0 OR risk_score > 100
  - name: methodology_version_present
    description: Every row must carry the methodology version for audit traceability.
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores
      WHERE methodology_version IS NULL OR methodology_version = ''
  - name: snapshot_week_present
    description: Every row must carry the snapshot week anchor.
    value: 0
    query: SELECT COUNT(*) FROM mart.package_scores WHERE snapshot_week IS NULL
  - name: flagged_packages_are_eligible
    description: Flagged packages must be eligible (never archived / unmappable / too_new / stub_types).
    value: 0
    query: |
      SELECT COUNT(*) FROM mart.package_scores s
      INNER JOIN mart.packages_current p
          ON s.ecosystem = p.ecosystem AND s.package_name = p.package_name
      WHERE s.flagged AND (p.is_eligible = FALSE OR p.exclusion_reason IS NOT NULL)
  - name: known_state_agreement_flagged
    description: |
      Every package listed in known_states with expected_flagged=true must be
      flagged by the pipeline (and vice versa). Drift breaks the run.
    value: 0
    query: |
      SELECT COUNT(*)
      FROM seed.known_states k
      LEFT JOIN mart.package_scores s
          ON k.ecosystem = s.ecosystem AND k.package_name = s.package_name
      WHERE '{{ var.source_mode }}' = 'fixture'
        AND k.expected_flagged IS NOT NULL
        AND k.expected_flagged <> COALESCE(s.flagged, FALSE)
  - name: known_state_agreement_state
    description: |
      Every package listed in known_states must land in the expected exclusion /
      eligibility bucket reported by mart.packages_current.
    value: 0
    query: |-
      SELECT COUNT(*)
      FROM seed.known_states k
      LEFT JOIN mart.packages_current p
          ON k.ecosystem = p.ecosystem AND k.package_name = p.package_name
      WHERE '{{ var.source_mode }}' = 'fixture'
        AND (
            p.ecosystem IS NULL
            OR (k.expected_state = 'flagged' AND COALESCE(p.is_eligible, FALSE) = FALSE)
            OR (k.expected_state = 'eligible_not_flagged' AND COALESCE(p.is_eligible, FALSE) = FALSE)
            OR (k.expected_state = 'excluded_unmappable' AND COALESCE(p.exclusion_reason, '') != 'unmappable')
            OR (k.expected_state = 'excluded_archived' AND COALESCE(p.exclusion_reason, '') != 'archived_deprecated')
            OR (k.expected_state = 'excluded_too_new' AND COALESCE(p.exclusion_reason, '') != 'too_new')
            OR (k.expected_state = 'excluded_stub_types' AND COALESCE(p.exclusion_reason, '') != 'stub_types')
        )
  - name: live_mode_min_scored_per_ecosystem
    description: |
      In live mode, each ecosystem must retain at least 50 scored packages.
      A smaller cohort almost always means a repo-mapping regression upstream
      (stg.*_registry.repository_url_clean no longer joining to stg.github_repos).
    value: 0
    query: |
      WITH required AS (
          SELECT 'npm' AS ecosystem
          UNION ALL
          SELECT 'pypi' AS ecosystem
      ),
      scored_counts AS (
          SELECT ecosystem, COUNT(*) AS n
          FROM mart.package_scores
          GROUP BY ecosystem
      )
      SELECT COUNT(*)
      FROM required r
      LEFT JOIN scored_counts c USING (ecosystem)
      WHERE '{{ var.source_mode }}' = 'live' AND COALESCE(c.n, 0) < 50
  - name: live_mode_sources_are_healthy
    description: Live public snapshots must not publish with stale, failed, degraded, or empty critical source rows.
    value: 0
    query: |
      SELECT COUNT(*)
      FROM stg.source_health
      WHERE '{{ var.source_mode }}' = 'live'
        AND source_name IN ('npm_registry', 'pypi_registry', 'deps_dev', 'github_repos')
        AND (
            status != 'ok'
            OR stale = TRUE
            OR failure_count > 0
            OR COALESCE(row_count, 0) = 0
        )

@bruin */

WITH snap AS (
    SELECT snapshot_week, methodology_version FROM int.snapshot
),
eligible AS (
    SELECT ecosystem, package_name, is_reduced_confidence_age
    FROM int.eligibility
    WHERE is_eligible
),
dep_pct AS (
    SELECT
        ii.ecosystem,
        ii.package_name,
        PERCENT_RANK() OVER (
            PARTITION BY ii.ecosystem ORDER BY ii.log_dependency_reach
        ) * 100.0 AS pct_dependency_reach
    FROM int.importance_inputs ii
    INNER JOIN eligible USING (ecosystem, package_name)
    WHERE ii.log_dependency_reach IS NOT NULL
),
download_pct AS (
    SELECT
        ii.ecosystem,
        ii.package_name,
        PERCENT_RANK() OVER (
            PARTITION BY ii.ecosystem ORDER BY ii.log_download_volume
        ) * 100.0 AS pct_download_volume
    FROM int.importance_inputs ii
    INNER JOIN eligible USING (ecosystem, package_name)
    WHERE ii.log_download_volume IS NOT NULL
),
security_pct AS (
    SELECT
        ii.ecosystem,
        ii.package_name,
        PERCENT_RANK() OVER (
            PARTITION BY ii.ecosystem ORDER BY ii.log_security_exposure
        ) * 100.0 AS pct_security_exposure
    FROM int.importance_inputs ii
    INNER JOIN eligible USING (ecosystem, package_name)
    WHERE ii.log_security_exposure IS NOT NULL
),
importance_pct AS (
    SELECT
        e.ecosystem,
        e.package_name,
        d.pct_dependency_reach,
        dl.pct_download_volume,
        s.pct_security_exposure
    FROM eligible e
    LEFT JOIN dep_pct d USING (ecosystem, package_name)
    LEFT JOIN download_pct dl USING (ecosystem, package_name)
    LEFT JOIN security_pct s USING (ecosystem, package_name)
),
source_health_rollup AS (
    SELECT
        CASE
            WHEN '{{ var.source_mode }}' != 'live' THEN 0
            ELSE SUM(CASE
                WHEN status != 'ok'
                     OR stale = TRUE
                     OR failure_count > 0
                     OR COALESCE(row_count, 0) = 0
                    THEN 1
                ELSE 0
            END)
        END AS unhealthy_sources,
        CASE
            WHEN '{{ var.source_mode }}' != 'live' THEN 0
            ELSE SUM(CASE
                WHEN source_name IN ('npm_registry', 'pypi_registry', 'deps_dev', 'github_repos')
                     AND (
                        status != 'ok'
                        OR stale = TRUE
                        OR failure_count > 0
                        OR COALESCE(row_count, 0) = 0
                     )
                    THEN 1
                ELSE 0
            END)
        END AS critical_unhealthy_sources
    FROM stg.source_health
),
scored AS (
    SELECT
        i.ecosystem,
        i.package_name,
        COALESCE(
            (
                COALESCE({{ var.importance_weight_dependency_reach }} * i.pct_dependency_reach, 0)
                + COALESCE({{ var.importance_weight_download_volume }} * i.pct_download_volume, 0)
                + COALESCE({{ var.importance_weight_security_exposure }} * i.pct_security_exposure, 0)
            ) / NULLIF(
                (CASE WHEN i.pct_dependency_reach IS NOT NULL THEN {{ var.importance_weight_dependency_reach }} ELSE 0 END)
                + (CASE WHEN i.pct_download_volume IS NOT NULL THEN {{ var.importance_weight_download_volume }} ELSE 0 END)
                + (CASE WHEN i.pct_security_exposure IS NOT NULL THEN {{ var.importance_weight_security_exposure }} ELSE 0 END),
                0
            ),
            0
        ) AS importance_score,
        f.fragility_score,
        f.release_recency,
        f.commit_recency,
        f.release_cadence_decay,
        f.issue_responsiveness,
        f.contributor_bus_factor,
        f.openssf_scorecard,
        m.mapping_bucket,
        e.is_reduced_confidence_age,
        COALESCE(sh.unhealthy_sources, 0) AS unhealthy_sources,
        COALESCE(sh.critical_unhealthy_sources, 0) AS critical_unhealthy_sources
    FROM importance_pct i
    INNER JOIN int.fragility_inputs f USING (ecosystem, package_name)
    INNER JOIN int.repo_mapping m USING (ecosystem, package_name)
    INNER JOIN eligible e USING (ecosystem, package_name)
    CROSS JOIN source_health_rollup sh
),
with_risk AS (
    SELECT
        *,
        (importance_score * fragility_score) / 100.0 AS risk_score
    FROM scored
),
with_tier AS (
    SELECT
        *,
        CASE
            WHEN risk_score <= {{ var.severity_stable_max }} THEN 'Stable'
            WHEN risk_score <= {{ var.severity_watch_max }} THEN 'Watch'
            WHEN risk_score <= {{ var.severity_elevated_max }} THEN 'Elevated'
            WHEN risk_score <= {{ var.severity_high_max }} THEN 'High'
            ELSE 'Critical'
        END AS severity_tier,
        (
            CASE WHEN release_recency >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN commit_recency >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN release_cadence_decay >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN issue_responsiveness >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN contributor_bus_factor >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN openssf_scorecard >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
        ) AS signals_above_threshold,
        (
            CASE WHEN commit_recency >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN release_cadence_decay >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN issue_responsiveness >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN contributor_bus_factor >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
            + CASE WHEN openssf_scorecard >= {{ var.flagged_signal_contribution_threshold }} THEN 1 ELSE 0 END
        ) AS non_paired_signals_above_threshold
    FROM with_risk
),
with_confidence AS (
    SELECT
        *,
        CASE
            WHEN critical_unhealthy_sources > 0 OR unhealthy_sources > 1 THEN 'low'
            WHEN mapping_bucket = 'high'
                 AND signals_above_threshold >= 2
                 AND NOT is_reduced_confidence_age
                 AND unhealthy_sources = 0
                THEN 'high'
            WHEN mapping_bucket IN ('high', 'medium')
                 AND signals_above_threshold >= 1
                 AND critical_unhealthy_sources = 0
                 AND unhealthy_sources <= 1
                THEN 'medium'
            ELSE 'low'
        END AS confidence
    FROM with_tier
),
with_rank AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY ecosystem ORDER BY importance_score)
            * 100.0 AS importance_percentile_within_eligible,
        ROW_NUMBER() OVER (
            PARTITION BY ecosystem ORDER BY risk_score DESC, importance_score DESC
        ) AS importance_rank_within_ecosystem
    FROM with_confidence
),
flagged_decision AS (
    SELECT
        *,
        (
            severity_tier IN ('High', 'Critical')
            AND risk_score >= {{ var.flagged_risk_score_min }}
            AND confidence IN ('medium', 'high')
            AND signals_above_threshold >= {{ var.flagged_min_independent_fragility_signals }}
            AND non_paired_signals_above_threshold >= 1
            AND importance_percentile_within_eligible >= (
                (1.0 - {{ var.flagged_importance_top_quantile }}) * 100.0
            )
        ) AS flagged
    FROM with_rank
)
SELECT
    f.ecosystem,
    f.package_name,
    s.snapshot_week,
    s.methodology_version,
    ROUND(f.importance_score, 4) AS importance_score,
    ROUND(f.fragility_score, 4) AS fragility_score,
    ROUND(f.risk_score, 4) AS risk_score,
    f.severity_tier,
    f.flagged,
    f.confidence,
    CAST(f.importance_rank_within_ecosystem AS INTEGER) AS importance_rank_within_ecosystem,
    ROUND(f.importance_percentile_within_eligible, 4) AS importance_percentile_within_eligible,
    f.signals_above_threshold,
    f.non_paired_signals_above_threshold
FROM flagged_decision f
CROSS JOIN snap s
