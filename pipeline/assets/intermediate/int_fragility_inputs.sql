/* @bruin

name: int.fragility_inputs
type: duckdb.sql

description: |
  Per-package fragility components, each 0-100, mirroring
  pipeline/lib/scoring.py. Thresholds MUST stay in sync with
  pipeline/config/scoring.yml; the Python mirror is the unit-test partner.

  Components:
    release_recency         : linear 90-540 days since latest release
    commit_recency          : linear 30-365 days since last commit
    release_cadence_decay   : 100 * (1 - last/prior) if prior >= 2
    issue_responsiveness    : linear 7-90 days on median response, gated by
                              issues_opened_last_180d >= min_eligible_issues
                              (see scoring.yml; currently 5 in v0.2.0,
                              was 10 in v0.1.0)
    contributor_bus_factor  : linear 0.30-0.90 on top-1 contributor share
    openssf_scorecard       : 100 - (score * 10), clamped 0-100

materialization:
  type: table

depends:
  - int.snapshot
  - stg.npm_registry
  - stg.pypi_registry
  - int.repo_mapping
  - stg.github_commits
  - stg.github_releases
  - stg.github_issues
  - stg.github_contributors
  - stg.scorecard

tags:
  - dialect:duckdb
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
  - name: days_since_release
    type: integer
  - name: days_since_commit
    type: integer
  - name: release_recency
    type: double
    checks:
      - name: non_negative
  - name: commit_recency
    type: double
    checks:
      - name: non_negative
  - name: release_cadence_decay
    type: double
    checks:
      - name: non_negative
  - name: issue_responsiveness
    type: double
    checks:
      - name: non_negative
  - name: contributor_bus_factor
    type: double
    checks:
      - name: non_negative
  - name: openssf_scorecard
    type: double
    checks:
      - name: non_negative
  - name: fragility_score
    type: double
    description: |
      Weighted sum of the six components using fragility.weights from
      scoring.yml (0.25 / 0.25 / 0.15 / 0.15 / 0.10 / 0.10).
    checks:
      - name: non_negative

@bruin */

WITH universe AS (
    SELECT ecosystem, package_name, latest_release_date
    FROM stg.npm_registry
    UNION ALL
    SELECT ecosystem, package_name, latest_release_date
    FROM stg.pypi_registry
),
snap AS (
    SELECT snapshot_week FROM int.snapshot
),
joined AS (
    SELECT
        u.ecosystem,
        u.package_name,
        s.snapshot_week,
        u.latest_release_date,
        m.repository_url,
        c.last_commit_date,
        r.releases_last_365d,
        r.releases_prior_365d,
        i.issues_opened_last_180d,
        i.median_time_to_first_response_days,
        co.top_contributor_share_365d,
        sc.aggregate_score
    FROM universe u
    CROSS JOIN snap s
    LEFT JOIN int.repo_mapping m
        ON u.ecosystem = m.ecosystem AND u.package_name = m.package_name
    LEFT JOIN stg.github_commits c
        ON m.repository_url = c.repo_url
    LEFT JOIN stg.github_releases r
        ON m.repository_url = r.repo_url
    LEFT JOIN stg.github_issues i
        ON m.repository_url = i.repo_url
    LEFT JOIN stg.github_contributors co
        ON m.repository_url = co.repo_url
    LEFT JOIN stg.scorecard sc
        ON m.repository_url = sc.repo_url
),
components AS (
    SELECT
        j.ecosystem,
        j.package_name,
        j.snapshot_week,
        CAST(DATE_DIFF('day', j.latest_release_date, j.snapshot_week) AS INTEGER)
            AS days_since_release,
        CAST(DATE_DIFF('day', j.last_commit_date, j.snapshot_week) AS INTEGER)
            AS days_since_commit,

        -- release_recency: floor/cap from scoring config vars
        CASE
            WHEN j.latest_release_date IS NULL THEN 0
            WHEN DATE_DIFF('day', j.latest_release_date, j.snapshot_week)
                <= {{ var.threshold_release_recency_floor_days }} THEN 0
            WHEN DATE_DIFF('day', j.latest_release_date, j.snapshot_week)
                >= {{ var.threshold_release_recency_cap_days }} THEN 100
            ELSE (
                DATE_DIFF('day', j.latest_release_date, j.snapshot_week)
                - {{ var.threshold_release_recency_floor_days }}
            ) / (
                {{ var.threshold_release_recency_cap_days }}.0
                - {{ var.threshold_release_recency_floor_days }}.0
            ) * 100.0
        END AS release_recency,

        -- commit_recency: floor/cap from scoring config vars
        CASE
            WHEN j.last_commit_date IS NULL THEN 0
            WHEN DATE_DIFF('day', j.last_commit_date, j.snapshot_week)
                <= {{ var.threshold_commit_recency_floor_days }} THEN 0
            WHEN DATE_DIFF('day', j.last_commit_date, j.snapshot_week)
                >= {{ var.threshold_commit_recency_cap_days }} THEN 100
            ELSE (
                DATE_DIFF('day', j.last_commit_date, j.snapshot_week)
                - {{ var.threshold_commit_recency_floor_days }}
            ) / (
                {{ var.threshold_commit_recency_cap_days }}.0
                - {{ var.threshold_commit_recency_floor_days }}.0
            ) * 100.0
        END AS commit_recency,

        -- release_cadence_decay: 0 if prior below configured minimum
        CASE
            WHEN j.releases_prior_365d IS NULL
                 OR j.releases_prior_365d < {{ var.threshold_release_cadence_min_prior_releases }}
                THEN 0
            WHEN j.releases_last_365d >= j.releases_prior_365d THEN 0
            ELSE 100.0 * (1.0 - (j.releases_last_365d * 1.0 / j.releases_prior_365d))
        END AS release_cadence_decay,

        -- issue_responsiveness: configured min-issues + floor/cap windows.
        CASE
            WHEN j.issues_opened_last_180d IS NULL
                 OR j.issues_opened_last_180d < {{ var.threshold_issue_min_eligible_issues }}
                 OR j.median_time_to_first_response_days IS NULL
                THEN 0
            WHEN j.median_time_to_first_response_days <= {{ var.threshold_issue_floor_days }} THEN 0
            WHEN j.median_time_to_first_response_days >= {{ var.threshold_issue_cap_days }} THEN 100
            ELSE (
                j.median_time_to_first_response_days - {{ var.threshold_issue_floor_days }}.0
            ) / (
                {{ var.threshold_issue_cap_days }}.0 - {{ var.threshold_issue_floor_days }}.0
            ) * 100.0
        END AS issue_responsiveness,

        -- contributor_bus_factor: linear share floor/cap from scoring vars
        CASE
            WHEN j.top_contributor_share_365d IS NULL THEN 0
            WHEN j.top_contributor_share_365d <= {{ var.threshold_contributor_share_floor }} THEN 0
            WHEN j.top_contributor_share_365d >= {{ var.threshold_contributor_share_cap }} THEN 100
            ELSE (
                j.top_contributor_share_365d - {{ var.threshold_contributor_share_floor }}
            ) / (
                {{ var.threshold_contributor_share_cap }}
                - {{ var.threshold_contributor_share_floor }}
            ) * 100.0
        END AS contributor_bus_factor,

        -- openssf_scorecard: 100 - (score * configured scale)
        CASE
            WHEN j.aggregate_score IS NULL THEN 0
            ELSE GREATEST(
                0,
                LEAST(100, 100.0 - (j.aggregate_score * {{ var.threshold_scorecard_scale }}.0))
            )
        END AS openssf_scorecard
    FROM joined j
)
SELECT
    ecosystem,
    package_name,
    snapshot_week,
    days_since_release,
    days_since_commit,
    release_recency,
    commit_recency,
    release_cadence_decay,
    issue_responsiveness,
    contributor_bus_factor,
    openssf_scorecard,
    (
        {{ var.fragility_weight_release_recency }} * release_recency
        + {{ var.fragility_weight_commit_recency }} * commit_recency
        + {{ var.fragility_weight_release_cadence_decay }} * release_cadence_decay
        + {{ var.fragility_weight_issue_responsiveness }} * issue_responsiveness
        + {{ var.fragility_weight_contributor_bus_factor }} * contributor_bus_factor
        + {{ var.fragility_weight_openssf_scorecard }} * openssf_scorecard
    ) AS fragility_score
FROM components
