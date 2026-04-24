/* @bruin

name: mart.package_evidence
type: bq.sql
description: |
  Long-form fragility evidence: one row per (package, signal) pair for the
  seven fragility components. Feeds the package detail page and the weekly
  card. The `evidence` column is a short human-readable phrase summarising
  the underlying measurement (e.g. "468 days since last release").
tags:
  - dialect:bigquery
  - layer:mart
  - domain:evidence

materialization:
  type: table
  partition_by: snapshot_week
  cluster_by: [ecosystem, package_name]

depends:
  - int.snapshot
  - int.fragility_inputs
  - stg.github_commits
  - stg.github_releases
  - stg.github_issues
  - stg.github_contributors
  - stg.scorecard
  - int.repo_mapping
  - int.eligibility

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
  - name: signal_name
    type: VARCHAR
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - release_recency
          - commit_recency
          - release_cadence_decay
          - issue_responsiveness
          - all_time_contribution_concentration
          - recent_commit_concentration_365d
          - openssf_scorecard
  - name: contribution
    type: DOUBLE
    description: Per-signal 0-100 contribution before applying weights.
    checks:
      - name: non_negative
  - name: evidence
    type: VARCHAR
    description: Short human-readable summary of the measurement.
    checks:
      - name: not_null
  - name: snapshot_week
    type: DATE
    checks:
      - name: not_null

custom_checks:
  - name: exactly_seven_signals_per_package
    description: Every package in the long-form table must expose all seven fragility signals.
    value: 0
    query: |
      SELECT COUNT(*) FROM (
          SELECT ecosystem, package_name, COUNT(DISTINCT signal_name) AS c
          FROM mart.package_evidence
          GROUP BY ecosystem, package_name
      ) t
      WHERE c != 7
  - name: contributions_within_bounds
    description: Each signal contribution must be in [0, 100].
    value: 0
    query: |-
      SELECT COUNT(*) FROM mart.package_evidence
      WHERE contribution < 0 OR contribution > 100

@bruin */

WITH snap AS (SELECT snapshot_week FROM int.snapshot),
base AS (
    SELECT
        f.ecosystem,
        f.package_name,
        s.snapshot_week,
        f.days_since_release,
        f.days_since_commit,
        f.release_recency,
        f.commit_recency,
        f.release_cadence_decay,
        f.issue_responsiveness,
        f.all_time_contribution_concentration,
        f.recent_commit_concentration_365d,
        f.openssf_scorecard,
        m.repository_url,
        rel.releases_last_365d,
        rel.releases_prior_365d,
        iss.issues_opened_last_180d,
        iss.median_time_to_first_response_days,
        co.top_contributor_share_all_time,
        co.contributors_all_time,
        c.top_contributor_share_365d,
        c.unique_contributors_last_365d,
        sc.aggregate_score
    FROM int.fragility_inputs f
    CROSS JOIN snap s
    LEFT JOIN int.repo_mapping m
        ON f.ecosystem = m.ecosystem AND f.package_name = m.package_name
    LEFT JOIN stg.github_commits c ON m.repository_url = c.repo_url
    LEFT JOIN stg.github_releases rel ON m.repository_url = rel.repo_url
    LEFT JOIN stg.github_issues iss ON m.repository_url = iss.repo_url
    LEFT JOIN stg.github_contributors co ON m.repository_url = co.repo_url
    LEFT JOIN stg.scorecard sc ON m.repository_url = sc.repo_url
)
SELECT ecosystem, package_name, 'release_recency' AS signal_name,
    release_recency AS contribution,
    CASE
        WHEN days_since_release IS NULL THEN 'No release history available.'
        ELSE CAST(days_since_release AS STRING) || ' days since latest release.'
    END AS evidence,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'commit_recency',
    commit_recency,
    CASE
        WHEN days_since_commit IS NULL THEN 'No commit history available.'
        ELSE CAST(days_since_commit AS STRING) || ' days since last commit on default branch.'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'release_cadence_decay',
    release_cadence_decay,
    CASE
        WHEN releases_prior_365d IS NULL OR releases_prior_365d < 2
            THEN 'Insufficient prior-year release history to compute cadence.'
        ELSE CAST(COALESCE(releases_last_365d, 0) AS STRING) || ' releases in the last 365 days vs '
             || CAST(releases_prior_365d AS STRING) || ' in the prior 365 days.'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'issue_responsiveness',
    issue_responsiveness,
    CASE
        WHEN issues_opened_last_180d IS NULL OR issues_opened_last_180d < 5
            THEN 'Fewer than 5 eligible issues in the last 180 days; signal skipped.'
        WHEN median_time_to_first_response_days IS NULL
            THEN 'Median first maintainer response unavailable on '
                 || CAST(issues_opened_last_180d AS STRING)
                 || ' issues.'
        ELSE 'Median first maintainer response: '
             || REGEXP_REPLACE(
                 CAST(ROUND(median_time_to_first_response_days, 1) AS STRING),
                 r'\.0$',
                 ''
             )
             || ' days on '
             || CAST(issues_opened_last_180d AS STRING)
             || ' issues.'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'all_time_contribution_concentration',
    all_time_contribution_concentration,
    CASE
        WHEN top_contributor_share_all_time IS NULL
            THEN 'Contributor concentration unavailable.'
        ELSE 'Top contributor authored '
             || CAST(ROUND(top_contributor_share_all_time * 100.0, 1) AS STRING)
             || '% of all-time commits (distinct contributors: '
             || CAST(COALESCE(contributors_all_time, 0) AS STRING) || ').'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'recent_commit_concentration_365d',
    recent_commit_concentration_365d,
    CASE
        WHEN top_contributor_share_365d IS NULL
            THEN 'Recent commit concentration unavailable for the last 365 days.'
        ELSE 'Top contributor authored '
             || CAST(ROUND(top_contributor_share_365d * 100.0, 1) AS STRING)
             || '% of commits over the last 365 days (unique contributors: '
             || CAST(COALESCE(unique_contributors_last_365d, 0) AS STRING) || ').'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'openssf_scorecard',
    openssf_scorecard,
    CASE
        WHEN aggregate_score IS NULL
            THEN 'OpenSSF Scorecard aggregate unavailable.'
        ELSE 'OpenSSF Scorecard aggregate score: '
             || CAST(ROUND(aggregate_score, 1) AS STRING) || ' / 10.'
    END,
    snapshot_week
FROM base
