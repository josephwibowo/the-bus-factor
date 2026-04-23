/* @bruin

name: mart.package_evidence
type: duckdb.sql

description: |
  Long-form fragility evidence: one row per (package, signal) pair for the
  six fragility components. Feeds the package detail page and the weekly
  card. The `evidence` column is a short human-readable phrase summarising
  the underlying measurement (e.g. "468 days since last release").

materialization:
  type: table

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

tags:
  - dialect:duckdb
  - layer:mart
  - domain:evidence

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
  - name: signal_name
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - release_recency
          - commit_recency
          - release_cadence_decay
          - issue_responsiveness
          - contributor_bus_factor
          - openssf_scorecard
  - name: contribution
    type: double
    description: Per-signal 0-100 contribution before applying weights.
    checks:
      - name: non_negative
  - name: evidence
    type: varchar
    description: Short human-readable summary of the measurement.
    checks:
      - name: not_null
  - name: snapshot_week
    type: date
    checks:
      - name: not_null

custom_checks:
  - name: exactly_six_signals_per_package
    description: Every package in the long-form table must expose all six fragility signals.
    query: |
      SELECT COUNT(*) FROM (
          SELECT ecosystem, package_name, COUNT(DISTINCT signal_name) AS c
          FROM mart.package_evidence
          GROUP BY ecosystem, package_name
      ) t
      WHERE c != 6

  - name: contributions_within_bounds
    description: Each signal contribution must be in [0, 100].
    query: |
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
        f.contributor_bus_factor,
        f.openssf_scorecard,
        m.repository_url,
        rel.releases_last_365d,
        rel.releases_prior_365d,
        iss.issues_opened_last_180d,
        iss.median_time_to_first_response_days,
        co.top_contributor_share_365d,
        co.contributors_last_365d,
        sc.aggregate_score
    FROM int.fragility_inputs f
    CROSS JOIN snap s
    LEFT JOIN int.repo_mapping m
        ON f.ecosystem = m.ecosystem AND f.package_name = m.package_name
    LEFT JOIN stg.github_releases rel ON m.repository_url = rel.repo_url
    LEFT JOIN stg.github_issues iss ON m.repository_url = iss.repo_url
    LEFT JOIN stg.github_contributors co ON m.repository_url = co.repo_url
    LEFT JOIN stg.scorecard sc ON m.repository_url = sc.repo_url
)
SELECT ecosystem, package_name, 'release_recency' AS signal_name,
    release_recency AS contribution,
    CASE
        WHEN days_since_release IS NULL THEN 'No release history available.'
        ELSE CAST(days_since_release AS VARCHAR) || ' days since latest release.'
    END AS evidence,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'commit_recency',
    commit_recency,
    CASE
        WHEN days_since_commit IS NULL THEN 'No commit history available.'
        ELSE CAST(days_since_commit AS VARCHAR) || ' days since last commit on default branch.'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'release_cadence_decay',
    release_cadence_decay,
    CASE
        WHEN releases_prior_365d IS NULL OR releases_prior_365d < 2
            THEN 'Insufficient prior-year release history to compute cadence.'
        ELSE CAST(COALESCE(releases_last_365d, 0) AS VARCHAR) || ' releases in the last 365 days vs '
             || CAST(releases_prior_365d AS VARCHAR) || ' in the prior 365 days.'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'issue_responsiveness',
    issue_responsiveness,
    CASE
        WHEN issues_opened_last_180d IS NULL OR issues_opened_last_180d < 5
            THEN 'Fewer than 5 eligible issues in the last 180 days; signal skipped.'
        ELSE 'Median first maintainer response: '
             || CAST(ROUND(median_time_to_first_response_days, 1) AS VARCHAR)
             || ' days on '
             || CAST(issues_opened_last_180d AS VARCHAR)
             || ' issues.'
    END,
    snapshot_week
FROM base
UNION ALL
SELECT ecosystem, package_name, 'contributor_bus_factor',
    contributor_bus_factor,
    CASE
        WHEN top_contributor_share_365d IS NULL
            THEN 'Contributor concentration unavailable.'
        ELSE 'Top contributor authored '
             || CAST(ROUND(top_contributor_share_365d * 100.0, 1) AS VARCHAR)
             || '% of commits over the last 365 days (distinct contributors: '
             || CAST(COALESCE(contributors_last_365d, 0) AS VARCHAR) || ').'
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
             || CAST(ROUND(aggregate_score, 1) AS VARCHAR) || ' / 10.'
    END,
    snapshot_week
FROM base
