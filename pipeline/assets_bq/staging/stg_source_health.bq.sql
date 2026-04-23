/* @bruin

name: stg.source_health
type: bq.sql

description: |
  Typed source-health table unioned from raw.source_health. Adds the
  `source_category` column the /sources page uses to group rows.

materialization:
  type: table

depends:
  - raw.source_health

tags:
  - dialect:bigquery
  - layer:staging
  - source:meta

columns:
  - name: source_name
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: source_category
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [registry, repo, dependency_graph, security, repo_posture]
  - name: status
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [ok, degraded, failed]
  - name: last_success_at
    type: timestamp
  - name: stale
    type: boolean
    checks:
      - name: not_null
  - name: failure_count
    type: integer
    checks:
      - name: non_negative
  - name: note
    type: varchar
  - name: latency_ms
    type: double
  - name: row_count
    type: bigint

@bruin */

SELECT
    source_name,
    CASE
        WHEN source_name IN ('npm_registry', 'pypi_registry') THEN 'registry'
        WHEN source_name IN (
            'github_api', 'github_repos', 'github_commits', 'github_releases',
            'github_issues', 'github_contributors'
        ) THEN 'repo'
        WHEN source_name = 'deps_dev' THEN 'dependency_graph'
        WHEN source_name = 'osv' THEN 'security'
        WHEN source_name = 'openssf_scorecard' THEN 'repo_posture'
        ELSE 'registry'
    END AS source_category,
    status,
    CAST(last_success_at AS TIMESTAMP) AS last_success_at,
    stale,
    CAST(failure_count AS INTEGER) AS failure_count,
    note,
    CAST(latency_ms AS FLOAT64) AS latency_ms,
    CAST(row_count AS BIGINT) AS row_count
FROM raw.source_health
