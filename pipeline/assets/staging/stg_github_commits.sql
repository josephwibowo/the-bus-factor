/* @bruin

name: stg.github_commits
type: duckdb.sql

description: |
  Typed per-repo commit activity. Repository URLs are lower-cased so they
  join cleanly against other staging assets.

  ``last_commit_date`` may be null for real repos where the GitHub API
  returned no commits on the default branch (empty repo, archived
  fork, rate-limited 403, or missing default branch). Per the
  missing-data-reduces-confidence rule in AGENTS.md, the not-null check
  is intentionally omitted here; ``int.fragility_inputs`` maps null
  ``last_commit_date`` to a neutral activity signal and the coverage
  gap is reported in ``mart.source_health``.

materialization:
  type: table

depends:
  - raw.github_commits

tags:
  - dialect:duckdb
  - layer:staging
  - source:github

columns:
  - name: repo_url
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: last_commit_date
    type: date
  - name: commits_last_365d
    type: bigint
    checks:
      - name: non_negative
  - name: top_contributor_share_365d
    type: double
    checks:
      - name: non_negative
  - name: unique_contributors_last_365d
    type: bigint
    checks:
      - name: non_negative

@bruin */

WITH source AS (
    SELECT * FROM raw.github_commits
    UNION ALL BY NAME
    SELECT
        CAST(NULL AS VARCHAR) AS repo_url,
        CAST(NULL AS DATE) AS last_commit_date,
        CAST(NULL AS BIGINT) AS commits_last_365d,
        CAST(NULL AS DOUBLE) AS top_contributor_share_365d,
        CAST(NULL AS BIGINT) AS unique_contributors_last_365d,
        CAST(NULL AS TIMESTAMP) AS ingested_at
    WHERE FALSE
)

SELECT
    LOWER(TRIM(repo_url)) AS repo_url,
    CAST(last_commit_date AS DATE) AS last_commit_date,
    COALESCE(commits_last_365d, 0) AS commits_last_365d,
    top_contributor_share_365d,
    COALESCE(unique_contributors_last_365d, 0) AS unique_contributors_last_365d
FROM source
