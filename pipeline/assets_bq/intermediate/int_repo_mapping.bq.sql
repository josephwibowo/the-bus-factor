/* @bruin

name: int.repo_mapping
type: bq.sql

description: |
  Per-package repository-mapping-confidence points. Points come directly
  from pipeline/config/scoring.yml mapping_confidence.points. Monorepo
  repositories (>= monorepo_sibling_manifests_threshold manifests) cap
  confidence at the medium bucket so siblings cannot inherit a high-
  confidence mapping.

  Bucket rules (see scoring.yml):
    high   : points >= 70
    medium : 40 <= points < 70
    low    : points <  40  (unmappable for ranking purposes)

materialization:
  type: table

depends:
  - stg.npm_registry
  - stg.pypi_registry
  - stg.deps_dev
  - stg.github_repos
  - stg.scorecard

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
  - name: repository_url
    type: varchar
    description: Cleaned lowercase repository URL, null when unmappable.
  - name: mapping_points
    type: integer
    description: Additive points (pre-cap).
    checks:
      - name: non_negative
  - name: effective_points
    type: integer
    description: Points after monorepo/cap rules applied.
    checks:
      - name: non_negative
  - name: mapping_bucket
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [high, medium, low]
  - name: mapping_rationale
    type: varchar
    description: Human-readable comma-separated list of point contributions.
    checks:
      - name: not_null
  - name: is_monorepo
    type: boolean
    checks:
      - name: not_null

@bruin */

WITH packages AS (
    SELECT
        ecosystem,
        package_name,
        repository_url_clean,
        homepage_url_clean,
        publisher
    FROM stg.npm_registry
    UNION ALL
    SELECT
        ecosystem,
        package_name,
        repository_url_clean,
        homepage_url_clean,
        publisher
    FROM stg.pypi_registry
),
deps_dev_matches AS (
    SELECT
        ecosystem,
        package_name,
        source_repo_url_clean
    FROM stg.deps_dev
),
scored AS (
    SELECT
        p.ecosystem,
        p.package_name,
        p.repository_url_clean AS repository_url,
        p.homepage_url_clean,
        p.publisher,
        gh.manifest_count,
        COALESCE(gh.manifest_count, 0) >= 5 AS is_monorepo,
        gh.owner_login,
        dd.source_repo_url_clean,
        sc.aggregate_score AS scorecard_aggregate,
        CASE
            WHEN p.repository_url_clean IS NOT NULL AND gh.repo_url IS NOT NULL THEN 40
            ELSE 0
        END AS pt_registry_resolves,
        CASE
            WHEN p.homepage_url_clean IS NOT NULL
                AND p.repository_url_clean IS NOT NULL
                AND (
                    p.homepage_url_clean = p.repository_url_clean
                    OR p.homepage_url_clean LIKE p.repository_url_clean || '%'
                )
            THEN 10
            ELSE 0
        END AS pt_homepage_match,
        CASE
            WHEN gh.repo_url IS NOT NULL AND COALESCE(gh.manifest_count, 0) > 0 THEN 30
            ELSE 0
        END AS pt_manifest_match,
        CASE
            WHEN dd.source_repo_url_clean IS NOT NULL
                AND dd.source_repo_url_clean = p.repository_url_clean
            THEN 20
            ELSE 0
        END AS pt_deps_dev_match,
        CASE
            WHEN sc.repo_url IS NOT NULL THEN 10
            ELSE 0
        END AS pt_scorecard_present,
        CASE
            WHEN gh.owner_login IS NOT NULL AND p.publisher IS NOT NULL
                AND (
                    LOWER(gh.owner_login) = LOWER(p.publisher)
                    OR LOWER(p.publisher) LIKE '%' || LOWER(gh.owner_login) || '%'
                    OR LOWER(gh.owner_login) LIKE '%' || LOWER(p.publisher) || '%'
                )
            THEN 10
            ELSE 0
        END AS pt_owner_publisher_match
    FROM packages p
    LEFT JOIN stg.github_repos gh
        ON p.repository_url_clean = gh.repo_url
    LEFT JOIN deps_dev_matches dd
        ON p.ecosystem = dd.ecosystem AND p.package_name = dd.package_name
    LEFT JOIN stg.scorecard sc
        ON p.repository_url_clean = sc.repo_url
),
rolled AS (
    SELECT
        ecosystem,
        package_name,
        repository_url,
        is_monorepo,
        (
            pt_registry_resolves
            + pt_homepage_match
            + pt_manifest_match
            + pt_deps_dev_match
            + pt_scorecard_present
            + pt_owner_publisher_match
        ) AS mapping_points,
        pt_registry_resolves,
        pt_homepage_match,
        pt_manifest_match,
        pt_deps_dev_match,
        pt_scorecard_present,
        pt_owner_publisher_match
    FROM scored
)
SELECT
    r.ecosystem,
    r.package_name,
    r.repository_url,
    r.mapping_points,
    CASE
        WHEN r.is_monorepo AND r.mapping_points >= 70 THEN 69
        ELSE r.mapping_points
    END AS effective_points,
    CASE
        WHEN (CASE WHEN r.is_monorepo AND r.mapping_points >= 70 THEN 69 ELSE r.mapping_points END) >= 70 THEN 'high'
        WHEN (CASE WHEN r.is_monorepo AND r.mapping_points >= 70 THEN 69 ELSE r.mapping_points END) >= 40 THEN 'medium'
        ELSE 'low'
    END AS mapping_bucket,
    TRIM(BOTH ', ' FROM CONCAT(
        CASE WHEN r.pt_registry_resolves > 0 THEN ', registry_repo_url_resolves(+40)' ELSE '' END,
        CASE WHEN r.pt_homepage_match > 0 THEN ', homepage_resolves_same_repo(+10)' ELSE '' END,
        CASE WHEN r.pt_manifest_match > 0 THEN ', repo_manifest_matches_package(+30)' ELSE '' END,
        CASE WHEN r.pt_deps_dev_match > 0 THEN ', deps_dev_source_repo_match(+20)' ELSE '' END,
        CASE WHEN r.pt_scorecard_present > 0 THEN ', openssf_scorecard_present(+10)' ELSE '' END,
        CASE WHEN r.pt_owner_publisher_match > 0 THEN ', repo_owner_matches_publisher(+10)' ELSE '' END,
        CASE WHEN r.is_monorepo THEN ', monorepo_cap_applied(medium_max)' ELSE '' END
    )) AS mapping_rationale,
    r.is_monorepo
FROM rolled r
