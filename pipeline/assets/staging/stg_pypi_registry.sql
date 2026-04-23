/* @bruin

name: stg.pypi_registry
type: duckdb.sql

description: |
  Typed, cleaned PyPI registry snapshot. Normalizes project names (PEP 503
  style), repository URLs, and flags PEP 561 stubs packages.

materialization:
  type: table

depends:
  - raw.pypi_registry

tags:
  - dialect:duckdb
  - layer:staging
  - source:pypi

columns:
  - name: ecosystem
    type: varchar
    checks:
      - name: not_null
      - name: accepted_values
        value: [pypi]
  - name: package_name
    type: varchar
    description: Lower-case PEP 503 normalized project name.
    checks:
      - name: not_null
      - name: unique
  - name: latest_version
    type: varchar
    checks:
      - name: not_null
  - name: first_release_date
    type: date
    checks:
      - name: not_null
  - name: latest_release_date
    type: date
    checks:
      - name: not_null
  - name: homepage_url_clean
    type: varchar
  - name: repository_url_clean
    type: varchar
  - name: is_deprecated
    type: boolean
    checks:
      - name: not_null
  - name: is_archived
    type: boolean
    checks:
      - name: not_null
  - name: publisher
    type: varchar
  - name: downloads_90d
    type: bigint
    checks:
      - name: non_negative
  - name: is_stub_types
    type: boolean
    description: True when project name matches PyPI stub conventions (types-* or *-stubs).
    checks:
      - name: not_null

@bruin */

SELECT
    'pypi' AS ecosystem,
    LOWER(REGEXP_REPLACE(TRIM(package_name), '[-_.]+', '-', 'g')) AS package_name,
    latest_version,
    CAST(first_release_date AS DATE) AS first_release_date,
    CAST(latest_release_date AS DATE) AS latest_release_date,
    NULLIF(TRIM(homepage_url), '') AS homepage_url_clean,
    CASE
        WHEN NULLIF(TRIM(repository_url), '') IS NULL THEN NULL
        WHEN REGEXP_EXTRACT(
            LOWER(TRIM(repository_url)) || '/',
            'github\.com[/:]([a-z0-9_.-]+?)/([a-z0-9_.-]+?)(\.git)?($|[/#?])',
            1
        ) = '' THEN NULL
        ELSE 'https://github.com/'
            || REGEXP_EXTRACT(
                LOWER(TRIM(repository_url)) || '/',
                'github\.com[/:]([a-z0-9_.-]+?)/([a-z0-9_.-]+?)(\.git)?($|[/#?])',
                1
            )
            || '/'
            || REGEXP_EXTRACT(
                LOWER(TRIM(repository_url)) || '/',
                'github\.com[/:]([a-z0-9_.-]+?)/([a-z0-9_.-]+?)(\.git)?($|[/#?])',
                2
            )
    END AS repository_url_clean,
    is_deprecated,
    is_archived,
    publisher,
    downloads_90d,
    CASE
        WHEN LOWER(TRIM(package_name)) LIKE 'types-%' THEN TRUE
        WHEN LOWER(TRIM(package_name)) LIKE '%-stubs' THEN TRUE
        ELSE FALSE
    END AS is_stub_types
FROM raw.pypi_registry
