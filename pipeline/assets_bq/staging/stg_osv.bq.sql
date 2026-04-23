/* @bruin

name: stg.osv
type: bq.sql

description: |
  Typed OSV advisory counts per tracked package. Feeds into importance
  (security_exposure) after being joined with transitive counts from deps.dev.

materialization:
  type: table

depends:
  - raw.osv

tags:
  - dialect:bigquery
  - layer:staging
  - source:osv

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
  - name: direct_vuln_count
    type: bigint
    checks:
      - name: non_negative
  - name: highest_severity
    type: varchar
    checks:
      - name: accepted_values
        value: [LOW, MODERATE, HIGH, CRITICAL]

@bruin */

SELECT
    LOWER(TRIM(ecosystem)) AS ecosystem,
    CASE
        WHEN LOWER(TRIM(ecosystem)) = 'pypi'
            THEN LOWER(REGEXP_REPLACE(TRIM(package_name), '[-_.]+', '-'))
        ELSE LOWER(TRIM(package_name))
    END AS package_name,
    direct_vuln_count,
    UPPER(TRIM(highest_severity)) AS highest_severity
FROM raw.osv
