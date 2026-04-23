/* @bruin

name: int.snapshot
type: duckdb.sql

description: |
  Single-row anchor table carrying the snapshot Monday (UTC) and methodology
  version for the whole pipeline run. Every downstream asset cross-joins
  this table so the reporting window is stamped deterministically.

materialization:
  type: table

tags:
  - dialect:duckdb
  - layer:intermediate
  - anchor

columns:
  - name: snapshot_week
    type: date
    description: ISO Monday anchor for this run (UTC).
    checks:
      - name: not_null
      - name: unique
  - name: snapshot_week_label
    type: varchar
    description: YYYY-Www label derived from snapshot_week.
    checks:
      - name: not_null
  - name: methodology_version
    type: varchar
    description: Hard-coded to match scoring.yml. Bump whenever any weight changes.
    checks:
      - name: not_null

@bruin */

WITH anchor AS (
    SELECT
        {% if var.snapshot_week and var.snapshot_week != '' %}
        DATE '{{ var.snapshot_week }}'
        {% else %}
        CAST(DATE_TRUNC('week', CURRENT_DATE) AS DATE)
        {% endif %}
        AS snapshot_week
)
SELECT
    snapshot_week,
    CAST(EXTRACT(YEAR FROM snapshot_week) AS VARCHAR)
        || '-W'
        || LPAD(CAST(EXTRACT(WEEK FROM snapshot_week) AS VARCHAR), 2, '0') AS snapshot_week_label,
    'v0.1.0' AS methodology_version
FROM anchor
