/* @bruin

name: int.snapshot
type: bq.sql

description: |
  Single-row anchor table carrying the snapshot Monday (UTC) and methodology
  version for the whole pipeline run. Every downstream asset cross-joins
  this table so the reporting window is stamped deterministically.

materialization:
  type: table

tags:
  - dialect:bigquery
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
    description: Injected via `var.methodology_version`; defaults should mirror scoring.yml.
    checks:
      - name: not_null

@bruin */

WITH anchor AS (
    SELECT
        {% if var.snapshot_week and var.snapshot_week != '' %}
        DATE '{{ var.snapshot_week }}'
        {% else %}
        CAST(DATE_TRUNC(CURRENT_DATE, ISOWEEK) AS DATE)
        {% endif %}
        AS snapshot_week
)
SELECT
    snapshot_week,
    CAST(EXTRACT(YEAR FROM snapshot_week) AS STRING)
        || '-W'
        || LPAD(CAST(EXTRACT(ISOWEEK FROM snapshot_week) AS STRING), 2, '0') AS snapshot_week_label,
    '{{ var.methodology_version }}' AS methodology_version
FROM anchor
