/* @bruin

name: mart.source_health
type: duckdb.sql

description: |
  Ingestion-source status table consumed by the `/sources` page and the
  metadata bundle. Reads from stg.source_health, which in fixture mode
  mirrors the committed CSV and in live mode drains per-asset telemetry
  buffered via pipeline/lib/sources.py.

materialization:
  type: table

depends:
  - int.snapshot
  - stg.source_health

tags:
  - dialect:duckdb
  - layer:mart
  - domain:health

columns:
  - name: snapshot_week
    type: date
    checks:
      - name: not_null
  - name: source_name
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: source_category
    type: varchar
    checks:
      - name: not_null
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
  - name: latency_ms
    type: double
  - name: row_count
    type: bigint
  - name: note
    type: varchar

@bruin */

WITH snap AS (SELECT snapshot_week FROM int.snapshot)
SELECT
    s.snapshot_week,
    h.source_name,
    h.source_category,
    h.status,
    h.last_success_at,
    h.stale,
    h.failure_count,
    h.latency_ms,
    h.row_count,
    h.note
FROM stg.source_health h
CROSS JOIN snap s
