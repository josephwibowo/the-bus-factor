/* @bruin

name: mart.source_health
type: duckdb.sql
description: |
  Ingestion-source status table consumed by the `/sources` page and the
  metadata bundle. Reads from stg.source_health, which in fixture mode
  mirrors the committed CSV and in live mode drains per-asset telemetry
  buffered via pipeline/lib/sources.py.
tags:
  - dialect:duckdb
  - layer:mart
  - domain:health

materialization:
  type: table

depends:
  - int.snapshot
  - stg.source_health

columns:
  - name: snapshot_week
    type: DATE
    checks:
      - name: not_null
  - name: source_name
    type: VARCHAR
    checks:
      - name: not_null
      - name: unique
  - name: source_category
    type: VARCHAR
    checks:
      - name: not_null
  - name: status
    type: VARCHAR
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - ok
          - degraded
          - failed
  - name: last_success_at
    type: TIMESTAMP
  - name: stale
    type: BOOLEAN
    checks:
      - name: not_null
  - name: failure_count
    type: INTEGER
    checks:
      - name: non_negative
  - name: latency_ms
    type: DOUBLE
  - name: row_count
    type: BIGINT
  - name: note
    type: VARCHAR

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
