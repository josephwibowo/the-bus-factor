/* @bruin

name: mart.market_positioning
type: duckdb.sql
description: |
  Market positioning table (`/positioning` page). Sourced from the
  `seed.market_positioning` fixture; always manually curated.
tags:
  - dialect:duckdb
  - layer:mart
  - domain:positioning

materialization:
  type: table

depends:
  - int.snapshot
  - seed.market_positioning

columns:
  - name: snapshot_week
    type: DATE
    checks:
      - name: not_null
  - name: row_order
    type: INTEGER
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: category
    type: VARCHAR
    checks:
      - name: not_null
  - name: example_products
    type: VARCHAR
    checks:
      - name: not_null
  - name: primary_question_answered
    type: VARCHAR
    checks:
      - name: not_null
  - name: relationship_to_bus_factor
    type: VARCHAR
    checks:
      - name: not_null

@bruin */

WITH snap AS (SELECT snapshot_week FROM int.snapshot)
SELECT
    s.snapshot_week,
    CAST(p.row_order AS INTEGER) AS row_order,
    p.category,
    p.example_products,
    p.primary_question_answered,
    p.relationship_to_bus_factor
FROM seed.market_positioning p
CROSS JOIN snap s
