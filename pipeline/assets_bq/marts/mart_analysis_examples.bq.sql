/* @bruin

name: mart.analysis_examples
type: bq.sql

description: |
  Analysis gallery rows used by the `/analysis` page. Carries the AI Data
  Analyst prompts, answer summaries, and screenshot references. Populated
  from the checked-in `seed.analysis_gallery` fixture; the canonical
  screenshot files live under `analysis/screenshots/` and
  `web/public/screenshots/`.

materialization:
  type: table
  partition_by: snapshot_week
  cluster_by: [example_id]

depends:
  - int.snapshot
  - seed.analysis_gallery

tags:
  - dialect:bigquery
  - layer:mart
  - domain:analysis

columns:
  - name: snapshot_week
    type: date
    checks:
      - name: not_null
  - name: example_id
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: prompt
    type: varchar
    checks:
      - name: not_null
  - name: answer_summary
    type: varchar
    checks:
      - name: not_null
  - name: screenshot_path
    type: varchar
    checks:
      - name: not_null
  - name: dataset_version
    type: varchar
    checks:
      - name: not_null
  - name: methodology_version
    type: varchar
    checks:
      - name: not_null
  - name: capture_date
    type: date
    checks:
      - name: not_null
  - name: capture_source
    type: varchar
    checks:
      - name: not_null

@bruin */

WITH snap AS (SELECT snapshot_week, methodology_version FROM int.snapshot)
SELECT
    s.snapshot_week,
    g.example_id,
    g.prompt,
    g.answer_summary,
    g.screenshot_path,
    g.dataset_version,
    s.methodology_version,
    g.capture_date,
    g.capture_source
FROM seed.analysis_gallery g
CROSS JOIN snap s
