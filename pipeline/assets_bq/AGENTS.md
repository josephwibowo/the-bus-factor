# `pipeline/assets_bq/`

BigQuery-dialect siblings of every DuckDB SQL asset under `../assets/`.

These files intentionally live **outside** `assets/` so Bruin's asset
discovery does not treat them as duplicate-name entries inside the
DuckDB-flavoured pipeline (Bruin rejects two assets that share a `name:`
field, regardless of `tags` or `--exclude-tag`).

## Why they exist anyway

- **Source of truth for the BigQuery dialect.** When we flip to BigQuery
  for the weekly live run, these files are the ones that execute. They
  capture the dialect translations (`DATE_DIFF`, `DATE_TRUNC`,
  `REGEXP_REPLACE`, `VARCHAR → STRING`, mart partitioning + clustering).
- **Reviewed by `tests/test_sql_parity.py`.** Every `.sql` under
  `../assets/staging/`, `../assets/intermediate/`, and `../assets/marts/`
  must have a matching `.bq.sql` here, and the top-level `SELECT` column
  list must agree.
- **Rendered in isolation.** `bruin render pipeline/assets_bq/marts/mart_packages_current.bq.sql`
  emits the BigQuery `CREATE OR REPLACE TABLE ... PARTITION BY ... CLUSTER BY ...`
  DDL without needing the BigQuery file registered in the live pipeline.

## Regeneration

```bash
uv run python scripts/generate_bq_siblings.py
```

Mechanical translation only — hand edits on the DuckDB side propagate on
the next run.  If a BigQuery sibling needs a permanent deviation (e.g.
array handling or window-function syntax diffs), extend the translator in
`scripts/generate_bq_siblings.py` rather than hand-patching the output.

## Promotion path (Cycle 3 `first_real_snapshot`)

When the weekly live run swaps in these assets, the runner copies this
tree over `../assets/` (or uses `--bruin-assets-dir assets_bq/`) so Bruin
sees only the BigQuery variant.  Until then these files are reviewed-only
and do not participate in fixture runs.
