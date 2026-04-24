# 2026-04-23 BigQuery Production Review Fixes

## Summary

Fixed two review findings from the BigQuery production accuracy pass and added deterministic weekly replay support.

## Changes

- Propagated `source_mode`, `warehouse`, `snapshot_week`, and `BRUIN_VARS` into the BigQuery public-bundle exporter subprocess so live BigQuery exports produce live metadata notes/license.
- Tightened the live scored-package floor check in both DuckDB and BigQuery marts so missing `npm` or `pypi` ecosystems fail publication instead of disappearing from the grouped result.
- Added optional `snapshot_week` to weekly `workflow_dispatch` and threaded it into both the Bruin live run and BigQuery runner.
- Documented deterministic replay in `README.md` and `docs/runbook.md`.
- Added regression tests for exporter environment propagation and missing-ecosystem guard shape.
- Fixed the BigQuery `int.repo_mapping` asset to use BigQuery-compatible `TRIM(value, characters)` syntax and added a parity regression for DuckDB-only `TRIM(BOTH ... FROM ...)`.
- Made the BigQuery runner drop each derived asset table before CTAS execution so reruns can change partitioning or clustering specs idempotently.
- Expanded BigQuery source-health check failures to include the specific unhealthy source rows in the smoke error message.
- Counted expected missing OpenSSF Scorecard API responses as successful null-score rows, not source-health ingestion loss.
- Cleared the per-window source-health buffer before weekly live ingestion so interrupted prior runs cannot poison the publish gate.

## Verification

- `spectacles resolve --paths ... --format md` failed because this checkout has no `.spectacles` directory.
- `spectacles validate-paths --paths ... --strict` failed for the same reason: no `.spectacles` directory.
- `uv run pytest tests/test_bigquery_smoke.py tests/test_sql_parity.py -q` passed: 20 tests.
- `uv run pytest tests/test_sql_parity.py tests/test_bigquery_smoke.py -q` passed: 21 tests.
- `uv run pytest tests/test_bigquery_smoke.py tests/test_sql_parity.py -q` passed: 22 tests.
- `uv run pytest tests/test_bigquery_smoke.py tests/test_sql_parity.py -q` passed: 23 tests.
- `uv run pytest tests/test_live_ingestion.py tests/test_bigquery_smoke.py tests/test_sql_parity.py -q` passed: 36 tests.
- `uv run ruff format --check tests/test_sql_parity.py` passed.
- `uv run ruff check tests/test_sql_parity.py` passed.
- `uv run ruff format --check scripts/run_bigquery_smoke.py tests/test_bigquery_smoke.py` passed.
- `uv run ruff check scripts/run_bigquery_smoke.py tests/test_bigquery_smoke.py` passed.
- `uv run ruff format --check pipeline/assets/raw/raw_scorecard.py tests/test_live_ingestion.py scripts/run_bigquery_smoke.py tests/test_bigquery_smoke.py` passed.
- `uv run ruff check pipeline/assets/raw/raw_scorecard.py tests/test_live_ingestion.py scripts/run_bigquery_smoke.py tests/test_bigquery_smoke.py` passed.
- `uv run ruff format --check .` passed.
- `uv run ruff check .` passed.
- `uv run mypy pipeline` passed.
- `uv run pytest tests/test_sql_parity.py tests/test_bigquery_smoke.py tests/test_scoring.py tests/test_live_ingestion.py tests/test_config.py tests/test_schemas.py -q` passed: 79 tests.
- `env -u INGEST_TOKEN -u GITHUB_INGEST_TOKEN -u GITHUB_TOKEN uv run pytest -q` passed: 131 tests.
- `bruin run --workers=1 --full-refresh -e fixture pipeline/pipeline.yml` passed: 51 assets and 309 quality checks.
- `pnpm build` passed in `web/`.

## Follow-ups

- BigQuery production export still needs a real BigQuery smoke/production credentialed run outside this sandbox before trusting the weekly workflow end to end.
