# Cycle 2 Live Ingestion Debugging

## What Changed

- Stopped the stalled `local_live_bq` run from `docs/runbook.md` (`pid 74152`, worker `pid 79362`).
- Verified Issue 1 was a false diagnosis: the live DuckDB tables contained 847 GitHub repo rows, and `repo_urls_from_duckdb()` returned 847 canonical URLs.
- Updated `raw.github_contributors` so each `/stats/contributors` request has an `asyncio.wait_for(...)` deadline and emits repo-scoped timeout, failure, and retry logs.
- Reduced GitHub stats polling from 3 attempts with 5-second stepped waits to 2 attempts with 3-second stepped waits.
- Added tests for full registry-to-repo fanout and contributor stats timeout/pending retry behavior.
- Updated `docs/runbook.md` with the corrected cycle-2 diagnosis and next steps.
- Added a shared Bruin-visible logging handler for `pipeline.*` loggers, standardized live-source `key=value` events, and repo/universe discovery count logs.
- Ran a 5 npm + 5 PyPI real-data `local_live_bq` smoke and fixed three additional live-path blockers:
  missing all-null contributor share column, nullable GitHub release dates, and fixture-only known-state checks blocking live samples.
- Confirmed the successful smoke writes 8 `public-data/*.json` files and weekly share cards; there is no configured GCS bucket in this repo path.

## Files Touched

- `pipeline/lib/sources.py`
- `pipeline/lib/live.py`
- `pipeline/assets/raw/raw_github_contributors.py`
- `tests/test_sources.py`
- `tests/test_live_ingestion.py`
- `tests/test_sql_parity.py`
- `pipeline/assets/staging/stg_github_releases.sql`
- `pipeline/assets_bq/staging/stg_github_releases.bq.sql`
- `pipeline/assets/marts/mart_package_scores.sql`
- `pipeline/assets_bq/marts/mart_package_scores.bq.sql`
- `docs/runbook.md`
- `docs/progress.md`
- `worklogs/2026-04-22_cycle-2-live-ingestion.md`

## Commands Run

- `spectacles resolve --paths pipeline/assets/raw/raw_github_contributors.py pipeline/lib/live.py tests docs/runbook.md --format md` - failed: repo has no `.spectacles` directory.
- `.venv/bin/python -m pytest tests/test_live_ingestion.py -q` - passed, 3 tests.
- `.venv/bin/python -m pytest tests/test_sources.py tests/test_live_ingestion.py -q` - passed, 15 tests.
- `.venv/bin/python -m ruff check pipeline/assets/raw/raw_github_contributors.py tests/test_live_ingestion.py` - passed.
- `.venv/bin/python -m ruff format --check pipeline/assets/raw/raw_github_contributors.py tests/test_live_ingestion.py` - passed.
- `.venv/bin/python -m mypy pipeline/assets/raw/raw_github_contributors.py tests/test_live_ingestion.py` - passed.
- `.venv/bin/python -m ruff check pipeline/lib/sources.py pipeline/lib/live.py pipeline/assets/raw/raw_github_contributors.py tests/test_sources.py tests/test_live_ingestion.py` - passed.
- `.venv/bin/python -m ruff format --check pipeline/lib/sources.py pipeline/lib/live.py pipeline/assets/raw/raw_github_contributors.py tests/test_sources.py tests/test_live_ingestion.py` - passed.
- `.venv/bin/python -m mypy pipeline/lib/sources.py pipeline/lib/live.py pipeline/assets/raw/raw_github_contributors.py tests/test_sources.py tests/test_live_ingestion.py` - passed.
- `.venv/bin/python -m ruff check pipeline scripts tests` - passed.
- `.venv/bin/python -m ruff format --check pipeline scripts tests` - passed.
- `.venv/bin/python -m mypy pipeline tests` - passed, 34 source files.
- `.venv/bin/python -m pytest -q` - passed, 106 tests.
- `bruin validate pipeline/pipeline.yml` - passed, 51 assets.
- `bruin run --workers=1 --full-refresh -e ci_fixture pipeline/pipeline.yml` - passed, 51 assets and 304 quality checks in 1m37.415s.
- `spectacles validate-paths --paths pipeline/assets/raw/raw_github_contributors.py tests/test_live_ingestion.py docs/runbook.md --strict` - failed: repo has no `.spectacles` directory.
- `bruin run --workers=1 --full-refresh -e local_live_bq --var 'source_mode="live"' --var 'warehouse="bigquery"' --var npm_package_limit=5 --var pypi_package_limit=5 pipeline/pipeline.yml` - failed first on live-data tolerance checks, then passed after fixes: 51 assets and 303 quality checks in 2m17.468s.
- `.venv/bin/python -m pytest tests/test_schemas.py -q` - passed, 14 tests.
- `pnpm --filter ./web build` - passed, 15 static pages.
- `.venv/bin/python -m ruff format --check pipeline scripts tests` - passed after formatting `raw_github_contributors.py`.
- `.venv/bin/python -m ruff check pipeline scripts tests` - passed.
- `.venv/bin/python -m mypy pipeline tests` - passed, 34 source files.
- `.venv/bin/python -m pytest -q` - passed, 109 tests.
- `BUS_FACTOR_LOG_LEVEL=INFO BQ_MAX_BYTES_BILLED=10000000000 .venv/bin/python scripts/run_bigquery_smoke.py --duckdb-path data/local_live_bq.duckdb --project-id bus-factor-494119 --location US --dataset-prefix bf_smoke` - passed; populated `bf_smoke_raw`, `bf_smoke_stg`, `bf_smoke_int`, `bf_smoke_mart`, ran all BigQuery sibling SQL assets and mart custom checks.
- `.venv/bin/python -m ruff format --check pipeline scripts tests` - passed.
- `.venv/bin/python -m ruff check pipeline scripts tests` - passed.
- `.venv/bin/python -m mypy pipeline tests` - passed, 35 source files.
- `.venv/bin/python -m pytest -q` - passed, 117 tests.
- `bruin validate pipeline/pipeline.yml` - passed, 51 assets.
- `bruin run --workers=1 --full-refresh -e ci_fixture pipeline/pipeline.yml` - passed, 51 assets and 303 quality checks in 2m5.025s.

## Follow-Ups

- Run the live pipeline at `50/50` before another `500/500` run.
- Keep `--workers=1` for local DuckDB-backed runs to avoid writer lock conflicts.
- Refresh the npm universe seed through the billed BigQuery path once live ingestion is stable.
- Promote or register `pipeline/assets_bq/` in Cycle 3 if the normal Bruin live run should materialize unprefixed BigQuery datasets directly. Cycle 2 now has a separate BigQuery validation harness that executes the BQ sibling SQL into `bf_smoke_*`.
