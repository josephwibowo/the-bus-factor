# 2026-04-23 BigQuery Production Data Accuracy

## Summary

Implemented the weekly production accuracy fixes for the BigQuery path, nullable dependency reach, source-health gating, deterministic snapshot windows, and maintainer-like issue responsiveness. This is a methodology change and bumps the pipeline default to `v0.3.0`.

## Changes

- Updated `.github/workflows/weekly.yml` so the deployable `public-data` bundle is exported after the BigQuery runner succeeds.
- Extended `scripts/run_bigquery_smoke.py` to run the BigQuery sidecar asset graph, render custom checks with pipeline defaults, require healthy sources when requested, and export the public bundle from BigQuery marts through the canonical Pydantic exporter.
- Kept missing `dependent_count` values as `NULL` in `raw.deps_dev`, propagated nullable dependency reach through importance inputs, and reweighted importance scoring over available components in DuckDB and BigQuery marts.
- Added live source-health custom checks and wired source health into package confidence so unhealthy live sources block high confidence and can fail production publication.
- Added `live.resolve_window_date()` and anchored npm download, GitHub commits, releases, issues, and contributors windows to `snapshot_week`.
- Restricted issue responsiveness to eligible non-PR issues opened by non-bot, non-maintainer authors, and counted only first OWNER/MEMBER/COLLABORATOR non-bot comments before the snapshot boundary.
- Refreshed fixture known-state and analysis-gallery methodology metadata to `v0.3.0`.
- Updated methodology/progress docs and pipeline AGENTS notes for the production BigQuery path.

## Verification

- `uv run pytest tests/test_sql_parity.py tests/test_bigquery_smoke.py tests/test_scoring.py tests/test_live_ingestion.py tests/test_config.py tests/test_schemas.py -q` passed: 77 tests.
- `uv run ruff format --check .` passed.
- `uv run ruff check .` passed.
- `uv run mypy pipeline` passed.
- `bruin run --workers=1 --full-refresh -e fixture pipeline/pipeline.yml` passed: 51 assets and 309 quality checks.
- `env -u INGEST_TOKEN -u GITHUB_INGEST_TOKEN -u GITHUB_TOKEN uv run pytest -q` passed: 129 tests.
- `pnpm lint`, `pnpm typecheck`, and `pnpm build` passed in `web/`.

## Notes

- Plain `uv run pytest -q` fails in this local shell because `INGEST_TOKEN` is set and intentionally outranks the legacy `GITHUB_INGEST_TOKEN` in `pipeline.lib.http._auth_headers`; the isolated run above removes local credentials and passes.
- `bruin validate pipeline/pipeline.yml` exited zero but emitted dependency-parser startup warnings for all assets; the full fixture `bruin run` completed successfully.
- Spectacles validation could not run because this checkout has no `.spectacles` directory. `spectacles impact` is also not available in the installed CLI.
