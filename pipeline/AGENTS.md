# AGENTS.md — `pipeline/`

The Bruin pipeline. See repo-root [`AGENTS.md`](../AGENTS.md) for baseline rules; the authoritative spec is [`docs/init.md`](../docs/init.md).

## Asset layer conventions

| Layer | Purpose | Language |
| --- | --- | --- |
| `assets/raw/` | Direct API extraction; minimal shaping. One asset per source endpoint. | Python (`materialize()` returning a DataFrame). |
| `assets/seeds/` | Fixture files loaded into DuckDB for the reviewer path. | YAML / CSV / JSON seed assets. |
| `assets/staging/` | Typed, deduped, source-faithful tables. | SQL (BigQuery live, DuckDB fixture — toggled by `{{ var('source_mode') }}`). |
| `assets/intermediate/` | Derived joins, normalization, percentile ranks. | SQL. |
| `assets/marts/` | Curated, AI-facing tables that match the public contract. | SQL. |

### Required marts

`mart.packages_current`, `mart.package_scores`, `mart.package_evidence`, `mart.weekly_findings`, `mart.coverage_summary`, `mart.analysis_examples`, `mart.source_health`, `mart.market_positioning`.

A final `export_public_bundle` asset writes JSON shards to `../public-data/`. A `generate_weekly_card` asset writes the share image to `../reports/cards/`.

## Rules specific to this directory

1. **Scoring weights and thresholds come from `config/scoring.yml`.** SQL reads them via Jinja; Python loads with `yaml.safe_load`. Never inline a number.
2. **SQL assets are `SELECT`-only.** Bruin wraps materialization (append/merge/create+replace). Choose materialization intentionally and put it in asset frontmatter.
3. **Declare dependencies explicitly.** Run `bruin patch fill-asset-dependencies .` to auto-fill from SQL refs, then review and commit. Do not hand-edit the DAG if a patch can infer it correctly.
4. **Every mart column with a contract needs a built-in check** (`not_null`, `unique`, accepted values, non-negative). Every cross-column or business invariant needs a custom SQL check.
5. **Custom checks that must exist** (enforced in CI):
   - No duplicate `(snapshot_week, ecosystem, package_name)` in `mart.packages_current`.
   - No `flagged = true` row with `confidence = low`.
   - No `flagged = true` row with fewer than 2 fragility inputs ≥ `flagged.signal_contribution_threshold`.
   - No archived/deprecated or unmappable package in the exported `leaderboard.json`.
   - Score fields bounded 0–100.
   - Every exported row has `methodology_version`, `snapshot_week`, and source-health metadata.
   - `mart.packages_current` agrees with `fixtures/known_states.md` expectations (fixture runs only).
6. **AI context**: mart tables and key columns must carry Bruin descriptions so the AI Data Analyst answers from grounded metadata. Use `bruin ai enhance` as needed but review the output.
7. **Fixture parity**: any new asset must have a fixture path that runs with no API keys. If a source cannot be reasonably faked, stub the raw extract with a seed asset.
8. **Reporting window**: Monday 00:00 UTC to Monday 00:00 UTC. `snapshot_week` in all marts is the Monday ISO date.
9. **Missing data reduces confidence, never inflates fragility.** If a source is stale, record it in `mart.source_health`; do not impute.

## Commands

```bash
# Validate the asset graph
bruin validate pipeline.yml

# Show final SQL Bruin will execute for a given mart
bruin render assets/marts/mart.packages_current.sql

# Fixture run (no creds required)
bruin run --workers=1 --full-refresh -e fixture pipeline.yml

# Live BigQuery run
bruin run -e local_live_bq --var source_mode=live --var warehouse=bigquery pipeline.yml

# Auto-fill dependencies from SQL refs (review diff before committing)
bruin patch fill-asset-dependencies .
```

## Layout

```
pipeline/
├── pipeline.yml              # Pipeline-level config, schedule, variables
├── config/
│   └── scoring.yml           # All weights, thresholds, tier bands, mapping rules
├── assets/
│   ├── raw/                  # Python source extractors
│   ├── seeds/                # Fixture seed assets
│   ├── staging/              # Typed source tables
│   ├── intermediate/         # Joins and normalization
│   └── marts/                # Curated output tables
├── fixtures/                 # Checked-in fixture data (<5 MB total)
│   └── known_states.md       # Expected-flag / expected-not-flag expectations
└── scripts/                  # One-off helpers (not part of the asset graph)
```
