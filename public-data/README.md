# `public-data/`

Versioned JSON bundle exported by the Bruin pipeline and consumed by the Astro site. These files are **generated**, not hand-edited. They are committed so GitHub Pages can serve the latest successful run even if the next scheduled run fails.

## Files the pipeline writes

| File | Source mart | Purpose |
| --- | --- | --- |
| `metadata.json` | `mart_packages_current` + `mart_source_health` | Reporting window, generated timestamp, methodology version, freshness, source summary. |
| `leaderboard.json` | `mart_weekly_findings` (flagged only) | Home page leaderboard. |
| `packages.json` | `mart_packages_current` + `mart_package_evidence` + `mart_package_scores` | Package detail data (may be sharded by ecosystem). |
| `weekly_report.json` | `mart_weekly_findings` | Narrative, notable packages, permalink metadata. |
| `coverage.json` | `mart_coverage_summary` | Tracked / ranked / unmappable / archived / too-new counts. |
| `analysis_gallery.json` | `mart_analysis_examples` | Prompts, screenshot paths, dataset + methodology version, capture date. |
| `source_health.json` | `mart_source_health` | Freshness, failures, stale-fallback state. |
| `market_positioning.json` | `mart_market_positioning` | Rows for the positioning / comparison page. |

## Invariants

1. Every file includes `methodology_version` and `snapshot_week`.
2. `leaderboard.json` never contains archived/deprecated, unmappable, too-new, or `confidence = low` rows.
3. Every row with a score has that score in `[0, 100]`.
4. Schemas are validated at build time in `web/`. Schema drift fails the build.
