# Known-state validation fixtures

The fixture pipeline must agree with the expectations below on every run. A custom Bruin SQL check asserts that `mart.packages_current` classifies each listed package exactly as stated; the pipeline fails otherwise.

The machine-readable source of truth is [`known_states.csv`](known_states.csv); this document mirrors it for human readers. Every package added must be accompanied by:

1. A row in [`known_states.csv`](known_states.csv).
2. Rows in every upstream seed (`npm_registry.csv` or `pypi_registry.csv`, `deps_dev.csv`, the GitHub seeds, `osv.csv` where relevant, `scorecard.csv` where relevant).
3. A 1–2 sentence rationale (included in the CSV `rationale` column).

All packages below are **synthetic / curated**. They are designed to exercise the pipeline's classification paths. They are not real-world claims about any shipping package.

Snapshot reference date: **2026-04-20** (Monday 00:00 UTC).

---

## npm — expected `flagged = true`

| Package | Rationale | Dominant fragility signals |
| --- | --- | --- |
| `legacy-forge` | Top-1 dep reach among the eligible cohort. | release_recency, commit_recency, contributor_bus_factor, scorecard |
| `neglected-stream` | Solo-maintainer pattern with oldest release. | contributor_bus_factor, release_recency, commit_recency |
| `stale-bundle` | Release cadence collapsed from 6 to 0 year-over-year. | release_cadence_decay, release_recency, commit_recency |
| `old-validator` | Broad fragility with low Scorecard. | release_recency, commit_recency, scorecard |
| `frozen-format` | Oldest release + commit, very high contributor concentration. | release_recency, commit_recency, contributor_bus_factor, scorecard |

## npm — expected `flagged = false` (ranked)

| Package | Expected state | Why not flagged |
| --- | --- | --- |
| `active-logger` | Stable/Watch | Actively maintained; no fragility accumulation. |
| `fresh-parser` | Stable | Strongest maintenance signals. |
| `vibrant-router` | Stable | Healthy; not in fragility band. |
| `aged-template` | Elevated (not flagged) | Fragile signals BUT importance rank 9 of 20 → below top quartile. Demonstrates the importance gate. |
| `robust-cache` | Watch/Elevated | Some weak signals but fewer than two independent inputs ≥40. |
| `quiet-daemon` | Stable/Watch | Middling signals; fragility below flagged floor. |

## npm — expected excluded

| Package | Exclusion | Why |
| --- | --- | --- |
| `@types/legacy-forge` | `stub_types` | Name matches `@types/*` pattern. |
| `archived-compiler` | `archived_deprecated` | Repo is archived. |
| `newborn-cli` | `too_new` | First release less than 12 months ago. |
| `orphan-package` | `unmappable` | No repository URL in registry. |

---

## PyPI — expected `flagged = true`

| Package | Rationale | Dominant fragility signals |
| --- | --- | --- |
| `legacy-pyforge` | Top-1 dep reach. | release_recency, commit_recency, contributor_bus_factor, scorecard |
| `neglected-pystream` | Solo-maintainer. | contributor_bus_factor, release_recency |
| `stale-pybundle` | Release cadence decay. | release_cadence_decay, release_recency |
| `old-pyvalidator` | Broad fragility + low Scorecard. | release_recency, commit_recency, scorecard |
| `frozen-pyformat` | Oldest release/commit. | release_recency, commit_recency, contributor_bus_factor |

## PyPI — expected `flagged = false` (ranked)

| Package | Expected state | Why not flagged |
| --- | --- | --- |
| `active-pylogger` | Stable/Watch | Active maintenance. |
| `fresh-pyparser` | Stable | Strong signals. |
| `vibrant-pyrouter` | Stable | Healthy. |
| `aged-pytemplate` | Elevated (not flagged) | Fragile but below top-quartile importance. |
| `robust-pycache` | Watch/Elevated | Fewer than two independent ≥40 signals. |
| `quiet-pydaemon` | Stable/Watch | Middling signals. |

## PyPI — expected excluded

| Package | Exclusion | Why |
| --- | --- | --- |
| `types-legacy-pyforge` | `stub_types` | Name matches `types-*` pattern. |
| `archived-pycompiler` | `archived_deprecated` | Repo archived. |
| `newborn-pycli` | `too_new` | < 12 months old. |
| `orphan-pypackage` | `unmappable` | No repository URL. |

---

## How the check works

A custom SQL check on `mart.packages_current` (declared inline in the asset frontmatter as `custom_checks`) joins against the `seed.known_states` table and returns any row where `actual_state <> expected_state` or `actual_flagged <> expected_flagged`. Zero rows = pass.

When changing scoring weights or thresholds, update the CSV's expectations and re-run the fixture pipeline before committing. Changing any expectation is equivalent to a methodology-version bump.
