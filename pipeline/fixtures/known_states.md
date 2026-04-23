# Known-state validation fixtures

The fixture pipeline must agree with the expectations below on every run. A custom Bruin SQL check asserts that `mart.packages_current` classifies each listed package exactly as stated; the pipeline fails otherwise.

The machine-readable source of truth is [`known_states.csv`](known_states.csv); this document mirrors it for human readers. Every package added must be accompanied by:

1. A row in [`known_states.csv`](known_states.csv).
2. Rows in every upstream seed (`npm_registry.csv` or `pypi_registry.csv`, `deps_dev.csv`, the GitHub seeds, `osv.csv` where relevant, `scorecard.csv` where relevant).
3. A 1–2 sentence rationale (included in the CSV `rationale` column).

All packages below are **synthetic / curated**. They are designed to exercise the pipeline's classification paths. They are not real-world claims about any shipping package.

Snapshot reference date: **2026-04-20** (Monday 00:00 UTC).

Scoring version: **v0.2.0** (risk_score_min: 30; severity tiers: Stable <15 / Watch 15–24 / Elevated 25–29 / High 30–49 / Critical ≥50).

---

## npm — expected `flagged = true`

| Package | Rationale | Dominant fragility signals |
| --- | --- | --- |
| `legacy-forge` | Top-1 dep reach; risk ~86 (Critical). | release_recency, commit_recency, release_cadence_decay, contributor_bus_factor, openssf_scorecard |
| `neglected-stream` | Solo-maintainer pattern; risk ~83 (Critical). | contributor_bus_factor, release_recency, commit_recency, release_cadence_decay |
| `stale-bundle` | Risk ~72 (Critical); cleared flag gate under v0.2.0. | release_recency, commit_recency, release_cadence_decay, contributor_bus_factor, openssf_scorecard |
| `old-validator` | Risk ~71 (Critical); cleared flag gate under v0.2.0. | release_recency, commit_recency, release_cadence_decay, contributor_bus_factor, openssf_scorecard |

## npm — expected `flagged = false` (ranked)

| Package | Expected state | Why not flagged |
| --- | --- | --- |
| `active-logger` | Stable | Actively maintained; no fragility accumulation. |
| `fresh-parser` | Stable | Strongest maintenance signals. |
| `vibrant-router` | Stable | Healthy; not in fragility band. |
| `frozen-format` | Critical (not flagged) | Risk ~67 (Critical) but importance percentile ~74% falls just below the top-quartile gate (75%). Demonstrates the importance gate. |
| `aged-template` | Critical (not flagged) | Importance percentile ~53%; well below top-quartile gate. Demonstrates the importance gate. |
| `robust-cache` | Watch/Elevated | Partial fragility signals; risk below 30. |
| `quiet-daemon` | Stable/Watch | Low fragility; risk below 30. |

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
| `legacy-pyforge` | Top-1 dep reach; risk ~87 (Critical). | release_recency, commit_recency, release_cadence_decay, contributor_bus_factor, openssf_scorecard |
| `neglected-pystream` | Solo-maintainer; risk ~83 (Critical). | contributor_bus_factor, release_recency, commit_recency, release_cadence_decay |
| `stale-pybundle` | Risk ~72 (Critical); cleared flag gate under v0.2.0. | release_recency, commit_recency, release_cadence_decay, contributor_bus_factor, openssf_scorecard |
| `old-pyvalidator` | Risk ~71 (Critical); cleared flag gate under v0.2.0. | release_recency, commit_recency, release_cadence_decay, contributor_bus_factor, openssf_scorecard |

## PyPI — expected `flagged = false` (ranked)

| Package | Expected state | Why not flagged |
| --- | --- | --- |
| `active-pylogger` | Stable | Active maintenance. |
| `fresh-pyparser` | Stable | Strong signals. |
| `vibrant-pyrouter` | Stable | Healthy. |
| `frozen-pyformat` | Critical (not flagged) | Risk ~67 (Critical) but importance percentile ~74% falls just below the top-quartile gate (75%). Demonstrates the importance gate. |
| `aged-pytemplate` | Critical (not flagged) | Importance percentile ~53%; well below top-quartile gate. |
| `robust-pycache` | Watch/Elevated | Partial signals; risk below 30. |
| `quiet-pydaemon` | Stable/Watch | Low fragility; risk below 30. |

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
