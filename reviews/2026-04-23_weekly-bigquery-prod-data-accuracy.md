---
title: Weekly BigQuery Production Data Accuracy Review
date: 2026-04-23
author: Codex
review_target: ".github/workflows/weekly.yml and weekly live data path"
changed_paths:
  - .github/workflows/weekly.yml
  - .bruin.yml.example
  - pipeline/pipeline.yml
  - pipeline/data/universe/npm.json
  - pipeline/data/universe/pypi.json
  - pipeline/lib/universe.py
  - pipeline/lib/live.py
  - pipeline/assets/raw
  - pipeline/assets/staging
  - pipeline/assets/intermediate
  - pipeline/assets/marts
  - pipeline/assets_bq
  - scripts/run_bigquery_smoke.py
  - public-data
spectacles:
  used: false
  resolve_command: "spectacles resolve --paths .github/workflows/weekly.yml pipeline/pipeline.yml pipeline/assets/raw pipeline/assets/staging pipeline/assets/intermediate pipeline/assets/marts pipeline/assets_bq pipeline/data/universe public-data --format md"
  validate_command: "spectacles validate-paths --paths .github/workflows/weekly.yml pipeline/pipeline.yml pipeline/assets/raw pipeline/assets/staging pipeline/assets/intermediate pipeline/assets/marts pipeline/assets_bq pipeline/data/universe public-data --strict"
  domains: []
summary:
  recommendation: request-changes
  risk: high
---

## Executive summary

- The scheduled workflow is named and configured like a BigQuery production run, but it runs the registered DuckDB asset tree and exports from DuckDB. The BigQuery sibling assets are explicitly documented as review-only until promoted.
- The npm universe carries seeded dependent counts, but the PyPI universe carries no dependency reach and the live deps.dev adapter coerces missing reach to zero. This depresses PyPI importance scores despite the code comment claiming a fallback.
- Source-health failures are surfaced in `mart.source_health`, but they do not block export and do not feed the confidence calculation. A degraded GitHub or Scorecard source can produce a public bundle with understated fragility.
- Several live source windows are anchored to wall-clock `now()` / `today()` rather than the `snapshot_week` anchor, so manual reruns or delayed schedules can produce a Monday-labeled snapshot using Tuesday or later activity.
- The issue responsiveness metric is labeled as maintainer response time, but the extractor counts the first non-author comment as a maintainer response. That can materially understate fragility on community-heavy repos.

## Spectacles alignment

Spectacles is installed, but this repository has no `.spectacles` directory, so `spectacles resolve` failed with "No .spectacles directory found." This review therefore uses the repo spec, AGENTS rules, and code-level contracts as the authoritative source.

Relevant constraints from repo instructions and `docs/init.md`:

- Weekly live and fixture runs must produce the same public-export schema.
- Bruin is the judged data product, and BigQuery live plus DuckDB fixture should be intentional and reproducible.
- Missing data reduces confidence and must not inflate or silently distort fragility.
- Scoring weights and thresholds must come from `pipeline/config/scoring.yml`.
- The bundle must be accurate enough for public surfaces; false positives cost more than false negatives, but silent false negatives still undermine the product.

Required follow-ups:

- After adding Spectacles project memory, run the resolve and validate commands listed in frontmatter.
- Because this review crosses workflow, warehouse, raw ingestion, scoring, and public export contracts, run `spectacles impact --paths .github/workflows/weekly.yml pipeline assets_bq public-data` once Spectacles is initialized.

## Findings (triage-ready)

1. **Severity**: BLOCKER
   **Category**: correctness/data/operability
   **Where**: `.github/workflows/weekly.yml:48`, `pipeline/assets_bq/AGENTS.md:35`, `pipeline/assets/marts/export_public_bundle.py:84`
   **Evidence**: The workflow runs `bruin run -e ci_live_bq ... pipeline/pipeline.yml`, but the BigQuery SQL assets live outside Bruin discovery under `pipeline/assets_bq`. That directory states the weekly run must copy/use those assets in the future and that they "do not participate in fixture runs." The export asset also connects only to DuckDB.
   **Recommendation**: Decide the production shape and make it explicit. Either rename the workflow to DuckDB-backed live export, or implement a BigQuery production path that uploads raw/seed tables, executes `assets_bq`, validates BigQuery custom checks, then exports the bundle from BigQuery or from a clearly synchronized DuckDB mirror. The existing `scripts/run_bigquery_smoke.py` can be promoted or adapted, but it is not invoked by the weekly workflow today.
   **Test impact**: Add a workflow-level test or CI guard asserting that the scheduled prod job either invokes the BigQuery smoke/promote path or uses a Bruin asset directory containing `type: bq.sql` assets. Add a post-run query/count check against the production BigQuery datasets.
   **Spectacles link**: Repo contract: live BigQuery and DuckDB fixture paths must be intentional, reproducible, and schema-compatible.

2. **Severity**: MAJOR
   **Category**: data/correctness
   **Where**: `pipeline/assets/raw/raw_deps_dev.py:117`, `pipeline/lib/universe.py:63`, `pipeline/pipeline.yml:39`
   **Evidence**: `UniversePackage` says missing `dependent_count` is unknown and downstream scoring falls back to the next importance signal, but `raw.deps_dev` writes missing counts as `0`. PyPI seeds from `hugovk/top-pypi-packages` contain no dependent counts, while dependency reach is weighted at 0.60. This makes PyPI importance scores structurally lower than intended, not merely less certain.
   **Recommendation**: Preserve unknown dependency reach as `NULL` and make `int.importance_inputs` / `mart.package_scores` reweight available importance components per ecosystem/package, or add a real PyPI dependency-reach source. Do not treat unknown reach as zero reach.
   **Test impact**: Add a PyPI fixture package with missing dependency reach and assert its importance score uses download/security weights normalized to available signals. Add a regression test that the PyPI ecosystem can reach a high importance percentile without seeded dependent counts.
   **Spectacles link**: Missing data must reduce confidence or reweight available evidence, not become a false zero.

3. **Severity**: MAJOR
   **Category**: data/observability
   **Where**: `pipeline/assets/marts/mart_source_health.sql:36`, `pipeline/assets/marts/mart_package_scores.sql:300`
   **Evidence**: Source health is exported as metadata, but no custom check fails the run for `status != 'ok'`, `stale = true`, `failure_count > 0`, or materially low `row_count`. The score confidence logic only considers mapping bucket, signal count, and age; it ignores source-health staleness despite `confidence_requirements.max_stale_sources` in `scoring.yml`.
   **Recommendation**: Add live-mode custom checks that either fail the run for critical source failures or force package confidence to low/medium when required source categories are stale. At minimum, block publish when registry sources fail, when all GitHub repo-derived sources fail, or when scored row counts fall below the expected live universe tolerance.
   **Test impact**: Add fixture/source-health tests where `github_commits` or `openssf_scorecard` is failed and assert flagged packages cannot remain high confidence. Add a Bruin custom check for unhealthy live sources.
   **Spectacles link**: Missing data reduces confidence and should not silently understate fragility.

4. **Severity**: MAJOR
   **Category**: data/correctness
   **Where**: `pipeline/assets/raw/raw_github_commits.py:85`, `pipeline/assets/raw/raw_github_releases.py:85`, `pipeline/assets/raw/raw_github_issues.py:119`, `pipeline/assets/raw/raw_npm_registry.py:143`
   **Evidence**: The snapshot mart stamps a Monday `snapshot_week`, but live raw assets compute their windows from wall-clock execution time. A workflow dispatch or delayed retry can label data as one Monday while including activity after that snapshot boundary or using a shifted 90-day download window.
   **Recommendation**: Pass `snapshot_week` into each extractor and derive all lookback windows from that anchor: downloads ending at `snapshot_week - 1 day`, GitHub issue lookback `[snapshot_week - 180 days, snapshot_week)`, commit/release windows ending at `snapshot_week`, and contributor windows ending at `snapshot_week`.
   **Test impact**: Freeze `snapshot_week` in unit tests and assert the generated API params use the anchor date, not `datetime.now()` or `date.today()`.
   **Spectacles link**: Reporting window is Monday 00:00 UTC to Monday 00:00 UTC, with `snapshot_week` as the canonical anchor.

5. **Severity**: MAJOR
   **Category**: data/correctness
   **Where**: `pipeline/assets/raw/raw_github_issues.py:80`
   **Evidence**: The asset describes "first maintainer response", but `_first_maintainer_response_days` returns the first comment from anyone other than the issue author. Community comments, bot comments, and other reporters can therefore be counted as maintainer responses.
   **Recommendation**: Use GitHub author association / collaborator signals to identify maintainer-like responses, and exclude bots unless explicitly accepted. A conservative fallback is to mark the response unavailable when no `MEMBER`, `OWNER`, or `COLLABORATOR` comment appears.
   **Test impact**: Add mocked issue/comment payloads covering author, external user, bot, member, owner, and collaborator comments. Assert only maintainer-like comments affect the median.
   **Spectacles link**: Evidence labels must match the actual measurement used in public findings.

## Suggested todos

- [ ] Promote or rename the weekly production path so `.github/workflows/weekly.yml` truthfully runs BigQuery assets or truthfully declares DuckDB live export. Acceptance: the weekly job executes BigQuery mart SQL or the workflow/title/docs no longer claim BigQuery production.
- [ ] Fix PyPI missing dependency reach handling. Acceptance: unknown reach stays `NULL` or is reweighted, and PyPI importance is not capped by a false zero in the 0.60 dependency-reach component.
- [ ] Gate bundle publication on live source health and wire source staleness into score confidence. Acceptance: failed/stale critical sources fail the workflow or force affected package confidence below the flaggable threshold.
- [ ] Anchor all extractor windows to `snapshot_week`. Acceptance: workflow dispatch for a historical Monday generates the same API windows independent of the run date.
- [ ] Replace "non-author comment" issue responsiveness with a maintainer-like author association rule. Acceptance: comments from unrelated users do not reduce response time.

## Nits

- `raw_pypi_registry.py` labels `last_month * 3` as `downloads_90d`; this is a useful approximation, but the public methodology should disclose it or the extractor should use an actual 90-day source.
- `pipeline/assets/raw/raw_github_issues.py` defines `COMMENTS_PAGE_CAP` but does not use it. Either use it for pagination clarity or remove it.
- `mart.package_scores.importance_rank_within_ecosystem` is described as an importance rank but ordered by `risk_score`; rename to `risk_rank_within_ecosystem` or update the description.

## Verification checklist

- `uv run pytest tests/test_sql_parity.py tests/test_bigquery_smoke.py -q` - passed, 18 tests.
- `bruin validate pipeline/pipeline.yml` - exited 0, but Bruin emitted SQL parser dependency warnings in this sandbox.
- Run the full fixture command after fixes: `bruin run --workers=1 --full-refresh -e fixture pipeline/pipeline.yml`.
- Run a live BigQuery validation path after fixes: production BigQuery mart execution plus custom checks, or `uv run python scripts/run_bigquery_smoke.py` against a fresh live DuckDB raw snapshot.
- Run `cd web && pnpm lint && pnpm typecheck && pnpm build` if export schemas or web-facing bundle fields change.

## Codify candidates

- Lesson: A workflow named BigQuery production must prove that BigQuery assets execute in that workflow, not only that BigQuery credentials are present.
  Suggested node type: contract
  Context files: `.github/workflows/weekly.yml`, `pipeline/assets_bq/AGENTS.md`, `scripts/run_bigquery_smoke.py`

- Lesson: Unknown source metrics must remain unknown and reduce confidence or reweight available evidence; they must not be coerced to zero unless zero is a real observation.
  Suggested node type: principle
  Context files: `pipeline/assets/raw/raw_deps_dev.py`, `pipeline/assets/intermediate/int_importance_inputs.sql`, `pipeline/assets/marts/mart_package_scores.sql`
