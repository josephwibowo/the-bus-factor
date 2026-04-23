# CLAUDE.md — The Bus Factor

Mirrors [`AGENTS.md`](AGENTS.md).


## What this is

A **Bruin-powered data engineering project** for the **Bruin Data Engineering Project Competition** (deadline **June 1, 2026, 12:00 UTC**). It ingests public package/repo/dependency/security data for top npm and PyPI packages, scores weekly "importance × fragility" snapshots through a Bruin pipeline, and publishes a static showcase (leaderboard, package detail, weekly report, AI-analyst screenshot gallery, methodology).

**The judged deliverable is the Bruin pipeline + GitHub repo + launch materials.** The website is a showcase layer, not the product.

Authoritative spec: [`docs/init.md`](docs/init.md) — read the relevant section before making non-trivial changes. If this file conflicts with the spec, the spec wins.

## Tech stack at a glance

- **Bruin CLI** — orchestrator, transformations, quality checks, lineage, AI context
- **Python 3.12.x** via **uv** — ingestion assets; tooling: Ruff, mypy, pytest
- **BigQuery** (live) + **DuckDB** (fixture) — same asset graph, toggled by Bruin variable `source_mode`
- **Astro + TypeScript (strict)** via **pnpm** (Node 24 LTS) — static site under `web/`
- **GitHub Actions** — weekly schedule + CI gates
- **GitHub Pages** — only hosting dependency; demo must work without credentials or live services
- **Bruin AI Data Analyst** — the only analysis surface; never build a custom NL/chat interface

## Repo map

```text
docs/init.md           # Authoritative spec — source of truth for all decisions
pipeline/              # Bruin pipeline (assets, config, fixtures); scoring weights live in pipeline/config/scoring.yml
public-data/           # JSON bundle exported by pipeline, consumed by web/
web/                   # Astro static site — reads public-data/ only
analysis/              # Canonical AI Data Analyst prompts + local screenshots
reports/               # Weekly narrative + share cards
launch/                # Slack post, LinkedIn post, comparison tables, submission checklist, maintainer-response template
docs/                  # methodology.md, sources.md, init.md
```

Per-directory `AGENTS.md` files will hold subsystem details as those directories get built. When adding a subsystem, add its `AGENTS.md` there rather than growing this file.

## Baseline rules (repo-wide)

1. **Never commit** `.bruin.yml`, local DuckDB files, or API caches. Only `.bruin.yml.example` with placeholders.
2. **Never hardcode scoring weights or thresholds.** They live in `pipeline/config/scoring.yml`. Changing any weight requires bumping `methodology_version`.
3. **Never re-implement scoring outside the Bruin marts.** Frontend and exports read from marts; they don't re-derive.
4. **Never present synthetic data as real results.** Honest empty state is fine; fake rankings are not.
5. **Never add a live backend.** No serverless, no hosted DB, no login. Static exports only.
6. **Tone**: use "flagged / watch / elevated / fragility signals / evidence / continuity risk." Never "abandoned / dead / negligent / neglected / unsafe." Maintainer names never appear in viral surfaces (Slack post, LinkedIn post, weekly card, leaderboard summaries). False positives cost more than false negatives.
7. **Fixture and live runs must produce the same public-export schema.** The fixture path must run with no API keys.
8. **Under scope pressure, cut package-count breadth first.** Never cut: quality checks, fixture reproducibility, README clarity, competitive positioning, AI-analyst screenshots, launch materials.

## Before committing

```bash
uv run ruff format --check . && uv run ruff check . && uv run mypy pipeline && uv run pytest
bruin validate pipeline/pipeline.yml
bruin run --workers=1 --full-refresh -e fixture pipeline/pipeline.yml
# if web/ touched:
cd web && pnpm lint && pnpm typecheck && pnpm build
```
