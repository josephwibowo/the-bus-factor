# LinkedIn post

## Final draft

The current committed `2026-W17` snapshot in **The Bus Factor** flags **8 packages for continuity risk** across npm and PyPI, and it only does that after clearing conservative, ecosystem-relative gates: high downstream importance, medium-or-higher confidence, and at least two independent fragility signals.

I built The Bus Factor as a **Bruin-powered data engineering project** for the Bruin competition: one public pipeline that ingests package, repository, dependency, security, and source-health signals; scores weekly "importance × fragility" snapshots; exports static JSON; and publishes a static showcase with a leaderboard, package detail pages, a weekly card, and a canonical analysis gallery.

What Bruin made materially easier:

- Python ingestion assets and export assets live in the same DAG as the SQL work
- The same graph runs against a reproducible DuckDB fixture and a BigQuery live environment
- Quality checks are inline, including custom SQL checks that block downstream exports when fixture expectations drift
- Lineage comes from the same asset declarations that run the pipeline
- `bruin ai enhance` helped fill mart metadata so the analysis layer is better grounded
- The repo includes exported prompt captures instead of a custom chat UI

Three design choices matter here:

1. Weekly instead of daily, because continuity signals move slower than incident-response metrics and weekly snapshots reduce noise.
2. Conservative thresholds, because false positives cost more than false negatives in a public package-risk surface.
3. Static site plus precomputed JSON, because the judged deliverable is the reproducible Bruin pipeline and repo, not a hosted backend.

The Bruin-vs-stitched-stack takeaway is simple: this project would normally need separate tooling for orchestration, SQL transformations, tests, lineage, docs, and AI-ready metadata. Bruin keeps those concerns on one asset graph, which makes the reviewer path much shorter and the repo much easier to audit.

The market-positioning point is also deliberate: commercial SCA answers "what is risky in my application?" The Bus Factor answers "which widely depended-on packages look structurally fragile this week, and what evidence supports that?" OpenSSF Scorecard is one input signal in the model, not a substitute for the weekly snapshot.

Repo: [https://github.com/josephwibowo/the-bus-factor](https://github.com/josephwibowo/the-bus-factor)  
Demo: [https://josephwibowo.github.io/the-bus-factor](https://josephwibowo.github.io/the-bus-factor)

## Screenshot set to attach

- `web/public/weekly/latest.png`
- `docs/images/bruin-lineage-hero.png`
- `docs/images/bruin-custom-check-failure.png`
- `analysis/screenshots/Q1_flagged_npm.png`
- `analysis/screenshots/Q8_tier_changes.png`

## Guardrails

- Do not claim the project replaces Snyk, Sonatype, Endor, or Socket.
- Do not call any package abandoned, dead, negligent, or unsafe.
- Do not name individual maintainers in the post copy.
