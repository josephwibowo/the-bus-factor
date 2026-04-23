# AGENTS.md — `web/`

Astro static showcase. See repo-root [`AGENTS.md`](../AGENTS.md) for baseline rules.

## Hard constraints

1. **Static output only.** `output: "static"` in `astro.config.mjs` — do not change. No SSR adapters, no API routes, no serverless functions, no middleware that runs at request time.
2. **The only data source is `../public-data/*.json`.** Do not fetch from remote endpoints at runtime. Do not embed API keys. Do not re-derive scores.
3. **Build-time JSON-schema validation.** Every artifact consumed from `public-data/` must be validated during `astro build`. If the bundle is missing or malformed, render an honest empty state; do not fabricate rankings.
4. **No named maintainers in viral surfaces** (home leaderboard, weekly card, social meta tags). Package detail pages may expose maintainer info behind a click.
5. **Accessibility baseline**: semantic HTML, visible focus states, color contrast AA, keyboard-navigable leaderboard.
6. **No heavy client runtime.** Islands for search/filter only. No framework-wide hydration.
7. **Deployment target is GitHub Pages.** The site must load without credentials, env vars, or runtime services.

## Expected pages (per spec §"Information architecture")

| Route | Purpose | Source JSON |
| --- | --- | --- |
| `/` | Leaderboard, ecosystem tabs, search | `leaderboard.json`, `metadata.json` |
| `/package/[slug]` | Package detail + evidence + score inputs | `packages.json` (or shards) |
| `/weekly` | Current weekly report narrative + card | `weekly_report.json` |
| `/analysis` | Bruin AI Data Analyst screenshot gallery | `analysis_gallery.json` |
| `/methodology` | Scoring philosophy, exclusions, coverage | `coverage.json` |
| `/positioning` | How The Bus Factor differs from SCA / Socket / Scorecard | `market_positioning.json` |

## Tone in copy

Use: flagged, watch, elevated, fragility signals, evidence, continuity risk.
Avoid: abandoned, dead, negligent, neglected, unsafe.
Always frame: "public-data signal," not a verdict.

## Commands

```bash
pnpm --filter ./web dev       # local dev server
pnpm --filter ./web build     # static build → web/dist
pnpm --filter ./web preview   # serve dist/ locally
pnpm --filter ./web test:smoke  # Playwright (after UI exists)
```

## Deferred for v1

- DuckDB-Wasm / client-side SQL. Nice-to-have, adds bundle/build/UX risk. Revisit after the demo, README, weekly report, and AI gallery are all shipped.
