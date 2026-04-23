# Competition submission checklist

> Every box green before **June 1, 2026 at 12:00 UTC**.

## Repo

- [ ] Public GitHub repo.
- [ ] README renders on github.com.
- [ ] README has: pitch, latest weekly card, `Run in 10 minutes` fixture path, `Run live sources` path, Bruin features table with file paths, "How this differs from SCA tools" table, OpenSSF Scorecard named as complementary signal, Bruin AI Data Analyst screenshots, architecture diagram, source limitations, GitHub Pages demo URL.
- [ ] `.bruin.yml` is gitignored; `.bruin.yml.example` has placeholders only.
- [ ] `pipeline.yml`, assets, checks, fixture data all committed.
- [ ] `pipeline/config/scoring.yml` contains all weights; no numbers inlined in SQL/Python.

## Pipeline

- [ ] `bruin validate pipeline/pipeline.yml` succeeds.
- [ ] Fixture run succeeds end-to-end with no API keys: `bruin run --full-refresh -e fixture`.
- [ ] Known-state custom check passes (expected-flag and expected-not-flag packages match fixture expectations).
- [ ] At least one custom SQL check returns offending rows on intentionally bad data, proving enforcement.
- [ ] Live MVP run has succeeded OR the latest successful live export is committed to `public-data/` with source-health metadata.
- [ ] `mart_analysis_examples` has 5–10 canonical prompts captured.

## Demo

- [ ] GitHub Pages site loads without credentials.
- [ ] Home viewport shows project name, reporting window, freshness, and leaderboard (or honest empty state).
- [ ] Package detail pages linkable by stable slug.
- [ ] Weekly card visible on `/weekly` and exported as a static image with a permalink.
- [ ] Analysis page renders local screenshot files (no remote links).

## Launch

- [ ] `slack-post.md` finalized with repo URL, demo URL, weekly card, Bruin feature summary, and the headline finding.
- [ ] `linkedin-post.md` finalized with Bruin features, design choices, Bruin-vs-stitched-stack paragraph, SCA-vs-Bus-Factor paragraph, and AI Data Analyst screenshots.
- [ ] `bruin-comparison.md` completed.
- [ ] `market-positioning.md` completed.
- [ ] `maintainer-response.md` reviewed.
- [ ] Flagged list manually reviewed for tone / false-positive risk.
- [ ] Official submission metadata ready: project name, description, GitHub URL, Slack thread URL placeholder, LinkedIn URL placeholder.

## Post-launch (during the voting window)

- [ ] Monitor Slack thread; respond to maintainer objections using the template.
- [ ] Pin the weekly card image in the Slack thread.
- [ ] LinkedIn post published same day as Slack for cross-visibility.
