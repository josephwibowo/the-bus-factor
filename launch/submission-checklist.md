# Competition submission checklist

> Every box green before **June 1, 2026 at 12:00 UTC**.
>
> Verified on **2026-04-23**. The remaining launch blocker is still the
> full live export baseline; the committed public bundle is fixture-backed.

## Repo

- [x] Public GitHub repo.
- [x] README renders on github.com.
- [x] README has: pitch, latest weekly card, `Run in 10 minutes` fixture path, `Run live sources` path, Bruin features table with file paths, "How this differs from SCA tools" table, OpenSSF Scorecard named as complementary signal, analysis screenshots, architecture diagram, source limitations, GitHub Pages demo URL.
- [x] `.bruin.yml` is gitignored; `.bruin.yml.example` has placeholders only.
- [x] `pipeline.yml`, assets, checks, fixture data all committed.
- [x] `pipeline/config/scoring.yml` contains all weights; no numbers inlined in SQL/Python.

## Pipeline

- [x] `bruin validate pipeline/pipeline.yml` succeeds.
- [x] Fixture run succeeds end-to-end with no API keys: `bruin run --full-refresh -e fixture`.
- [x] Known-state custom check passes (expected-flag and expected-not-flag packages match fixture expectations).
- [x] At least one custom SQL check returns offending rows on intentionally bad data, proving enforcement.
- [ ] Live MVP run has succeeded OR the latest successful live export is committed to `public-data/` with source-health metadata.
- [x] `mart_analysis_examples` has 5–10 canonical prompts captured.

## Demo

- [x] GitHub Pages site loads without credentials.
- [x] Home viewport shows project name, reporting window, freshness, and leaderboard (or honest empty state).
- [x] Package detail pages linkable by stable slug.
- [x] Weekly card visible on `/weekly` and exported as a static image with a permalink.
- [x] Analysis page renders local screenshot files (no remote links).

## Launch

- [x] `slack-post.md` finalized with repo URL, demo URL, weekly report link, Bruin feature summary, and the headline finding.
- [x] `linkedin-post.md` finalized with Bruin features, design choices, Bruin-vs-stitched-stack paragraph, SCA-vs-Bus-Factor paragraph, and analysis screenshots.
- [x] `bruin-comparison.md` completed.
- [x] `market-positioning.md` completed.
- [x] `maintainer-response.md` reviewed.
- [x] Flagged list manually reviewed for tone / false-positive risk.
- [ ] Official submission metadata ready: project name, description, GitHub URL, Slack thread URL placeholder, LinkedIn URL placeholder.

## Post-launch (during the voting window)

- [ ] Monitor Slack thread; respond to maintainer objections using the template.
- [ ] Pin the weekly card image in the Slack thread.
- [ ] LinkedIn post published same day as Slack for cross-visibility.
