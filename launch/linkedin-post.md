# LinkedIn post

> Outstanding Project path requires Top-10 likes for draw eligibility. This post is longer-form than Slack; the hook still comes first.

## Structure

1. **Hook** (headline finding, same as Slack, reformatted for LinkedIn tone).
2. **What I built** (2–3 sentences).
3. **Bruin features I used** — pull from `bruin-comparison.md`. Mention by name: ingestion assets, BigQuery + DuckDB SQL transformations, quality checks (built-in + custom SQL), asset DAG / lineage, `bruin render` for materialization introspection, AI context + Bruin AI Data Analyst.
4. **Design choices worth explaining**:
   - Why weekly, not daily.
   - Why conservative flagged thresholds (false positives cost more than false negatives).
   - Why ecosystem-relative percentiles instead of a global rank.
   - Why static site + precomputed JSON instead of a hosted backend.
5. **How Bruin compares to a stitched-together stack** — pull summary from `bruin-comparison.md`.
6. **How The Bus Factor differs from SCA tools** — pull from `market-positioning.md`. One-liner: "Commercial SCA answers 'what is risky in my application?' — The Bus Factor answers 'which widely depended-on packages look structurally fragile this week?'"
7. **Screenshots**: weekly card + 2–3 Bruin AI Data Analyst captures.
8. **Links**: GitHub repo + hosted demo.

## Guardrails

- Same tone rules as Slack.
- Credit OpenSSF Scorecard as a complementary signal, not a substitute.
- No claim of replacing Snyk/Sonatype/Endor/Socket.
