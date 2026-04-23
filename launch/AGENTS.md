# AGENTS.md — `launch/`

Competition launch package. See repo-root [`AGENTS.md`](../AGENTS.md) for baseline rules; the authoritative spec is [`docs/init.md`](../docs/init.md) §"Competition launch package".

## Why this directory is first-class

The competition is two popularity contests:

1. Top 3 → most 👍 reactions on the Slack `#projects` post.
2. Outstanding Project → most LinkedIn likes → random draw among top 10.

The Slack post and LinkedIn post are therefore **north-star artifacts written before the pipeline** and iterated as the data takes shape. If an architectural decision strengthens the pipeline but weakens the launch post, it loses.

## Files

| File | Purpose |
| --- | --- |
| `slack-post.md` | 30-second community-voter conversion surface. Leads with the single headline finding (one striking, data-backed statistic). Includes repo URL, demo URL, weekly card, concise Bruin feature summary. Never names commercial tools in the hook. |
| `linkedin-post.md` | Explains Bruin features used, design choices, how Bruin compares to a stitched stack (pulls the table from `bruin-comparison.md`), how The Bus Factor differs from SCA (pulls from `market-positioning.md`), and embeds Bruin AI Data Analyst screenshots. |
| `bruin-comparison.md` | Completed table: Bruin vs a stitched-together data stack (ingestion + transformation + orchestration + checks + AI context + lineage). |
| `market-positioning.md` | The Bus Factor vs SCA platforms / Socket-style supply-chain tooling / OpenSSF Scorecard. |
| `maintainer-response.md` | Pre-drafted measured response template for handling maintainer objections in the Slack thread. Do not improvise live. |
| `submission-checklist.md` | Final verification pass before submission (repo public, README renders, Pages demo works, screenshots load, fixture run succeeds, submission metadata ready). |

## Copy rules (enforced as review, not aspiration)

1. Never: abandoned, dead, negligent, neglected, unsafe.
2. Always: flagged, watch, elevated, fragility signals, evidence, continuity risk.
3. No named maintainers in Slack post, LinkedIn post, or weekly card.
4. The flagged list is manually reviewed for tone and false-positive risk before the Slack post goes live; launch is held if a sensitive false positive appears.
5. "Bus factor" is explained as a continuity-risk shorthand, not a literal claim about people.

## Deadline

**June 1, 2026 at 12:00 UTC.** `submission-checklist.md` must be green before this moment.
