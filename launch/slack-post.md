# Slack `#projects` launch post

> Draft this **before** the pipeline. Iterate as the data takes shape. Lead with the single headline finding.

## Target audience

Bruin Slack community. Reviewers, data engineers, and maintainers who will react in under 60 seconds.

## Structure

1. **Headline finding** (one striking data-backed sentence). Example placeholder:
   > _"Of the top 500 most-depended-on npm packages, X look structurally fragile this week — all have shipped no release in 6+ months and show declining commit activity from a single top contributor."_
2. **What it is**, in one sentence. "A Bruin-powered weekly snapshot of continuity fragility for the most-depended-on npm and PyPI packages."
3. **Demo + repo links** (weekly card image pinned as the first attachment).
4. **Bruin feature summary**, 3–5 bullets (ingestion, transformations, quality checks, AI Data Analyst, lineage).
5. **Methodology trust line** ("ecosystem-relative, conservative thresholds, two independent signals required to flag, maintainers not individually named").
6. **Soft ask**: "React if you found this useful; open an issue if your package shows up and looks wrong — we have a measured response template and will iterate."

## Guardrails

- Do not name commercial SCA tools in the hook. Comparison lives in README/LinkedIn.
- Do not claim any package is abandoned, dead, or unsafe.
- No named maintainers in the post.
- Single headline finding must be replicable from the pipeline output that ships with the post.

## Final check before posting

- [ ] Demo URL loads without credentials.
- [ ] Repo is public, README renders, fixture run succeeds.
- [ ] Flagged list manually reviewed for false positives.
- [ ] `maintainer-response.md` is open in a side tab.
