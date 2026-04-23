# Canonical Bruin AI Data Analyst prompts

Every screenshot in `screenshots/` must use one of the prompts below **verbatim**, and record (prompt, answer, `dataset_version`, `methodology_version`, `capture_date`, and whether the capture came from Bruin AI Data Analyst or the local grounded-analysis render path) in `mart_analysis_examples`.

The gallery must avoid open-ended prompts that ask the analyst to infer maintainer intent, burnout, or motivation. Acceptable prompt types are: ranking, filtering, grouping, comparison, and explaining displayed evidence.

## Primary prompts (v1 — use these first)

1. Which currently flagged npm packages have the highest risk score, and what evidence explains each score?
2. Which currently flagged PyPI packages have the highest risk score, and what evidence explains each score?
3. Group the current flagged packages by ecosystem and severity tier.
4. Which high-importance packages have elevated fragility signals but are not flagged, and why are they below the threshold?
5. Which packages were excluded because they are archived or deprecated, and why are they not ranked?
6. Which tracked packages are excluded because their repository mapping confidence is too low?
7. Compare the top flagged npm and PyPI packages by importance score, fragility score, confidence, and primary findings.
8. Which packages changed severity tier since the previous methodology-compatible weekly snapshot?

## Fallback prompts (use only when the conservative threshold yields too few flagged packages)

A. Which high-importance packages are currently in the Elevated tier, and what evidence kept them below flagged status?
B. Which excluded packages would have been important if they had a confident repository mapping?
C. Which source coverage gaps most affect confidence in this week's snapshot?

## Capture workflow

1. Run the pipeline (fixture or live).
2. Note the `methodology_version` and `snapshot_week` from `public-data/metadata.json`.
3. Run the prompt through the local Bruin analysis workflow.
4. Preferred capture path: Bruin MCP in your AI coding tool (or Bruin Cloud AI Data Analyst, if available). Fallback capture path: a local grounded render backed by the same marts.
5. Screenshot the full response (prompt + answer + enough table context to prove grounding).
6. Save as `screenshots/<prompt-slug>__<methodology_version>__<YYYY-MM-DD>.png`.
7. Append a row to `mart_analysis_examples` via the export asset.
