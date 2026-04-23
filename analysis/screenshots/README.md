# AI Data Analyst Screenshots

This directory holds the canonical Bruin AI Data Analyst screenshots that
feed the `/analysis` page of the site.

The canonical prompts listed in
`pipeline/fixtures/analysis_gallery.csv` are answered from the checked-in
Bruin marts and rendered into this directory as local analysis captures.
Each file is tied to `dataset_version`, `methodology_version`,
`capture_date`, and `capture_source` through `mart.analysis_examples`.

The matching `web/public/screenshots/*.png` copies are served by the Astro
site; keep the two directories in sync on every screenshot refresh.

## Canonical prompts

| File | Prompt summary |
| --- | --- |
| `Q1_flagged_npm.png` | Flagged npm packages with evidence |
| `Q2_flagged_pypi.png` | Flagged PyPI packages with evidence |
| `Q3_grouped.png` | Flagged packages grouped by ecosystem / tier |
| `Q4_elevated_not_flagged.png` | High-fragility packages missing the flag gate |
| `Q5_archived.png` | Archived / deprecated exclusions |
| `Q6_unmappable.png` | Unmappable repositories |
| `Q7_cross_ecosystem.png` | Cross-ecosystem top-flagged comparison |
| `Q8_tier_changes.png` | Severity-tier changes between methodology-compatible weeks |
