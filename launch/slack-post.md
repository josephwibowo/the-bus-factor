# Slack `#projects` launch post

Use the weekly card image as the first attachment.

## Final draft

The current committed `2026-W17` snapshot in **The Bus Factor** flags **8 packages for continuity risk** across npm and PyPI, using ecosystem-relative thresholds and requiring at least **two independent fragility signals** before anything is flagged.

The Bus Factor is a **Bruin-powered weekly snapshot of open-source package continuity fragility** for widely depended-on npm and PyPI packages.

Demo: [https://josephwibowo.github.io/the-bus-factor](https://josephwibowo.github.io/the-bus-factor)  
Repo: [https://github.com/josephwibowo/the-bus-factor](https://github.com/josephwibowo/the-bus-factor)  
Weekly report: [https://josephwibowo.github.io/the-bus-factor/weekly/](https://josephwibowo.github.io/the-bus-factor/weekly/)

Bruin is doing the heavy lifting here:

- Python ingestion assets for public-source collection
- DuckDB and BigQuery transformations on the same asset graph
- Built-in checks plus custom SQL checks that block downstream exports
- Lineage and AI context from the same DAG the pipeline runs
- Exported analysis captures grounded in the curated marts

Methodology line: the scoring is ecosystem-relative, conservative by design, requires medium-or-higher confidence plus two independent signals to flag, and never names maintainers individually on public surfaces.

If you find this useful, react here. If a package looks wrong, open an issue and I’ll review the evidence row or repo mapping with the public maintainer-response template.

## Posting notes

- Keep the weekly card pinned as the first image.
- Link the repo and hosted demo in the body, not only in the thread.
- Do not name individual packages or maintainers in the hook.
