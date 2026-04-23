# AGENTS.md — `analysis/`

Gallery of Bruin AI Data Analyst screenshots. See repo-root [`AGENTS.md`](../AGENTS.md) for baseline rules.

## Hard rules

1. **Prompts come from [`prompts.md`](prompts.md) verbatim.** Do not invent new prompts for the gallery without updating that file first.
2. **Never build a custom NL/chat interface.** Analysis is delivered through Bruin AI Data Analyst (local MCP or Bruin Cloud). The site only *displays* screenshots of those outputs.
3. **Every screenshot is tied to a dataset version, methodology version, and capture date.** Add the row to `mart_analysis_examples` in the same commit.
4. **Store local PNG/WebP only** under `screenshots/`. No remote / private / expiring links.
5. **File naming**: `<prompt-slug>__<methodology_version>__<YYYY-MM-DD>.<png|webp>`.
6. **Outdated screenshots get retired**, not edited. When `methodology_version` bumps, recapture the gallery.
7. **At least one screenshot must show the AI analyst explaining an exclusion or a non-flagged elevated package**, not only top-ranked items. This proves the grounding isn't cherry-picked.
