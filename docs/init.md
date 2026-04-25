# Project Spec — The Bus Factor

## Overview / Purpose

The Bus Factor is a Bruin-powered data engineering project that identifies widely used open source packages whose public maintenance signals look fragile relative to their systemic importance. It ingests public package, repository, dependency, and security data; transforms those inputs into a scored weekly snapshot; and publishes a simple public showcase consisting of ecosystem-grouped leaderboards, package detail pages, a weekly report, and a gallery of Bruin AI Data Analyst answers. The problem it solves is that critical package risk is scattered across GitHub, registries, dependency datasets, and security feeds, making it hard for engineers and data professionals to answer a simple question: “Which important packages have the lowest margin for maintenance continuity right now, and why?”

---

## Contest context and scope

This project is explicitly scoped for the **Bruin Data Engineering Project Competition**. The judged deliverable is not just a website; it is a public, reproducible **Bruin pipeline** that demonstrates ingestion, transformation, orchestration, quality checks, and analysis using **Bruin AI Data Analyst**, shipped as a GitHub repo with a strong README and supported by a weekly shareable artifact, a Slack launch post, a website submission, and a LinkedIn post for the Outstanding Project path. The hosted web experience is intentionally narrow: it showcases the output of the pipeline, the methodology, and screenshots of the AI analysis rather than implementing its own custom natural-language interface.

The competition deadline is **June 1, 2026 at 12:00 UTC**. The project should optimize for two parallel outcomes:

1. **Qualification and community voting (Top 3)** — GitHub repo with README, Slack `#projects` post, and official competition submission. Winners are selected by **most 👍 reactions on the Slack post**.
2. **Outstanding Project eligibility** — LinkedIn post explaining Bruin features used, design choices, how Bruin compares to other tools, and screenshots of Bruin AI Data Analyst analysis. **Top 10 posts by likes enter a random draw** for the Outstanding prize.

Because winners depend heavily on community attention, the README, Slack post, website hero, weekly card, and LinkedIn narrative are competition-critical deliverables, not polish extras.

### Virality-first framing

Both judged paths are popularity contests. Slack reactions decide Top 3; LinkedIn likes decide the Outstanding draw pool. Data engineering quality is the **credibility substrate** that lets readers trust the project enough to react — it is not itself the judged output. The project is built with two co-equal priorities:

1. A technically serious Bruin pipeline that withstands expert scrutiny.
2. A launch moment engineered to earn reactions in under 60 seconds.

Operationally this means:

1. The Slack launch post and LinkedIn post drafts are written **before** the pipeline is built, not after. They become the north-star artifacts that every architectural decision reinforces.
2. The project commits to a single **headline finding** — one striking, data-backed statistic — that anchors the Slack post, the LinkedIn post, and the weekly card.
3. Any design choice that would make the pipeline marginally more rigorous but the launch post less punchy is evaluated against the virality cost before shipping.

### Maintainer-backlash risk

Publicly labeling open-source packages as "fragile" carries a direct risk of offending Slack community members whose packages appear in the leaderboard. A single maintainer complaint in the Slack thread can cost votes. Mitigations:

1. Tone and naming guardrails (see UX principles) are enforced as copy review, not just aspiration.
2. The flagged threshold is deliberately conservative to minimize false positives.
3. A pre-drafted response template for maintainer objections lives in `launch/maintainer-response.md` so any thread response is measured and consistent.
4. The flagged list is reviewed manually before the Slack post goes live; the launch is held if a particularly sensitive false positive appears.

---

## Competitive positioning

The Bus Factor sits near open-source risk tooling, but it is not trying to replace commercial software composition analysis, dependency scanning, or repository security posture tools. This distinction must be explicit in the README, website methodology page, and LinkedIn post because the likely objection is: “Don’t Snyk, Sonatype, Endor, Socket, or Scorecard already do this?” The Slack launch should stay focused on the hook, with the comparison available one click away and in the maintainer/objection response materials.

### Category taxonomy

| Category | Examples | What they primarily do | Relationship to The Bus Factor |
| --- | --- | --- | --- |
| Commercial SCA / open-source risk platforms | Snyk, Sonatype, Endor Labs | Scan an organization’s applications or repositories, inventory dependencies, prioritize vulnerabilities/license/risk issues, and support remediation workflows. | Closest broad product category, but not the same deliverable. The Bus Factor is a public, reproducible dataset and competition showcase, not an enterprise scanner or remediation product. |
| Supply-chain attack and malicious-package detection | Socket | Detect malicious-package, typosquat, risky install behavior, unstable ownership, and package-behavior alerts. | Adjacent inspiration for package-level heuristics, but The Bus Factor focuses on continuity/maintenance fragility weighted by systemic importance, not malware or install-time behavior. |
| Repository security posture checks | OpenSSF Scorecard | Run automated checks against repositories for security posture and development-practice risk. | Complementary source signal. Scorecard is an input to The Bus Factor, not a substitute for the package-level pipeline or public weekly report. |
| Public competition data product | The Bus Factor | Combines public package demand, dependency reach, repository activity, release behavior, source health, and Scorecard-style signals into a weekly, explainable, Bruin-powered public snapshot. | The actual project category. Built to demonstrate Bruin and earn community attention, not to sell enterprise SCA. |

### Differentiation claim

The Bus Factor is best framed as:

> A public, reproducible “importance × continuity fragility” dataset for widely used packages, built with Bruin and explained through a static showcase plus Bruin AI Data Analyst examples.

It should not be framed as:

1. A replacement for Snyk, Sonatype, Endor, or Socket.
2. A commercial SCA platform.
3. A dependency scanner for a user’s private application.
4. A malware detector.
5. A definitive abandoned-package classifier.

### Why this can still win

The fact that related tools exist is a feature for the competition story, not a weakness. It lets the project say:

1. The professional market already agrees open-source risk matters.
2. Most commercial tools answer “what is risky in my application?”
3. The Bus Factor answers a different public-interest question: “which widely depended-on packages look structurally fragile this week, and what evidence supports that?”
4. Bruin is the visible differentiator: one project shows ingestion, transformations, checks, lineage, AI context, AI Data Analyst screenshots, static exports, and a public data product.

### Required positioning artifacts

1. README must include a “How this differs from SCA tools” table.
2. LinkedIn post must include a short paragraph contrasting Bruin-built public analysis with commercial SCA platforms and Scorecard.
3. Methodology page must state that commercial products are not used as source data.
4. Source documentation must identify OpenSSF Scorecard as a complementary public signal source.
5. Slack post must avoid naming commercial tools in the hook; the comparison belongs in README/LinkedIn for people who click through.

---

## Goals and non-goals

### Goals

1. Build a credible, reproducible Bruin pipeline that ingests, transforms, scores, and publishes package-maintenance-risk data.
2. Publish a GitHub repository and README that stand on their own as a strong data engineering project, even if a reviewer never opens the hosted site.
3. Produce a simple public showcase that proves the data is real and useful: leaderboard, package detail, weekly report, methodology.
4. Use Bruin AI Data Analyst as the analysis surface and capture canonical example analyses for reviewers and social sharing.
5. Generate a weekly shareable artifact designed to earn Slack reactions and LinkedIn engagement.
6. Keep the system free-tier friendly by limiting scope, cadence, and infrastructure complexity.
7. Optimize for credibility and low false positives over exhaustive coverage.
8. Make Bruin usage obvious in the repo, screenshots, README, and launch materials.
9. Package the project for Slack community voting and the Outstanding Project LinkedIn path.
10. Commit to a single striking, data-backed headline finding that anchors the Slack post, LinkedIn post, and weekly card.
11. Demonstrate Bruin features that surface well in screenshots — lineage/asset graph, AI context descriptions, variable-driven environment swap, rendered materialization SQL, and custom checks — not only the baseline ingestion/transformation/orchestration/analysis surface.
12. Clearly differentiate The Bus Factor from SCA platforms, malicious-package detectors, and repository posture tools.

### Non-goals

1. This is not a full open source risk platform for enterprise dependency management.
2. This is not a definitive statement that a package is abandoned.
3. This does not infer maintainer psychology, intent, or burnout.
4. This does not build a custom chat or NL query backend.
5. This does not require login, collaboration, or user accounts.
6. This does not support real-time or daily refresh in v1.
7. This does not ship a global cross-ecosystem ranking as a core experience.
8. This does not expose named maintainers in viral or summary surfaces.
9. This does not optimize for full ecosystem coverage beyond the defined tracked universe.
10. This does not scan a user’s private application dependency tree.
11. This does not provide vulnerability remediation, license-policy enforcement, pull requests, or enterprise workflow integrations.
12. This does not use commercial SCA platforms as source data.

---

## Target user and core use cases

### Target user

The primary user is a technically literate engineer, data professional, security-minded developer, engineering leader, or open-source-curious operator who wants a fast, credible way to understand which critical packages look fragile.

### Core use cases

1. A reviewer wants to inspect the pipeline and repo so they can judge the project as a serious data engineering submission.
2. A user wants to see which packages are currently most concerning so they can understand ecosystem risk at a glance.
3. A user wants to inspect one package so they can understand why it was flagged.
4. A user wants to see the current weekly Bus Factor report so they can share the output with their team or on social media.
5. A user wants to see examples of questions answered by Bruin AI Data Analyst so they can understand how the dataset can be analyzed without writing SQL.
6. A Slack community voter wants to understand the project and react within 30 seconds.
7. A LinkedIn reader wants to understand what Bruin did, why the design choices matter, and how the project compares to a stitched-together data stack.
8. A skeptical reader wants to know how The Bus Factor differs from SCA products, Socket-style supply-chain tooling, and OpenSSF Scorecard.

---

## Key user flows

### Flow 1: Reviewer validates the project

Reviewer opens the GitHub repo → reads the README → understands the project pitch, data sources, Bruin features used, architecture, and how to run locally → runs the tiny fixture pipeline locally with Bruin CLI and DuckDB → sees that the weekly snapshot, score table, and exported artifacts are produced → optionally inspects the live BigQuery-backed run and hosted static outputs.

### Flow 2: User explores the showcase

User opens the public site → sees the latest weekly flagged leaderboard and freshness status → opens a package → sees the evidence behind the score and methodology explanation.

### Flow 3: User reviews the weekly artifact

User opens the weekly report page → sees top flagged packages and why they matter → shares the report or the generated image card.

### Flow 4: User inspects the analysis experience

User opens the Analysis page → sees a gallery of canonical Bruin AI Data Analyst screenshots with the exact question asked and answer returned → follows a CTA explaining that analysis is performed through Bruin AI Data Analyst over the curated dataset, not a custom chat interface.

### Flow 5: User investigates exclusions

User opens Methodology / Coverage → sees which packages are excluded from ranking because they are unmappable, archived/deprecated, or too young to be flagged.

### Flow 6: Community voter reacts

Community voter sees the Slack `#projects` post → immediately understands the hook, the Bruin features used, the demo link, and the GitHub link → opens either the weekly card or hosted showcase → reacts to the Slack post.

### Flow 7: Outstanding Project reviewer follows the LinkedIn story

LinkedIn reader sees a post with the weekly artifact and Bruin AI Data Analyst screenshots → reads the short architecture/design explanation → understands how Bruin replaces separate ingestion, transformation, orchestration, quality, and analysis glue → follows the repo or demo link.

---

## Feature list / functional requirements

### A. Bruin pipeline

**Must**

1. The project must use Bruin for ingestion, transformation, orchestration, and analysis.
2. The pipeline must produce a reproducible weekly snapshot of package-level signals and scores.
3. The fixture pipeline must run locally using Bruin CLI and DuckDB without requiring external cloud infrastructure.
4. The pipeline must include data quality checks at staging and curated layers.
5. The pipeline must stamp each scored snapshot with a `methodology_version`.
6. The pipeline must export a curated artifact suitable for both the web showcase and Bruin AI Data Analyst analysis.
7. The repo must include Bruin project structure at the repo root: `.bruin.yml.example`, `.gitignore` entry for `.bruin.yml`, pipeline folder, `pipeline.yml`, and version-controlled assets.
8. The pipeline must visibly use Python assets for source extraction, BigQuery SQL assets for live transformations, and DuckDB SQL assets for fixture transformations.
9. Asset dependencies must be declared explicitly so the Bruin DAG is reviewable without reading orchestration glue code.
10. Materialization strategy must be intentional and documented: append/merge for source snapshots where practical, create/replace for deterministic derived marts.
11. Quality checks must include built-in checks such as `not_null`, `unique`, accepted values, and non-negative numeric fields, plus custom SQL checks for scoring-specific invariants.
12. CI must run `bruin validate` and a small fixture-mode pipeline run before publishing artifacts.
13. The project must include Bruin AI context assets or generated descriptions sufficient for Bruin AI Data Analyst to answer grounded questions without guessing table meaning.
14. The README must include a Bruin feature proof table showing exactly where ingestion, transformation, orchestration, quality checks, AI analysis, and optional lineage are implemented.
15. The repo must include a small fixture dataset and fixture run path so reviewers can reproduce the pipeline quickly even if live APIs are slow, rate-limited, or unavailable.
16. The fixture dataset must include a **known-state validation set**: 5–10 packages expected to flag and 5–10 expected not to flag per ecosystem, with rationales recorded in `pipeline/fixtures/known_states.md`. A custom SQL check asserts scoring agrees with these expectations on every fixture run.
17. The pipeline must use **Bruin variables** to toggle fixture vs live-source mode so the same asset graph runs in both environments without code duplication.
18. Mart tables and key columns must include Bruin-style descriptions suitable for **AI context**, so the AI Data Analyst gallery demonstrably benefits from grounded metadata rather than guessing.
19. The pipeline must emit a Bruin lineage artifact (VS Code extension screenshot or generated DAG diagram) used as the README hero image.
20. All scoring weights and thresholds must live in `pipeline/config/scoring.yml` and be read by the SQL and Python assets rather than hard-coded.

**Should**
1. The pipeline should be schedulable weekly through GitHub Actions.
2. The pipeline should publish artifacts for the hosted showcase and weekly social card.
3. The README should include a Bruin lineage screenshot as its visual hero, plus a generated DAG diagram if useful.
4. The full live-source pipeline should be runnable separately from the fixture pipeline.
5. The pipeline should demonstrate at least one **custom SQL check** that returns the offending rows or an explicit failure count, so reviewers can see Bruin enforcing business-specific data rules.

### B. Public showcase

**Must**
1. The public site must show the latest flagged leaderboard drawn from the tracked universe.
2. Because scores are ecosystem-relative, the leaderboard must default to ecosystem-grouped sections or ecosystem tabs rather than implying one absolute npm-vs-PyPI rank.
3. The leaderboard must show package name, ecosystem, risk score, severity tier, confidence, and short explanation.
4. The site must support search by package name.
5. The site must support ecosystem filtering.
6. The site must provide a package detail page with score explanation and evidence.
7. The site must provide a weekly report page.
8. The site must provide a methodology / coverage page.
9. The site must provide an analysis page that displays example Bruin AI Data Analyst screenshots and prompts.
10. The site must display freshness and reporting-window information.
11. The site must be buildable as static frontend assets and deployable to a free public demo host.

**Should**
1. The site should include annotated tables for archived/deprecated and unmappable packages on the methodology / coverage page.
2. The site should include one “Bruin pipeline proof” page or README-linked section showing the asset graph, checks, and exported data contract.

### C. Analysis via Bruin AI Data Analyst workflow

**Must**
1. Analysis must be delivered through Bruin's AI Data Analyst workflow over the curated dataset: Bruin-generated context plus Bruin MCP with an AI coding assistant for the local path, or Bruin Cloud AI Data Analyst if a free or competition-provided path is available.
2. The project must not implement its own custom NL interface.
3. The public showcase must include 5–10 canonical example question/answer screenshots from Bruin AI Data Analyst.
4. Each example must show the exact question asked and the grounded result.
5. Each screenshot must be tied to a dataset version, methodology version, and capture date.
6. The README and/or showcase must explain whether each screenshot came from the local Bruin MCP path or Bruin Cloud AI Data Analyst.

**Should**
1. Example questions should cover filtering/ranking, aggregation, and comparison only.
2. At least one example should demonstrate Bruin AI Data Analyst explaining an exclusion or non-flagged elevated package, not only top-ranked packages.

### D. Weekly artifact and virality

**Must**
1. The project must generate a weekly static image artifact suitable for LinkedIn/X/Slack sharing.
2. The artifact must summarize the week’s most notable flagged packages or explicitly state when no high-confidence packages were flagged.
3. The site must provide a permalink to the full weekly report behind the artifact.
4. The project must produce a LinkedIn-ready narrative with screenshots of Bruin AI Data Analyst analysis as part of the submission workflow.
5. The project must produce a Slack-ready launch post optimized for quick community understanding and reactions.

**Should**
1. The artifact should be generated automatically by the pipeline or CI.
2. The artifact should be visually strong enough to stand alone without the README.
3. The Slack post should lead with a concrete finding or striking coverage statistic, not with implementation details.

### E. Repository and README

**Must**
1. The repo must contain the pipeline, showcase app, workflow config, README, methodology documentation, fixture data, exported public artifacts, and launch-material drafts.
2. The README must include the project pitch, architecture diagram, data sources, how to run locally, Bruin features used, screenshots of Bruin AI Data Analyst answers, methodology summary, and links to the hosted showcase and weekly report.
3. The README must include a “Run in 10 minutes” path using fixture data and a separate “Run live sources” path.
4. The README must include a “Bruin features used” table with file/path references.
5. The README must include source limitations, API-token expectations, and rate-limit notes.
6. The README must include a “How this differs from SCA tools” table using the taxonomy in “Competitive positioning.”
7. The README must name OpenSSF Scorecard as a complementary signal source, not a direct substitute.
8. The repo structure must make it obvious where the pipeline, data artifacts, and web app live.

**Should**
1. The repo should be understandable to a community voter in under 10 minutes.
2. The README should start with the latest weekly card and the shortest credible explanation of why the project matters.

### F. Competition launch package

**Must**

1. The Slack and LinkedIn drafts must be written **before** the pipeline is built. They live in `launch/` from week 1 and are iterated as the data takes shape.
2. The project must include a prepared Slack `#projects` post with GitHub URL, hosted demo URL, weekly card, concise Bruin feature summary, and the committed headline finding.
3. The Slack post must lead with the headline finding — a single striking, data-backed statistic — not with implementation details or a generic leaderboard blurb.
4. The project must include the official submission metadata: project name, description, GitHub URL, Slack thread URL placeholder, LinkedIn profile URL placeholder, and optional LinkedIn post URL placeholder.
5. The project must include a LinkedIn post draft that explains Bruin features used, design choices, how Bruin compares to a stitched-together stack (using the comparison table in this spec as the canonical framing), how The Bus Factor differs from SCA tools, and includes Bruin AI Data Analyst screenshots.
6. The launch materials must include `launch/maintainer-response.md`, a pre-drafted response template for handling maintainer objections in the Slack thread.
7. The launch materials must include `launch/bruin-comparison.md`, a completed comparison table of Bruin vs a stitched-together data stack, derived from the table in the spec.
8. The launch materials must include `launch/market-positioning.md`, a concise comparison of The Bus Factor vs SCA platforms, Socket-style supply-chain tooling, and OpenSSF Scorecard.
9. The launch checklist must include submission before **June 1, 2026 at 12:00 UTC**.
10. The launch checklist must include a final verification pass that the GitHub repo is public, the README renders, the GitHub Pages demo works, screenshots load, and the fixture run succeeds.

---

## Proposed tech stack and development standards

### Bruin-first stack summary

The project is a Bruin project first. Python, BigQuery/DuckDB SQL, and Astro are implementation layers inside or downstream of Bruin:

1. **Bruin CLI/project model** defines the project root, environments, pipeline, assets, variables, materializations, dependencies, quality checks, validation, lineage, and AI context.
2. **Python assets** handle ingestion and custom extraction logic.
3. **BigQuery SQL assets** handle live transformations and marts; **DuckDB SQL assets** keep the fixture path tiny and local.
4. **Bruin quality checks** enforce data contracts after assets run and gate downstream execution.
5. **Bruin-generated public exports** are the only data contract consumed by the static frontend.
6. **BigQuery free tier** is the default live warehouse so live-source data does not accumulate on the developer machine.
7. **DuckDB** is retained for tiny fixture/reviewer runs and optional local smoke checks, not for storing the live MVP dataset by default.
8. **Bruin AI context + MCP / AI Data Analyst** is the only analysis surface; the site displays captured analysis outputs instead of implementing chat.
9. **Astro** is a static presentation layer over Bruin outputs, not the source of business logic.

This ordering should be visible in the README, repo tree, CI, and launch materials.

### Core languages and runtimes

1. **Python 3.12.x** for ingestion assets, source normalization, scoring helpers, export generation, weekly report generation, and share-card generation.
2. **BigQuery SQL** for the default live-source staging, intermediate, and mart transformations.
3. **DuckDB SQL** for fixture-mode staging, intermediate, and mart transformations.
4. **YAML** for Bruin project, pipeline, asset metadata, checks, schedules, and environment configuration.
5. **TypeScript 5.x** for any frontend interactivity, data loading helpers, and build-time validation in the static showcase.
6. **Node.js 24 LTS** for the frontend toolchain.

Python 3.12 is the default because it is mature, widely supported by data libraries, and less likely than newest Python releases to create compatibility friction during a competition build. The exact patch version should be pinned in `.python-version` and CI before final submission.

### Python tooling

1. **Package manager**: `uv`.
2. **Project metadata**: `pyproject.toml` plus checked-in `uv.lock`.
3. **Formatter/linter**: Ruff, using `ruff format --check` and `ruff check`.
4. **Type checking**: mypy for project-owned Python modules and pipeline helper code.
5. **Tests**: pytest for scoring helpers, source normalization, fixture behavior, and export schema tests.
6. **Data contracts**: Pydantic or dataclasses for exported JSON schemas where helpful; DuckDB table contracts remain enforced through Bruin and SQL checks.

Recommended Python command set:

```bash
uv sync --locked
uv run ruff format --check .
uv run ruff check .
uv run mypy pipeline
uv run pytest
```

### SQL and data quality tooling

1. **SQL dialects**: BigQuery SQL for live mode; DuckDB SQL for fixture mode.
2. **SQL linting**: SQLFluff configured for BigQuery and DuckDB where practical.
3. **SQL correctness**: Bruin validation, BigQuery/DuckDB execution, and mart-level tests are the source of truth.
4. **Data quality**: Bruin built-in checks plus custom SQL checks for domain invariants.

Required custom checks should include:

1. No duplicate `(snapshot_week, ecosystem, package_name)` rows in current package marts.
2. No flagged package with `confidence = low`.
3. No flagged package with fewer than two independent fragility evidence signals.
4. No archived/deprecated package in `leaderboard.json`.
5. No unmappable package in `leaderboard.json`.
6. All exported packages have `methodology_version`, `snapshot_week`, and source-health metadata.
7. Score fields are bounded from 0 to 100.

### Frontend tooling

1. **Framework**: Astro static site.
2. **Language**: TypeScript with `strict` enabled.
3. **Package manager**: pnpm, with `pnpm-lock.yaml` checked in.
4. **Linting**: ESLint for TypeScript/Astro code.
5. **Formatting**: Prettier for frontend source, Markdown, and JSON where practical.
6. **Validation**: build-time JSON schema validation for pipeline-exported public artifacts.
7. **Smoke tests**: Playwright for one desktop and one mobile smoke pass once the UI exists.

Recommended frontend command set:

```bash
pnpm install --frozen-lockfile
pnpm lint
pnpm typecheck
pnpm build
pnpm test:smoke
```

### Bruin architecture

Bruin is the project orchestrator and competition proof point. It replaces separate orchestration, transformation wiring, materialization wrapping, quality-check glue, lineage, and AI-data-context setup for v1.

1. The final project root must be git-initialized because Bruin uses the Git root to locate the project.
2. The Bruin scaffold should be created with `bruin init empty` or manually kept equivalent to Bruin's project layout.
3. `.bruin.yml` is local configuration for environments, connections, and secrets; it must be gitignored. The repo ships `.bruin.yml.example` and setup instructions instead of committing real credentials.
4. `pipeline/pipeline.yml` defines the Bus Factor pipeline, schedule, default connections, and variables such as `source_mode`, `warehouse`, `snapshot_week`, and package limits.
5. Assets live under `pipeline/assets/`; each asset is one file that defines one data action and includes Bruin metadata plus SQL, Python, or seed/YAML content.
6. Python assets expose `materialize()` functions that return DataFrames for API/source extraction and custom logic; Bruin creates/inserts destination tables.
7. BigQuery SQL assets are the default live-mode transformation layer; DuckDB SQL assets provide the same shape for fixture-mode and local smoke checks.
8. SQL assets contain `SELECT` queries only; Bruin wraps them with materialization logic such as append, merge/delete+insert, or create+replace at runtime.
9. Seed/YAML assets load fixture files into DuckDB for the reviewer-friendly fixture path.
10. Bruin dependencies define the DAG and execution order. `bruin patch fill-asset-dependencies` may be used to infer dependencies from SQL, but dependencies must be reviewed and committed.
11. Bruin variables drive the same asset graph in fixture and live modes. SQL uses Jinja variables; Python reads built-in and custom variables from environment variables.
12. Bruin quality checks run after assets execute; blocking checks gate downstream assets, while selected diagnostic checks may be marked non-blocking.
13. Built-in checks cover column-level invariants such as `not_null`, `unique`, accepted values, and positive/non-negative numeric values.
14. Custom SQL checks enforce cross-column and business invariants such as “no flagged package with fewer than two fragility signals.”
15. Bruin render is used to show the final SQL Bruin will execute for materialized assets and to document materialization choices.
16. Bruin lineage is shown through the VS Code extension or exported/generated diagrams derived from committed asset dependencies.
17. Bruin AI context is created from table/column metadata and improved with `bruin ai enhance` where appropriate.
18. Bruin MCP and/or Bruin Cloud AI Data Analyst provide the actual analysis surface. The local-first path uses Bruin-generated context plus MCP so an AI assistant can query data with Bruin; Bruin Cloud is optional if a free or competition-provided path is available.
19. A final Bruin export asset writes the static public data bundle consumed by the Astro site.
20. A Bruin report/card asset generates the weekly report inputs and static share image.
21. `bruin validate` runs in CI before fixture execution and site publication.

Planned Bruin environments:

| Environment | Purpose | External access | Warehouse target |
| --- | --- | --- | --- |
| `fixture` | Fast reviewer and CI path using checked-in fixture data. | None. | Tiny local DuckDB file in the working tree or CI workspace. |
| `local_live_bq` | Developer live-source run that stores durable tables in BigQuery, not on the laptop. | Public APIs plus BigQuery; optional GitHub token. | BigQuery free-tier dataset. |
| `ci_fixture` | GitHub Actions validation and fixture artifact generation. | None. | Ephemeral CI DuckDB file. |
| `ci_live_bq` | Scheduled live run that writes to BigQuery and exports static site artifacts. | Public APIs plus BigQuery; optional GitHub token. | BigQuery free-tier dataset. |

Recommended Bruin command set:

```bash
cp .bruin.yml.example .bruin.yml
bruin validate pipeline/pipeline.yml
bruin patch fill-asset-dependencies pipeline
bruin render pipeline/assets/marts/mart_packages_current.sql
bruin run --full-refresh -e fixture --var source_mode=fixture pipeline/pipeline.yml
bruin run -e local_live_bq --var source_mode=live --var warehouse=bigquery pipeline/pipeline.yml
```

The exact run commands should be verified against the final Bruin scaffold and then copied into the README verbatim. The README should also include the Bruin MCP setup used to capture AI analysis screenshots, for example `bruin mcp` through the user's AI coding tool.

### Storage strategy

The project should be cloud-first for live data because the developer machine has limited storage. Local storage is reserved for source code, tiny fixtures, screenshots, and static assets.

Default storage choices:

| Data class | Default location | Local storage expectation | Notes |
| --- | --- | ---: | --- |
| Fixture inputs | `pipeline/fixtures/` in Git | Target <5 MB | Small enough for reviewer runs and CI. |
| Fixture warehouse | Local DuckDB file | Target <50 MB, disposable | Created only for fixture runs; safe to delete after checks. |
| Live raw extracts | BigQuery staging tables; optional compressed GCS objects | None by default | Avoid persistent local API caches. |
| Live staging/intermediate/marts | BigQuery dataset | None by default | Partition by `snapshot_week`, cluster by `ecosystem` and package/repo identifiers where useful. |
| Public site exports | `public-data/` and GitHub Pages artifact | Target <25 MB | Only curated JSON, screenshots, and cards are committed/published. |
| Weekly card/screenshots | `reports/cards/` and `analysis/screenshots/` | Target <25 MB | Keep optimized PNG/WebP assets. |

BigQuery is the default live warehouse because its free tier currently includes **10 GiB of storage per month** and **1 TiB of query data processed per month**. Cloud Storage can be used for compressed raw extracts only if needed; its free tier currently includes **5 GB-months of regional storage in eligible US regions**. Google Cloud free-tier products generally require an active Cloud Billing account; BigQuery sandbox is useful for trying BigQuery without a credit card, but the scheduled CI/live path should assume a normal GCP project with budgets and caps configured. The live MVP should remain far below these limits by avoiding GH Archive-scale raw event storage and by storing only weekly package/repository/source summaries.

Cost-control rules:

1. Do not store full GitHub Archive event history in v1.
2. Do not store full registry dumps.
3. Use batch loads, not streaming inserts.
4. Partition BigQuery tables by `snapshot_week`.
5. Cluster high-read BigQuery tables by `ecosystem`, `package_name`, and/or `repository_id`.
6. Set maximum bytes billed on exploratory BigQuery queries where practical.
7. Query only selected columns; avoid `SELECT *` in analysis examples.
8. Keep only enough historical snapshots for the competition narrative unless explicitly archiving more.
9. Add a CI/source-health check that estimates public export size and fails if it exceeds the agreed cap.
10. Keep local DuckDB files out of Git and document `rm data/*.duckdb` cleanup.
11. Configure a low GCP budget alert before enabling scheduled live runs.

If BigQuery setup becomes a distraction, the fallback is GitHub Actions ephemeral DuckDB for live runs: CI creates the DuckDB file on the runner, exports the static artifacts, and discards the database after publication. This avoids laptop storage at the cost of less cloud-queryable analysis.

### CI gates

GitHub Actions should block artifact publication unless these pass:

1. Python dependency lock install.
2. Ruff format check.
3. Ruff lint.
4. mypy.
5. pytest.
6. SQLFluff lint, if it does not fight valid DuckDB/Bruin syntax.
7. `bruin validate`.
8. Bruin fixture run.
9. Public export schema validation.
10. Frontend lint, typecheck, and static build.
11. Playwright smoke checks after the frontend exists.

### Version pinning policy

1. Pin Python with `.python-version`.
2. Pin Python dependencies with `uv.lock`.
3. Pin Node with `.nvmrc` or `.node-version`.
4. Pin frontend dependencies with `pnpm-lock.yaml`.
5. Pin or record the Bruin CLI version used for the final submission in the README and CI logs.
6. Avoid unpinned global tools in the competition-critical run path.

---

## Domain model / core concepts

### Ecosystem

A package registry domain supported in the project, limited in v1 to npm and PyPI.

### Package

A published package within an ecosystem.

### Candidate package

A package selected for initial tracking before eligibility, repository mapping, age, archival, and deprecation filters are applied.

### Repository

The source code project associated with a package.

### Repository mapping

The project’s association between a package and its source repository.

### Repository mapping confidence

A label derived from a concrete scoring algorithm run during ingestion. Each candidate mapping accumulates points; the total is bucketed into `high`, `medium`, or `low`.

Points (additive):

1. Registry metadata `repository.url` resolves to an existing public GitHub repo: **+40**.
2. Registry `homepage` resolves to the same repo: **+10**.
3. Repo has a manifest file matching the ecosystem (`package.json` for npm; `pyproject.toml` / `setup.py` / `setup.cfg` for PyPI) that declares a matching package name: **+30**.
4. deps.dev returns a `SOURCE_REPO` link to the same repo: **+20**.
5. OpenSSF Scorecard has data for the same repo: **+10**.
6. Repo owner/org name matches a publisher or namespace associated with the package: **+10**.

Caps and adjustments:

1. Multiple candidate repos with comparable scores: cap total at medium; record all candidates in `mart_package_evidence`.
2. Monorepo with ≥5 sibling manifest files: cap at medium and record the subdirectory path in evidence.
3. Archived or deprecated repos may still be high-confidence mappings; the archival state is handled by exclusion, not by downgrading the mapping.

Buckets:

| Total points | Bucket |
| ---: | --- |
| ≥70 | high |
| 40–69 | medium |
| <40 | low (unmappable for ranking purposes) |

Stub/types packages (npm `@types/*`, PyPI `*-stubs`, `types-*`) are detected by naming convention and routed to their own `excluded: stub_types` state rather than inheriting the source package's repo — they must not be ranked.

### Source extract

A raw or lightly normalized record pulled from an external public source during ingestion.

### Evidence signal

A normalized package-level fact used in scoring, display, exclusion, or confidence calculation.

### Weekly snapshot

A time-bounded weekly record of all package-level signals used for scoring.

### Reporting window

The one-week period used for refresh, score computation, change detection, and weekly reporting.

### Risk score

A numeric composite score indicating how concerning a package’s maintenance state appears relative to its importance.

### Importance score

A normalized ecosystem-relative score based primarily on dependency reach, with download volume as a supporting signal.

### Fragility score

A normalized score based on current maintenance, release, issue, security, and repository-health signals.

### Severity tier

A human-readable label derived from the score or relative rank.

### Confidence

A label indicating how complete and fresh the package evidence is.

### Finding

A short, human-readable reason a package is notable.

### Curated dataset

The final package-level dataset exposed to the showcase and to Bruin AI Data Analyst.

### Weekly report

The weekly narrative and tabular summary of notable packages.

### Shareable artifact

The generated weekly image card used for social and Slack distribution.

### Methodology version

A version label indicating the scoring logic used for a given snapshot.

### Fixture dataset

A small checked-in dataset that exercises the same pipeline contracts as live ingestion so reviewers can run the project quickly and reliably.

### Launch package

The Slack post, LinkedIn post draft, submission metadata, weekly card, screenshots, and final verification checklist used for competition submission.

Relationships:

* An Ecosystem contains Packages.
* A Candidate package may become Eligible, Ranked, or Excluded.
* A Package may have one Repository mapping.
* A Package belongs to one Weekly snapshot per Reporting window.
* A Weekly snapshot contains Source extracts and Evidence signals.
* A Weekly snapshot produces a Risk score, Severity tier, Confidence, and Findings for each eligible Package.
* The Curated dataset is the union of the latest scored packages and supporting evidence needed for analysis and display.
* The Weekly report summarizes Findings from one Reporting window.

---

## Rules, logic, and invariants

1. The pipeline’s canonical cadence is **weekly**, not daily.
2. A reporting window is one week, Monday 00:00 UTC to Monday 00:00 UTC.
3. The competition MVP tracked universe is limited to approximately the top 250–500 candidate packages per ecosystem by demand; 1,000–2,000 packages per ecosystem is a stretch target only after fixture and live runs are stable.
4. Candidate package selection happens before repository eligibility filters; a package without a confident repository mapping may be tracked but must not be ranked in the main leaderboard.
5. Packages without confident repository mappings must be disclosed in methodology/coverage output rather than silently dropped.
6. Archived or deprecated packages must not appear in the main leaderboard.
7. Archived or deprecated packages may appear in annotated coverage tables.
8. Packages younger than 12 months must not be eligible for flagged status.
9. Packages between 12 and 24 months may be scored with reduced confidence.
10. Low activity must not be treated as proof of abandonment.
11. Missing data must not be treated as zero-valued negative evidence.
12. Dependency reach is a stronger importance signal than raw download volume.
13. The scoring philosophy is: **importance** multiplied by **fragility**, normalized within ecosystem.
14. The default interpretation of the score is ecosystem-relative, not globally absolute.
15. False positives are costlier than false negatives and the scoring logic must err on the side of caution.
16. A package’s displayed explanation must correspond to the evidence in the current snapshot.
17. If `methodology_version` changes, comparisons across versions must be labeled as non-comparable.
18. If the current week’s report generation fails, the last successful weekly report remains live and is labeled stale.
19. Bruin AI Data Analyst may answer only from the curated dataset prepared by the pipeline.
20. The public site must not simulate or fake AI analysis outside of actual Bruin AI Data Analyst outputs.
21. The public demo must not require a live backend, paid database, paid warehouse, hosted API, login, or manually running the pipeline during the judging session.
22. The frontend consumes versioned, precomputed static artifacts exported by the pipeline.
23. The main leaderboard displays only packages that meet the v1 flagged definition.
24. Search and detail pages may expose non-flagged eligible packages for context, but they must distinguish them from flagged packages.
25. The weekly card summarizes the top flagged packages for the current reporting window and must not pad the list with packages that do not meet the flagged definition.
26. Cross-ecosystem ranking must be avoided unless the UI clearly groups by ecosystem and explains that scores are ecosystem-relative.
27. Multiple packages mapping to the same repository must share repository-level evidence but retain separate package-level demand and release signals.
28. Repository identity should use stable GitHub repository IDs when available, with owner/name treated as mutable display metadata.
29. Every displayed finding must point to at least one evidence signal in the curated dataset.
30. Every flagged package must have at least two independent fragility evidence signals and no critical source-quality failure.
31. Fixture runs and live-source runs must produce the same public artifact schema.

---

## Competition MVP scope

The project should prioritize a small, reliable, highly demonstrable pipeline over broad coverage. The competition-critical build order is:

1. **Positioning path** — write the Slack hook, LinkedIn angle, “How this differs from SCA tools” table, and Bruin feature proof plan before implementation.
2. **Fixture path** — checked-in fixture data, full Bruin run, checks, mart tables, static exports, and demo build.
3. **Live MVP path** — top 250–500 npm packages and top 250–500 PyPI packages, using rate-limit-aware ingestion and cached raw extracts.
4. **Launch path** — README, weekly card, AI Data Analyst screenshots, Slack post, LinkedIn post, hosted demo, and official submission metadata.
5. **Stretch path** — increase to 1,000–2,000 packages per ecosystem only if weekly run time, API limits, and artifact size remain comfortable.

If scope pressure appears, cut breadth first. Do not cut Bruin quality checks, fixture reproducibility, README clarity, competitive positioning, AI analysis screenshots, or launch materials.

### Competition-winning execution plan

1. **Week 1: Story and scaffold**
   Define the headline finding target, market-positioning table, Slack draft, LinkedIn draft, Bruin project scaffold, fixture schema, scoring config, and known-state validation set.

2. **Week 2: Fixture pipeline**
   Implement the full Bruin asset graph on fixture data: Python ingestion assets, DuckDB SQL transformations, quality checks, mart tables, export assets, and static demo reading fixture exports.

3. **Week 3: Live MVP ingestion**
   Add live npm, PyPI, deps.dev, GitHub, OSV, and OpenSSF Scorecard ingestion for 250–500 packages per ecosystem with source caching and source-health metadata.

4. **Week 4: Scoring, evidence, and demo polish**
   Tune scoring against known-state fixtures and live outputs, build package detail pages, methodology/coverage pages, source-health displays, and the weekly card.

5. **Week 5: Bruin proof and AI gallery**
   Capture Bruin lineage, custom check output, AI context improvements, and Bruin AI Data Analyst screenshots. Finish the README feature-proof table and comparison-to-SCA section.

6. **Final days: Launch hardening**
   Run fixture and live checks, manually review flagged packages for tone/false-positive risk, freeze screenshots, verify GitHub Pages, post in Slack, publish LinkedIn, and submit before the deadline.

---

## Curated data model

The public and AI-facing marts should be small, explicit, and easy to explain:

1. `mart_packages_current` — one row per package in the latest snapshot, including ecosystem, package name, version, age, repository mapping status, exclusion state, scores, tier, confidence, and display summary.
2. `mart_package_scores` — score components and normalized inputs for all eligible packages in the latest snapshot.
3. `mart_package_evidence` — one row per evidence signal used for scoring, confidence, explanation, or exclusion.
4. `mart_weekly_findings` — ranked flagged packages, notable elevated packages, and generated report text inputs for the current reporting window.
5. `mart_coverage_summary` — counts by ecosystem, candidate/tracked/eligible/ranked/excluded state, confidence, and data-source availability.
6. `mart_analysis_examples` — canonical Bruin AI Data Analyst prompts, screenshot paths, dataset version, methodology version, and capture date.
7. `mart_source_health` — ingestion status, source freshness, rate-limit/failure notes, and stale-source flags.
8. `mart_market_positioning` — static project-positioning rows used by README/site generation to explain what The Bus Factor overlaps with and how it differs from SCA, Socket-style supply-chain tooling, and OpenSSF Scorecard.

The static frontend exports should be generated from these marts and should not contain a second hand-coded interpretation of scoring logic.

---

## Scoring display decisions

The internal score is a 0–100 ecosystem-relative `risk_score`. The public UI uses conservative severity labels so the project communicates risk without implying that maintainers have abandoned a package.

### Score components

The v1 score uses a transparent weighted model rather than opaque ML:

1. `importance_score` is 0–100, normalized within ecosystem.
2. `fragility_score` is 0–100, normalized within ecosystem.
3. `risk_score = round(importance_score * fragility_score / 100)`, then capped or adjusted only by explicit confidence and exclusion rules.

All weights and thresholds below live in `pipeline/config/scoring.yml` and are read by the SQL and Python assets. Changing any weight requires bumping `methodology_version`. Initial v1 values are reviewed against the known-state validation fixture before being committed.

#### Importance inputs (v1 weights)

Each input is normalized to 0–100 within ecosystem (percentile rank on log-scaled raw values where noted), then combined with weights that sum to 1.0.

| Input | Weight | Source | Notes |
| --- | ---: | --- | --- |
| Dependency reach (downstream dependents, log-scaled percentile) | 0.60 | deps.dev | Primary importance signal. |
| Download volume (90-day, log-scaled percentile) | 0.25 | npm / PyPI | Dampened by log + percentile because registry downloads are noisy. |
| Security exposure (transitive vulnerability count, log-scaled percentile) | 0.15 | OSV + deps.dev | Elevates importance when users may be exposed. |

#### Fragility inputs (v1 weights)

Each input is converted to a 0–100 fragility contribution using the thresholds below, then combined with weights that sum to 1.0.

| Input | Weight | Fragility contribution | Notes |
| --- | ---: | --- | --- |
| Release recency | 0.25 | 0 if ≤90 days since last release; linear ramp to 100 at 540 days; clamp at 100 beyond. | Paired-signal only — never flags alone. |
| Commit activity recency (default branch) | 0.25 | 0 if ≤30 days since last commit; linear ramp to 100 at 365 days; clamp at 100 beyond. | Low activity alone is not proof of neglect. |
| Release cadence decay | 0.15 | 100 × max(0, 1 − (releases in last 365d / releases in prior 365d)); 0 if prior-year denominator <2. | Captures slowdown trends, not absolute volume. |
| Issue/PR responsiveness | 0.15 | Based on median time-to-first-maintainer-response on issues opened in last 180 days; 0 at ≤7 days, 100 at ≥90 days (linear). | Excluded if fewer than 10 eligible issues. |
| Contributor bus-factor proxy | 0.10 | Top-1 contributor share of commits in last 365d: 0 at ≤0.30, 100 at ≥0.90 (linear in between). | Repository-level only; maintainer names never exposed in viral surfaces. |
| OpenSSF Scorecard health | 0.10 | 100 − (scorecard_aggregate × 10), clamped 0–100. | Supporting signal; never dominates. |

`Archival/deprecation state`, `package age <12 months`, and `stub/types` are exclusion flags, not fragility inputs — they short-circuit to the corresponding excluded state.

#### Minimum evidence for flagging

A package is flagged only if at least two independent fragility inputs each contribute ≥40 to the fragility score. This is enforced as a custom SQL check: no row in `mart_packages_current` with `flagged = true` may have fewer than two inputs ≥40.

#### Ecosystem normalization

All percentile ranks are computed within ecosystem (npm or PyPI) on the eligible, non-excluded tracked set for the current snapshot. A package's rank does not depend on packages in the other ecosystem.

### Confidence labels

| Confidence | Definition |
| --- | --- |
| High | High mapping confidence, not in reduced-confidence age band, and no unhealthy critical/non-critical sources. |
| Medium | High/medium mapping confidence, no critical source-health failures, and at most one non-critical unhealthy source. |
| Low | Mapping confidence, source health, or package age is insufficient for a reliable stability verdict. |

Low-confidence packages may be displayed in detail/search views but must not be marked `flagged = true`.

### Severity tiers

| Tier | Score range | Public meaning |
| --- | ---: | --- |
| Unrated | n/a (confidence-driven) | Evidence quality is too low to make a stable-vs-risk claim; assigned whenever confidence is low. |
| Stable | 0–14 | No concerning combination of importance and fragility in the current snapshot. |
| Watch | 15–24 | Some weak signals worth monitoring, but not enough evidence to flag. |
| Elevated | 25–29 | Meaningful fragility signals on an important package; visible in detail/search but not treated as headline flagged risk. |
| High | 30–49 | Flagged. Important package with multiple current fragility signals and sufficient evidence quality. |
| Critical | 50–100 | Flagged. Highest ecosystem-relative risk with strong evidence across importance and fragility signals. |

### Flagged definition

A package is `flagged = true` in v1 only when all of the following are true:

1. The package is eligible for ranking.
2. `risk_score >= 30`.
3. `severity_tier` is `High` or `Critical`.
4. `confidence` is `medium` or `high`.
5. At least two independent fragility signals contribute to the explanation.
6. The package is in the top 25% of the tracked ecosystem by importance score.

This definition intentionally leaves some concerning packages in `Elevated` rather than headline surfaces. The project can still expose them on package detail pages and in Bruin AI Data Analyst examples, but the weekly report and share card should reserve the word “flagged” for high-confidence cases.

---

## Canonical Bruin AI Data Analyst questions

The analysis gallery should use the following v1 prompts. Each screenshot must show the exact prompt, the answer, and enough table context to prove the answer came from the curated dataset.

1. “Which currently flagged npm packages have the highest risk score, and what evidence explains each score?”
2. “Which currently flagged PyPI packages have the highest risk score, and what evidence explains each score?”
3. “Group the current flagged packages by ecosystem and severity tier.”
4. “Which high-importance packages have elevated fragility signals but are not flagged, and why are they below the threshold?”
5. “Which packages were excluded because they are archived or deprecated, and why are they not ranked?”
6. “Which tracked packages are excluded because their repository mapping confidence is too low?”
7. “Compare the top flagged npm and PyPI packages by importance score, fragility score, confidence, and primary findings.”
8. “Which packages changed severity tier since the previous methodology-compatible weekly snapshot?”

The gallery should avoid open-ended prompts that ask the analyst to infer maintainer intent. Acceptable prompts are limited to ranking, filtering, grouping, comparison, and explaining displayed evidence.

If the conservative flagged threshold produces too few examples in a given week, use these fallback prompts rather than weakening the methodology:

1. “Which high-importance packages are currently in the Elevated tier, and what evidence kept them below flagged status?”
2. “Which excluded packages would have been important if they had a confident repository mapping?”
3. “Which source coverage gaps most affect confidence in this week’s snapshot?”

---

## Weekly card decision

The weekly shareable card shows up to five flagged packages, ordered by `risk_score` descending with ecosystem-aware tie-breaking by importance score. It should not always force a fixed package count:

1. If five or more packages are flagged, show the top five.
2. If one to four packages are flagged, show only those packages and use the remaining space for reporting-window, freshness, and methodology notes.
3. If zero packages are flagged, show a “No high-confidence flagged packages this week” card and link to the full report for watchlist context.

This keeps the card honest while preserving a stable, shareable visual.

---

## States and transitions

### Pipeline states

**Not run** → no current weekly snapshot exists yet.

**Running** → ingestion, transforms, checks, exports, or artifact generation are in progress.

**Succeeded** → the weekly snapshot, curated dataset, and public artifacts were produced successfully.

**Partially succeeded** → the weekly snapshot succeeded but one or more downstream public artifacts failed.

**Failed** → no new current snapshot was produced.

Transitions:

* Not run → Running on manual or scheduled execution.
* Running → Succeeded when all critical stages complete.
* Running → Partially succeeded if snapshot creation succeeds but weekly card or site export fails.
* Running → Failed if a critical pipeline stage fails.

### Package ranking states

**Tracked** → package is in the tracked universe.

**Eligible** → package is old enough and mappable enough to be scored.

**Excluded: unmappable** → tracked but no confident repo mapping.

**Excluded: archived/deprecated** → intentionally ended and not ranked.

**Excluded: too new** → not old enough for flagged status.

**Ranked** → package has an eligible score and may appear in package detail/search or analysis outputs.

**Flagged** → ranked package meets the v1 flagged definition and appears on the main leaderboard, weekly report, and shareable artifact.

### Weekly report states

**Current** → generated for the latest reporting window.

**Stale fallback** → prior week’s report is still served because the latest generation failed. 

---

## UX principles and visual direction

1. The project should feel like a **data engineering showcase**, not a startup SaaS shell.
2. The most important thing should always be: what did the pipeline produce, and why should I trust it?
3. The site should be compact, dense, and evidence-forward.
4. The weekly artifact should be the visual hero.
5. The analysis page should feel like proof that Bruin AI Data Analyst works over the dataset, not like a separate product surface.
6. The methodology page should feel unusually transparent.
7. The overall tone should be credible, sharp, and slightly provocative without becoming alarmist or disrespectful toward maintainers.

### Tone and naming guardrails

The Bus Factor name is intentionally memorable, but the product copy must be precise:

1. Never claim a package is abandoned, dead, negligent, or unsafe.
2. Use “flagged,” “watch,” “elevated,” “fragility signals,” and “evidence” rather than maintainer-blame language.
3. Always frame results as public-data signals, not verdicts.
4. Do not expose named maintainers in viral surfaces, weekly cards, or leaderboard summaries.
5. Give maintainers the benefit of the doubt when explaining stable but low-activity projects.
6. Explain that “bus factor” is used as a continuity-risk shorthand, not as a literal claim about individual maintainers.

---

## Information architecture

### 1. Home / Leaderboard

Latest flagged packages, ecosystem filter, search, short summary, freshness.

### 2. Package Detail

Package score, tier, confidence, explanation, evidence table, score inputs, methodology notes.

### 3. Weekly Report

Current week’s notable flagged packages, short narrative, generated image card, linkable permalink.

### 4. Analysis

Gallery of canonical Bruin AI Data Analyst screenshots, prompts used, and explanation of how analysis is delivered.

### 5. Methodology / Coverage

Scoring philosophy, tracked universe definition, reporting window definition, methodology versioning, exclusions, annotated archived/deprecated table, annotated unmappable table, source limitations.

### 6. Repo / Run Instructions

May live in README only rather than the site; if present on-site, it links users back to the GitHub repo.

---

## Frontend and demo implementation

The v1 frontend should be a static showcase, not an application server. The recommended implementation is an Astro static site with small client-side islands only where needed for search and filtering. A plain Vite static app is acceptable if it preserves the same static-output constraint.

### Free demo hosting

1. The default public demo host is GitHub Pages.
2. GitHub Actions builds the pipeline artifacts and the static site on a weekly schedule and on manual dispatch.
3. The site is published from static build output only; no serverless functions, hosted database, paid warehouse, or login is required.
4. Vercel, Netlify, or Cloudflare Pages may be used as secondary mirrors, but the project must not depend on them for judging.
5. The README must include both the GitHub Pages demo URL and local preview instructions.

### Static data contract

The frontend consumes precomputed artifacts exported by the Bruin pipeline. The minimum public data bundle is:

1. `metadata.json` — reporting window, generated timestamp, methodology version, freshness state, and source summary.
2. `leaderboard.json` — flagged packages shown on the home page.
3. `packages.json` or package-level JSON shards — package detail data, evidence, score inputs, findings, and exclusion state.
4. `weekly_report.json` — current report narrative, notable packages, and permalink metadata.
5. `coverage.json` — tracked, ranked, unmappable, archived/deprecated, too-new, and confidence counts.
6. `analysis_gallery.json` — Bruin AI Data Analyst prompts, screenshot paths, dataset version, and capture date.
7. `source_health.json` — source freshness, source failures, stale fallback state, and live-vs-fixture run metadata.
8. `market_positioning.json` — concise comparison rows for SCA platforms, Socket-style supply-chain tooling, OpenSSF Scorecard, and The Bus Factor.

The repo should include the latest successful real exported data bundle so the public demo remains displayable even if the next scheduled run fails. If no real bundle exists yet during early development, the site may show an explicit empty state with setup instructions, but synthetic rankings must not be presented as real results.

### Demo display requirements

1. The first viewport must show the project name, reporting window, freshness state, and at least part of the current leaderboard or empty-state proof.
2. The weekly card must be visible on the weekly report page and stored as a static image file for sharing.
3. The analysis page must render local screenshot assets, not remote private links.
4. Package detail pages must be linkable by stable slugs so reviewers can deep-link from the report and README.
5. The site must remain usable when opened from the hosted demo without any API keys or local services.

### Deferred frontend scope

DuckDB-Wasm is deferred for v1. The showcase should stay purely precomputed because client-side SQL exploration adds build, bundle, and UX risk without improving the core competition story. It can be reconsidered after the static demo, README, weekly report, and Bruin AI Data Analyst gallery are complete.

---

## Recommended repo structure

The final repository should make the competition story obvious from the root:

```text
.
├── .bruin.yml.example
├── .github/
│   └── workflows/
├── .gitignore
├── .nvmrc
├── .python-version
├── .sqlfluff
├── README.md
├── eslint.config.js
├── package.json
├── playwright.config.ts
├── pnpm-lock.yaml
├── pnpm-workspace.yaml
├── prettier.config.mjs
├── pyproject.toml
├── tsconfig.json
├── pipeline/
│   ├── pipeline.yml
│   ├── assets/
│   │   ├── raw/
│   │   ├── seeds/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── config/
│   │   └── scoring.yml
│   ├── fixtures/
│   └── scripts/
├── public-data/
├── uv.lock
├── web/
│   ├── astro.config.mjs
│   └── src/
├── reports/
│   ├── weekly/
│   └── cards/
├── analysis/
│   ├── prompts.md
│   └── screenshots/
├── docs/
│   ├── methodology.md
│   └── sources.md
├── launch/
│   ├── bruin-comparison.md
│   ├── slack-post.md
│   ├── linkedin-post.md
│   ├── maintainer-response.md
│   ├── market-positioning.md
│   └── submission-checklist.md
└── tests/
```

The README should point reviewers to three paths immediately: hosted demo, fixture run, and Bruin feature proof.

---

## Competition launch checklist

1. Public GitHub repo exists and README renders correctly.
2. `.bruin.yml.example`, `.gitignore`, `pipeline.yml`, assets, checks, and fixture data are committed; real `.bruin.yml` is not committed.
3. CI creates local `.bruin.yml` from `.bruin.yml.example` with non-secret fixture settings.
4. `bruin validate pipeline/pipeline.yml` succeeds.
5. Fixture pipeline run succeeds and produces the expected mart/export schema.
6. Live MVP run succeeds or the latest successful live export is checked in with source-health metadata.
7. GitHub Pages demo loads without credentials or runtime services.
8. Weekly card renders and has a stable permalink.
9. Bruin AI Data Analyst screenshots are captured with prompt, answer, dataset version, methodology version, and capture date.
10. README includes the “How this differs from SCA tools” table.
11. Slack post draft includes the demo URL, GitHub URL, weekly card, and concise Bruin feature summary.
12. LinkedIn post draft explains Bruin features used, design choices, how Bruin compares to other tools, how The Bus Factor differs from SCA tools, and includes AI analysis screenshots.
13. Official submission metadata is ready, including GitHub URL, Slack thread URL, and optional LinkedIn post URL.
14. Final submission happens before **June 1, 2026 at 12:00 UTC**.

---

## Constraints

1. Build as a public web showcase plus a public GitHub repository.
2. The web showcase must be static.
3. The fixture pipeline must be runnable locally with Bruin CLI and DuckDB.
4. The live MVP should use BigQuery free-tier storage/querying by default to avoid storing live data locally.
5. Weekly scheduled execution should be possible on free-tier GitHub Actions.
6. The scope must remain small enough for a six-week competition build.
7. No custom NL backend.
8. No login or multi-user state.
9. No real-time updates.
10. BigQuery is not required for reviewer fixture reproduction, but is the default live-source warehouse.
11. The hosted demo must work from static files on GitHub Pages.
12. The fixture pipeline must run without API keys.
13. The live-source pipeline may use optional GitHub credentials for higher rate limits, but the README must document unauthenticated limits and fallback behavior.
14. The MVP live-source run should target 250–500 packages per ecosystem before expanding coverage.
15. Real Bruin connection/secrets configuration belongs in local `.bruin.yml` and must not be committed; committed examples must use placeholders only.
16. Local generated data files must be disposable, gitignored, and kept small enough for the developer machine.

---

## Data and persistence expectations

### Must persist

1. Raw staged source extracts or reproducible equivalents in BigQuery and/or optional compressed GCS objects
2. Weekly snapshots in BigQuery for live runs and tiny DuckDB for fixture runs
3. Curated scored dataset in BigQuery plus static public exports
4. Methodology version per snapshot
5. Weekly report outputs
6. Generated shareable artifact
7. Exported data used by the site
8. Canonical Bruin AI Data Analyst screenshots used in the showcase and README
9. Fixture input data and fixture expected-output snapshots
10. Source health metadata for the latest run
11. Slack post draft, LinkedIn post draft, and competition submission metadata placeholders

### May be ephemeral

1. Intermediate pipeline execution artifacts
2. Temporary build outputs
3. Local UI preferences like last-selected ecosystem
4. Local DuckDB fixture files
5. CI runner DuckDB files for fallback live runs

### Persistence principle

Historical storage only needs to be deep enough to support the competition narrative and current weekly reporting. Long-term trend infrastructure is not required in v1.

---

## Source strategy and rate-limit policy

1. Live ingestion should cache raw source extracts by reporting window in BigQuery staging tables or optional compressed GCS objects so reruns do not repeatedly hit public APIs.
2. GitHub ingestion should work unauthenticated for small fixture/dev cases and support an optional token for the live MVP.
3. Source failures should reduce confidence or mark source-health warnings; they must not be converted into negative package evidence.
4. Every public methodology page should name the source category behind each evidence signal.
5. If a source is stale or partially failed, the site should show that in freshness/source-health metadata.
6. GH Archive is a stretch source, not a dependency for MVP scoring.
7. The fixture dataset should include representative examples for mappable, unmappable, archived/deprecated, too-new, elevated, flagged, and stale-source cases.
8. Snyk, Sonatype, Endor Labs, Socket, and similar commercial platforms must not be used as source data for v1.
9. OpenSSF Scorecard may be used as a public complementary source signal, but the project must not imply Scorecard alone produces The Bus Factor ranking.

---

## External dependencies and integrations

### Core pipeline

* Bruin CLI
* Python 3.12.x
* uv
* BigQuery free-tier dataset for live storage and transformations
* DuckDB
* Optional Cloud Storage bucket for compressed raw extracts if BigQuery staging tables are insufficient
* GitHub REST/GraphQL data, with optional token for higher rate limits
* deps.dev
* npm metadata sources
* PyPI metadata sources
* OSV
* OpenSSF Scorecard
* Optional GH Archive access for broader GitHub activity context after MVP stability

### Analysis

* Bruin AI Data Analyst workflow
* Bruin AI asset/context enhancement where useful for table and column descriptions
* Bruin MCP for local AI-agent access to Bruin context and data querying
* Optional Bruin Cloud AI Data Analyst for exposing the curated dataset to the AI analyst, only if available on a free or competition-provided path

### Orchestration and publishing

* GitHub Actions for weekly runs
* `bruin validate` in CI
* GitHub Pages for the primary showcase demo
* Optional artifact hosting via GitHub Releases or equivalent

### Frontend and quality tooling

* Astro static site
* TypeScript 5.x
* Node.js 24 LTS
* pnpm
* Ruff
* mypy
* pytest
* SQLFluff
* ESLint
* Prettier
* Playwright smoke tests

### Optional differentiators

* DuckDB-Wasm for client-side dataset exploration after v1, not for the competition-critical path
* Bruin Cloud AI Dashboard screenshots if they can be produced on a free or competition-provided path without distracting from the static demo

This architecture should be expressed conceptually in the spec and visually in the README. 

---

## Edge cases and failure modes

1. A package cannot be confidently mapped to a repository.
2. Multiple packages map to the same repository.
3. A repository changes ownership or name.
4. A package is extremely popular but intentionally stable, leading to low activity without neglect.
5. Download counts are noisy or inflated.
6. A package is too new to interpret fairly.
7. Archived projects are still widely depended on.
8. A source API rate-limits or partially fails.
9. The weekly site export succeeds but the shareable card fails.
10. The weekly snapshot succeeds but the hosted site is stale.
11. A methodology change makes cross-week comparisons misleading.
12. A Bruin AI Data Analyst screenshot becomes outdated relative to the current methodology or dataset version.
13. A reviewer follows the README and encounters local-environment issues.
14. The tracked universe grows beyond free-tier practicality.
15. A package is excluded and the user incorrectly interprets exclusion as absence from source data.
16. A conservative week produces zero flagged packages.
17. A live-source run is rate-limited after fixture mode succeeds.
18. A repository is renamed, transferred, deleted, or made private after a prior mapping.
19. A package has multiple active distributions that share one repository but differ in release cadence or demand.
20. The hosted demo is current but the LinkedIn or Slack screenshots show an older methodology version.
21. A source API returns partial data that would change confidence but should not be treated as negative evidence.

---

## Acceptance criteria

1. A reviewer can clone the repo, follow the README, and reproduce the fixture pipeline locally with Bruin CLI and DuckDB without external cloud dependencies.
2. A reviewer can see clearly in the repo how Bruin is used for ingestion, transformation, orchestration, and analysis.
3. A reviewer can run or inspect a weekly snapshot artifact produced by the pipeline.
4. A reviewer can open the public site and see a leaderboard, package detail page, weekly report, and methodology page.
5. A reviewer can understand the score philosophy, reporting window, exclusions, and coverage limits.
6. A reviewer can see a weekly static image artifact suitable for sharing.
7. A reviewer can see example Bruin AI Data Analyst screenshots and prompts on the site or in the README.
8. A reviewer can confirm the project did not build a custom NL interface.
9. A reviewer can identify freshness and methodology-version context for the displayed data.
10. A reviewer can inspect how unmappable and archived/deprecated packages are handled.
11. A reviewer can understand the project within roughly ten minutes of opening the repo.
12. A reviewer can open the hosted demo from GitHub Pages without credentials, API keys, running services, or paid infrastructure.
13. A reviewer can verify that the hosted demo is backed by static pipeline exports rather than a live application backend.
14. A reviewer can run the fixture path without external API keys.
15. A reviewer can identify the exact Bruin assets, dependencies, materializations, and quality checks used.
16. A community voter can understand the project from the Slack post and weekly card without reading the full README.
17. The repo contains a prepared Slack post, LinkedIn post draft, and official submission metadata placeholders.
18. The LinkedIn draft explains Bruin features used, design choices, comparison to other tools, and includes Bruin AI Data Analyst screenshots.
19. The project can be submitted before **June 1, 2026 at 12:00 UTC** with public GitHub, public demo, Slack thread URL, and optional LinkedIn URL.
20. The README and methodology page explain how The Bus Factor differs from Snyk/Sonatype/Endor-style SCA, Socket-style supply-chain tooling, and OpenSSF Scorecard.
21. The project uses OpenSSF Scorecard only as a complementary source signal and does not rely on commercial SCA platforms as data sources.
22. The live MVP stores durable live-source data in BigQuery free-tier resources by default rather than on the developer machine.
23. Local generated data files are documented as disposable and remain below the agreed fixture-size cap.

---

## Assumptions and v1 decisions

### Assumptions

1. v1 supports only npm and PyPI.
2. Weekly cadence is sufficient for the competition.
3. A top 250–500 package universe per ecosystem is enough for the competition MVP; 1,000–2,000 packages per ecosystem is a stretch target.
4. Reviewers care more about pipeline credibility and clarity than surface-area breadth.
5. Bruin AI Data Analyst screenshots are part of the core judged output.
6. The site is a showcase layer, not the primary product.
7. Slack community voters will reward a fast, understandable story more than maximum methodological breadth.
8. A fixture run is necessary because live package and repository APIs can be slow, rate-limited, or temporarily inconsistent.

### Final v1 decisions

1. Severity tiers are `Unrated`, `Stable`, `Watch`, `Elevated`, `High`, and `Critical` (`Unrated` is confidence-driven for low-confidence rows).
2. `flagged = true` requires eligibility, `risk_score >= 30`, `High` or `Critical` tier, medium/high confidence, at least two independent fragility signals, and top-quartile ecosystem importance.
3. The canonical Bruin AI Data Analyst gallery uses the eight prompts listed in “Canonical Bruin AI Data Analyst questions.”
4. The weekly card shows up to five flagged packages and does not pad with non-flagged packages.
5. DuckDB-Wasm is deferred for v1; the showcase remains purely precomputed and static.
6. The default frontend path is an Astro static site deployed to GitHub Pages from pipeline-exported JSON and local screenshot/image assets.
7. The demo must be free to host and view, and must not depend on paid infrastructure or a live backend.
8. The default leaderboard presentation is grouped by ecosystem because scores are ecosystem-relative.
9. The project includes both fixture and live-source run paths.
10. Bruin feature proof is a README requirement, not optional implementation detail.
11. The launch package is part of the v1 deliverable.
12. Python 3.12.x, BigQuery SQL, DuckDB SQL, Bruin, Astro, TypeScript, and GitHub Pages are the default implementation stack.
13. Ruff, mypy, pytest, SQLFluff, ESLint, Prettier, Bruin validation, and frontend build checks are CI gates.
14. The project is positioned as a public continuity-risk data product, not as SCA, malicious-package detection, or repository posture scanning.
15. Commercial SCA products are peer context for positioning, not data sources or implementation dependencies.
16. BigQuery free-tier storage/querying is the default live warehouse; DuckDB is the tiny fixture/reviewer warehouse.

---

## Glossary

**The Bus Factor** — the project name and shorthand for package continuity-risk signals, not a literal claim about individual maintainers.

**Bruin pipeline** — the competition’s core deliverable: ingestion, transformation, orchestration, checks, and analysis.

**Curated dataset** — the final package-level output generated by the pipeline and exposed to the showcase and Bruin AI Data Analyst.

**Reporting window** — one weekly period used for the snapshot and report.

**Methodology version** — version label for the scoring logic.

**Weekly snapshot** — the produced weekly package-state dataset.

**Shareable artifact** — static image card generated from the weekly report.

**Unmappable package** — tracked package lacking a confident repo mapping and therefore excluded from the main ranking.

**Archived/deprecated package** — intentionally ended package disclosed in coverage but not ranked with live packages.

**Bruin AI Data Analyst gallery** — the curated set of screenshot-based example analyses shown to reviewers instead of a custom NL interface.
