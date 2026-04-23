# Bruin vs a stitched-together data stack

> Used in the LinkedIn post and README. Keep this table concrete — name real tools the stitched stack would use.

| Concern | Bruin (one tool) | Stitched-together stack | Why Bruin wins here for this project |
| --- | --- | --- | --- |
| Ingestion | Python asset with `materialize()`, metadata co-located | Custom Python + Airflow/Prefect operator + separate connection config | One file per source, less glue. |
| Transformation | SQL asset (BigQuery or DuckDB), materialization chosen in frontmatter | dbt + separate warehouse adapter + separate run command | Same asset graph works in fixture and live. |
| Orchestration | `bruin run` honors declared DAG + variables | Airflow / Dagster DAG defined separately from transformation code | Dependencies live next to the asset. |
| Quality checks | Built-in + custom SQL checks declared inline; gate downstream | dbt tests or Great Expectations wired in separately | Failing checks block the DAG; reviewers see one place. |
| Lineage | `bruin lineage` / VS Code extension reads asset deps | OpenLineage + Marquez / Datahub setup | Lineage comes from the same declarations that run the DAG. |
| AI context | Bruin AI context + `bruin ai enhance` produces metadata | Manual schema docs + custom RAG / embedding pipeline | Bruin AI Data Analyst queries the curated dataset with grounded metadata. |
| Fixture vs live parity | One `source_mode` variable | Duplicate asset definitions or env-specific forks | No code duplication. |
| Reviewer path | Clone → `bruin run -e fixture` → done | Install N tools, configure N connections | Judges can reproduce in 10 minutes. |

## One-paragraph summary

Bruin bundles what a serious data project would otherwise assemble from dbt + Airflow + a testing framework + a lineage service + a docs-and-AI layer. For a six-week competition build, the win is that one reviewer command reproduces ingestion, transformations, checks, lineage, and AI-ready metadata from a single asset graph.
