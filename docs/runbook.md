# Runbook — live ingestion debugging

Working journal for the first end-to-end `local_live_bq` run. Updated as
issues are found and resolved; promote stable sections into
`docs/sources.md` or `README.md` once the pipeline is green.

---

## Architecture shift (applied)

Before this run, `raw.deps_dev` issued BigQuery jobs directly for every
weekly run. That produced per-run BQ scans of ~450 GB for npm universe
selection + another large scan for the per-package join, which is
unsustainable on a free tier. We moved to a two-tier architecture:

- **Hot path (weekly, zero BQ cost).**
  `raw.deps_dev` now reads the committed universe seed
  (`pipeline/data/universe/{npm,pypi}.json`) for `package_name` and
  `dependent_count`, and joins `source_repo_url` from the already
  materialised `raw.npm_registry` / `raw.pypi_registry.repository_url`
  via DuckDB. No BigQuery jobs are issued.
- **Cold path (manual, monthly).**
  `scripts/refresh_universe.py` rewrites the seed. The npm branch hits
  deps.dev BigQuery (`bigquery-public-data.deps_dev_v1.Dependents`,
  partition-pinned); the PyPI branch pulls hugovk/top-pypi-packages
  JSON.

Schema change:
`pipeline/data/universe/{ecosystem}.json.packages` is now a list of
`{name, dependent_count}` objects (legacy string-list form still loads
via `_coerce_package` for back-compat).

See [`pipeline/lib/universe.py`](../pipeline/lib/universe.py) module
docstring for the full decision trace.

---

## Commands executed

All run from the repo root. `cd /Users/joseph/programming/github/the_bus_factor`.

### Quality gates (green)

```bash
.venv/bin/python -m pytest -q                         # 101 passed
.venv/bin/python -m ruff check pipeline scripts tests # All checks passed
.venv/bin/python -m ruff format --check pipeline scripts tests
.venv/bin/python -m mypy pipeline                     # Success, 24 files
```

### Pipeline validation

```bash
bruin validate pipeline/pipeline.yml > .cache/bruin_validate.log 2>&1
# exit 0, 51 "used-tables" warnings (Bruin-internal SQL parser quirk,
# pre-existing, unrelated to this refactor).
```

### Fixture regression check

```bash
rm -f data/ci_fixture.duckdb
bruin run --workers=1 --full-refresh -e ci_fixture pipeline/pipeline.yml \
    > .cache/bruin_fixture.log 2>&1
# 51 assets executed, 304 quality checks, exit 0 in 1m46s.
```

First attempt failed with `Operation not permitted (os error 1)` on
`/Users/joseph/.local/share/uv/tools/.tmp...`. That path is outside the
sandbox write allowlist; rerunning with `required_permissions: ["all"]`
fixed it. This means the sandbox needs `all` for any command that
invokes `uv tool run ingestr` (every `seed.*` asset does).

### Live run (500/500 attempt stalled, later fixed)

Pre-flight check:

```bash
echo "token_present=$(test -n "$GITHUB_INGEST_TOKEN" && echo yes || echo no)"
ls ~/.config/gcloud/application_default_credentials.json    # present
```

Universe seeds confirmed populated:

```text
npm count: 500  source: wooorm/npm-high-impact (bootstrap)
pypi count: 500 source: hugovk/top-pypi-packages
```

Note: the npm seed is still the bootstrap (no `dependent_count`). A
billed BigQuery refresh via `scripts/refresh_universe.py --ecosystem npm
--limit 500` is needed before scoring importance from real dependent
counts.

Kicked off (workers=1, to avoid the DuckDB lock conflict below):

```bash
rm -f data/local_live_bq.duckdb
: > .cache/bruin_live.log
nohup bruin run --workers=1 -e local_live_bq \
    --var 'source_mode="live"' \
    --var 'warehouse="bigquery"' \
    --var 'npm_package_limit=500' \
    --var 'pypi_package_limit=500' \
    pipeline/pipeline.yml \
    > .cache/bruin_live.log 2>&1 &
```

Bruin `--var` values must be valid JSON, hence the shell-quoted double
quotes around string values. Plain `--var source_mode=live` errors with
`invalid character 'l' looking for beginning of value`.

Cycle-2 follow-up: the stalled Bruin parent (`pid 74152`) and Python
asset worker (`pid 79362`) were terminated before applying fixes.

### Small live smoke (5 npm + 5 PyPI, green)

After fixing the GitHub contributor timeout and live-data tolerance
issues below, the small real-data smoke completed end to end:

```bash
rm -f data/local_live_bq.duckdb data/local_live_bq.duckdb.wal
BUS_FACTOR_LOG_LEVEL=INFO BQ_MAX_BYTES_BILLED=10000000000 \
bruin run --workers=1 --full-refresh -e local_live_bq \
    --var 'source_mode="live"' \
    --var 'warehouse="bigquery"' \
    --var npm_package_limit=5 \
    --var pypi_package_limit=5 \
    pipeline/pipeline.yml | tee .cache/bruin_live_smoke.log
```

Result:

```text
51 assets executed, 303 quality checks, exit 0 in 2m17s
export_public_bundle wrote 8 artefacts to public-data/
generate_weekly_card wrote reports/cards/weekly-2026-04-20.png and latest.png
```

Observed row counts from `data/local_live_bq.duckdb`:

| Table | Rows |
| --- | ---: |
| `raw.npm_registry` | 5 |
| `raw.pypi_registry` | 5 |
| `raw.deps_dev` | 10 |
| `raw.github_repos` | 10 |
| `raw.github_commits` | 10 |
| `raw.github_contributors` | 10 |
| `raw.github_issues` | 10 |
| `raw.github_releases` | 10 |
| `raw.scorecard` | 10 |
| `mart.packages_current` | 10 |
| `mart.package_scores` | 5 |
| `mart.coverage_summary` | 2 |
| `mart.weekly_findings` | 0 |
| `mart.source_health` | 10 |

This smoke uses real public APIs and live source mode, but the current
registered Bruin assets still execute the DuckDB SQL tree. There is no
GCS bucket configured in `.bruin.yml` or the workflows; Cycle 2 publishes
local `public-data/` plus the share card, then validates BigQuery with
the separate smoke below.

### BigQuery SQL smoke (5 npm + 5 PyPI, green)

The BigQuery smoke uploads the live run's `seed` and `raw` DuckDB tables
to prefixed BigQuery datasets, renders every sibling under
`pipeline/assets_bq/`, executes them in dependency order, and runs mart
custom checks directly in BigQuery:

```bash
BUS_FACTOR_LOG_LEVEL=INFO BQ_MAX_BYTES_BILLED=10000000000 \
uv run python scripts/run_bigquery_smoke.py \
    --duckdb-path data/local_live_bq.duckdb \
    --project-id bus-factor-494119 \
    --location US \
    --dataset-prefix bf_smoke
```

Result:

```text
BigQuery smoke complete
project_id=bus-factor-494119
dataset_prefix=bf_smoke
bf_smoke_raw.npm_registry=5
bf_smoke_raw.pypi_registry=5
bf_smoke_raw.source_health=10
bf_smoke_mart.source_health=10
bf_smoke_mart.packages_current=10
bf_smoke_mart.package_scores=5
bf_smoke_mart.coverage_summary=2
```

All mart custom checks returned `failures=0`. The smoke writes
`bf_smoke_raw`, `bf_smoke_stg`, `bf_smoke_int`, and `bf_smoke_mart`.
Use the `bf_smoke_` prefix for validation so smoke runs do not clobber a
future production `raw` / `stg` / `int` / `mart` dataset set.

---

## Progress so far

| Asset | Duration | Rows resolved | Notes |
| --- | --- | --- | --- |
| `seed.*` (all 14) | < 4s each | — | ingestr / dlt / DuckDB |
| `int.snapshot` | 60ms | 1 | |
| `mart.analysis_examples`, `mart.market_positioning` | < 50ms each | small | |
| `raw.npm_registry` | 1m27s | 500 packages | `repository_url` populated on 496/500 |
| `raw.pypi_registry` | 2m12s | 500 packages | `repository_url` populated on 469/500 |
| `raw.source_health` | 4.4s | per-source rows | |
| `raw.deps_dev` | **5.5s** | 1000 rows | New zero-BQ path worked: reads seed + joins registry |
| `raw.github_repos` | 22s | 847 repos | GraphQL batched 25/query |
| `raw.osv` | 33s | | POST /v1/query |
| `raw.github_commits` | 1m26s | 847 repos | |
| `raw.github_contributors` | **25+ min before kill** | — | See **Issue 2** |

---

## Issue 1 — Resolved: "Resolved 66 packages" was not repo coverage

Several raw.github_* assets log "Resolved 66 packages" on entry.
Ground-truth query against the same DuckDB file from outside the run:

```text
raw.npm_registry:   500 rows, 496 with repository_url
raw.pypi_registry:  500 rows, 469 with repository_url
unique canonical github urls (repo_url_canonical across both): 847
```

So `pipeline.lib.live.repo_urls_from_duckdb()` should return ~847, not
66. Canonicalisation itself is fine — direct test returned 847 when run
offline against the same DB file.

Cycle-2 check after killing the stalled run showed the concern was a
misread of Bruin output, not a data bug:

```text
raw.npm_registry:     500 rows, 496 with repository_url
raw.pypi_registry:    500 rows, 469 with repository_url
raw.deps_dev:        1000 rows
raw.github_repos:     847 rows
raw.github_commits:   847 rows
repo_urls_from_duckdb: 847 canonical GitHub URLs
```

The "Resolved 66 packages" line is Bruin's Python environment/package
resolution output, not the number of package or repo rows processed by
the asset. Added a focused regression test for
`repo_urls_from_duckdb()` so future changes protect the 847-style
registry-to-GitHub fanout.

## Issue 2 — `raw.github_contributors` stalled

Started at `[18:56:39]` with "Resolved 66 packages". After 25+ minutes
wall clock:

- Bruin parent pid 74152 still alive.
- Python asset worker pid 79362 in state `SN` (sleeping), 0% CPU.
- 4 ESTABLISHED TCP connections to `api.github.com` per `lsof`.
- Zero progress in the log since the "Resolved 66 packages" line.
- Zero new files in `.cache/http/` for 5+ minutes (expected — the
  `/stats/contributors` code path in `raw_github_contributors.py`
  deliberately bypasses `HttpClient` disk cache).
- GitHub rate limit check: `core used=867 remaining=4133` — NOT rate
  limited.

**Fix applied.** `_stats_contributors` now wraps each
`client.get(...)` in `asyncio.wait_for(...)`, logs timeout/failure/poll
events with `repo=owner/name` and attempt counts, and caps polling at 2
attempts with a 3-second stepped wait. A single stuck repo now returns
missing contributor evidence instead of blocking the entire asset.

Regression tests cover both the timeout path and a 202-then-200 pending
stats retry.

## Logging standard added in cycle 2

Live raw assets now emit standardized, Bruin-visible Python logs through
the `pipeline` logger. The handler is installed once per Python asset
process, writes to stdout so Bruin captures it in real time, and includes
Bruin context:

```text
level=INFO asset=raw.github_repos run_id=<id> logger=pipeline.lib.sources event=source_start source=github_repos window=2026-04-20
level=INFO asset=raw.github_repos run_id=<id> logger=pipeline.lib.live event=repo_urls_resolved db_path=/.../local_live_bq.duckdb registry_rows=1000 canonical_urls=847
level=INFO asset=raw.github_repos run_id=<id> logger=pipeline.lib.sources event=source_finish source=github_repos window=2026-04-20 status=ok rows=847 latency_ms=21909.0 failures=0 note=
```

Useful event names:

- `source_start` / `source_finish` — emitted by every live raw source
  that uses `live.tracker(...)`.
- `source_degraded` / `source_failed` / `source_exception` — emitted
  when source-health status changes or an exception escapes an asset.
- `universe_resolved` — package universe count and seed provenance for
  npm/PyPI assets.
- `repo_urls_resolved` — registry rows read and canonical GitHub repo
  URLs discovered before GitHub fanout.
- `github_contributors_stats_pending`,
  `github_contributors_stats_timeout`,
  `github_contributors_stats_request_failed`, and
  `github_contributors_stats_unexpected_status` — repo-scoped
  diagnostics for the GitHub stats endpoint.

Set `BUS_FACTOR_LOG_LEVEL=DEBUG` to raise verbosity for project logs
without changing code. Current events are INFO/WARNING/ERROR only.

## Issue 3 — workers=2 caused DuckDB lock conflict

First live-run attempt used `--workers=2`:

```text
dlt.destinations.exceptions.DestinationConnectionError:
Connection with `client_type=DuckDbSqlClient` to `dataset_name=seed` failed.
IO Error: Could not set lock on file ".../data/local_live_bq.duckdb":
Conflicting lock is held by ... (PID 36539)
```

DuckDB allows only one writer on a file. Bruin can parallelise Python +
SQL asset steps, but when two workers both try to write via different
ingestr subprocesses we hit the lock. **Mitigation:** run with
`--workers=1` locally. For CI we either (a) keep workers=1, (b) switch
ingestr destination from DuckDB to BigQuery, or (c) serialize the
`seed.*` group via tags and only parallelise post-staging.

## Issue 4 — sandbox friction (process, not pipeline)

Every `bruin run` / `uv tool run ingestr` command needs
`required_permissions: ["all"]` because ingestr writes to
`~/.local/share/uv/tools/.tmp*`, which is outside the workspace write
allowlist. Plain `full_network` isn't sufficient.

This doesn't affect the production pipeline — only agent-driven
invocations from this sandboxed shell. Documenting so that future runs
don't start with the `Operation not permitted (os error 1)` red
herring.

## Issue 5 — Resolved: all-null contributor share column vanished

In a 5/5 live smoke, every `/stats/contributors` request returned 202
pending through the bounded retry window. The raw rows were still useful
(`contributors_last_365d = 0`, share unknown), but `top_contributor_share_365d`
was all null and the loader inferred a table without that column. Then
`stg.github_contributors` failed at bind time.

Fix: `raw.github_contributors` now builds the live DataFrame through a
typed helper that always includes `repo_url`,
`top_contributor_share_365d` as `float64`, and
`contributors_last_365d` as `int64`, even when every share is unknown.

## Issue 6 — Resolved: repos without GitHub releases are valid

Live GitHub data included repositories with no release objects, so
`raw.github_releases.latest_release_date` was null. That is a valid
source state and downstream scoring already treats missing release
metadata as neutral. The `stg.github_releases.latest_release_date`
`not_null` check was fixture-biased and blocked live runs.

Fix: removed the `not_null` check for the GitHub release date in both
DuckDB and BigQuery staging siblings. Package registry latest release
dates remain required.

## Issue 7 — Resolved: known-state checks were fixture-only expectations

`seed.known_states` is a fixture calibration table, not an expected-state
truth table for arbitrary live samples. The live 5/5 smoke correctly did
not contain those fixture package names, so the `mart.package_scores`
known-state checks failed.

Fix: the two known-state custom checks now run only when
`source_mode = fixture`. They still protect fixture scoring drift, but
they no longer block live source samples.

---

## Next steps (not yet done)

1. Re-run at **50/50** first to sanity-check scale in under 10 minutes,
   before committing to another 500/500 attempt.
2. Only then run 500/500, commit `public-data/` +
   `reports/cards/latest.png`, confirm 25 MB cap and
   `mart.source_health` lists all 10 sources.
3. Run `scripts/refresh_universe.py --ecosystem npm --limit 500` (the
   billed BQ path) so the npm seed carries real `dependent_count`
   values, then re-run the snapshot.
4. Promote the BigQuery SQL siblings into the registered Bruin live asset
   tree in Cycle 3 if we want the normal `bruin run` command itself to
   materialize `raw` / `stg` / `int` / `mart` BigQuery datasets without
   the validation harness.

---

## Useful inspection commands

```bash
# Log tail
tail -f /Users/joseph/programming/github/the_bus_factor/.cache/bruin_live.log

# Asset-level progress
grep -E "^\[..:..:..\] (Running|Finished|Failed): +(raw|stg|int|mart|export|generate)\." \
    .cache/bruin_live.log | tail -40

# Project structured logs
grep -E "event=(source_start|source_finish|repo_urls_resolved|universe_resolved|github_contributors_stats_)" \
    .cache/bruin_live.log | tail -80

# Is a Python asset actually doing HTTP?
lsof -p <python_pid> 2>/dev/null | grep -E "TCP|api\.github|pypi\.org|registry\.npmjs"
find .cache/http -type f -mmin -2 | wc -l

# GitHub rate limit
curl -sS -H "Authorization: Bearer $GITHUB_INGEST_TOKEN" \
    https://api.github.com/rate_limit

# Ground-truth DuckDB content while a run is in progress
.venv/bin/python -c "
import duckdb
c = duckdb.connect('data/local_live_bq.duckdb', read_only=True)
print(c.execute('SELECT COUNT(*) FROM raw.npm_registry').fetchone())
print(c.execute('SELECT COUNT(*) FROM raw.pypi_registry').fetchone())
"
```
