"""@bruin

name: raw.source_health
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.source_health

description: |
  Per-source ingestion telemetry. In fixture mode this mirrors the committed
  source_health CSV (same data, different code path). In live mode it drains
  the reporting-window buffer that each raw asset writes to via
  pipeline/lib/sources.py.

tags:
  - layer:raw
  - source:meta

columns:
  - name: source_name
    type: varchar
    primary_key: true
  - name: status
    type: varchar
  - name: last_success_at
    type: timestamp
  - name: stale
    type: boolean
  - name: failure_count
    type: bigint
  - name: note
    type: varchar
  - name: latency_ms
    type: double
  - name: row_count
    type: bigint
  - name: window
    type: varchar
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.lib import live, sources

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"

# Ten canonical ingestion sources that must appear every run.
EXPECTED_SOURCES: tuple[str, ...] = (
    "npm_registry",
    "pypi_registry",
    "deps_dev",
    "github_repos",
    "github_commits",
    "github_releases",
    "github_issues",
    "github_contributors",
    "osv",
    "openssf_scorecard",
)


def _fixture_frame() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "source_health.csv")
    df["latency_ms"] = 0.0
    df["row_count"] = 0
    df["window"] = ""
    return df


def _live_frame(window: str) -> pd.DataFrame:
    buffered = sources.dedupe_latest_per_source(sources.read_buffer(window))
    rows = [
        {
            "source_name": row.source_name,
            "status": row.status,
            "last_success_at": row.last_success_at,
            "stale": row.stale,
            "failure_count": int(row.failure_count),
            "note": row.note,
            "latency_ms": float(row.latency_ms),
            "row_count": int(row.row_count),
            "window": row.window,
        }
        for row in buffered
    ]
    present = {r["source_name"] for r in rows}
    for name in EXPECTED_SOURCES:
        if name in present:
            continue
        rows.append(
            {
                "source_name": name,
                "status": "failed",
                "last_success_at": None,
                "stale": True,
                "failure_count": 1,
                "note": "no telemetry emitted by ingestion asset this run",
                "latency_ms": 0.0,
                "row_count": 0,
                "window": window,
            }
        )
    return pd.DataFrame(rows)


def materialize() -> pd.DataFrame:
    df = _live_frame(live.resolve_window()) if live.live_mode() else _fixture_frame()
    df["last_success_at"] = pd.to_datetime(df["last_success_at"], errors="coerce", utc=True)
    df["stale"] = df["stale"].astype(bool)
    df["failure_count"] = df["failure_count"].astype("int64")
    df["row_count"] = df["row_count"].astype("int64")
    df["latency_ms"] = df["latency_ms"].astype(float)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df
