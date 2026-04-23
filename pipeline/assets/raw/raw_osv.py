"""@bruin

name: raw.osv
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.osv
  - raw.npm_registry
  - raw.pypi_registry

description: |
  OSV advisories affecting tracked packages: direct vulnerability count and
  highest severity. Live mode calls https://api.osv.dev/v1/querybatch for
  both npm and PyPI universes resolved at ingest time.

tags:
  - layer:raw
  - source:osv

columns:
  - name: ecosystem
    type: varchar
  - name: package_name
    type: varchar
  - name: direct_vuln_count
    type: bigint
  - name: highest_severity
    type: varchar
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
logger = logging.getLogger(__name__)

OSV_BATCH_URL = "https://api.osv.dev/v1/querybatch"
OSV_VULN_URL = "https://api.osv.dev/v1/vulns"
BATCH_SIZE = 100

SEVERITY_ORDER = {"low": 1, "moderate": 2, "medium": 2, "high": 3, "critical": 4}


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "osv.csv")
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _ecosystem_key(ecosystem: str) -> str:
    return {"npm": "npm", "pypi": "PyPI"}[ecosystem]


def _normalise_severity(value: str | None) -> str | None:
    if not value:
        return None
    val = value.lower()
    if val == "moderate":
        return "medium"
    return val if val in {"low", "medium", "high", "critical"} else None


async def _fetch_vuln_severity(vuln_id: str, client: HttpClient) -> str | None:
    payload = await client.get_json(f"{OSV_VULN_URL}/{vuln_id}", missing_statuses=(404,))
    if not isinstance(payload, dict):
        return None
    db_specific = payload.get("database_specific") or {}
    sev = db_specific.get("severity") if isinstance(db_specific, dict) else None
    return _normalise_severity(sev if isinstance(sev, str) else None)


async def _query_batch(
    items: list[tuple[str, str]], client: HttpClient
) -> list[list[dict[str, Any]]]:
    body = {
        "queries": [
            {"package": {"ecosystem": _ecosystem_key(eco), "name": name}} for eco, name in items
        ]
    }
    payload = await client.post_json(OSV_BATCH_URL, json_body=body)
    results = []
    if isinstance(payload, dict) and "results" in payload:
        for entry in payload["results"]:
            vulns = entry.get("vulns") if isinstance(entry, dict) else None
            results.append(list(vulns or []))
    return results


async def _ingest(window: str, pairs: list[tuple[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    async with HttpClient(window=window) as client:
        for start in range(0, len(pairs), BATCH_SIZE):
            chunk = pairs[start : start + BATCH_SIZE]
            batch = await _query_batch(chunk, client)
            # Gather all unique vuln ids in this chunk to look up severities.
            vuln_ids: set[str] = {
                str(v.get("id"))
                for vulns in batch
                for v in vulns
                if isinstance(v, dict) and v.get("id")
            }
            sev_lookup = await asyncio.gather(
                *[_fetch_vuln_severity(vid, client) for vid in vuln_ids],
                return_exceptions=True,
            )
            sev_map: dict[str, str | None] = {}
            for vid, sev_result in zip(vuln_ids, sev_lookup, strict=True):
                sev_map[vid] = None if isinstance(sev_result, BaseException) else sev_result
            for (eco, name), vulns in zip(chunk, batch, strict=True):
                count = len(vulns)
                highest: str | None = None
                best_rank = 0
                for vuln in vulns:
                    if not isinstance(vuln, dict):
                        continue
                    vuln_id = vuln.get("id")
                    vuln_sev = sev_map.get(str(vuln_id)) if vuln_id else None
                    if vuln_sev and SEVERITY_ORDER.get(vuln_sev, 0) > best_rank:
                        best_rank = SEVERITY_ORDER[vuln_sev]
                        highest = vuln_sev
                rows.append(
                    {
                        "ecosystem": eco,
                        "package_name": name,
                        "direct_vuln_count": count,
                        "highest_severity": highest,
                    }
                )
    return rows


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    with live.tracker("osv") as t:
        npm_pkgs = list(live.resolve_universe("npm", window=window))
        pypi_pkgs = list(live.resolve_universe("pypi", window=window))
        pairs: list[tuple[str, str]] = [("npm", p) for p in npm_pkgs] + [
            ("pypi", p) for p in pypi_pkgs
        ]
        rows = asyncio.run(_ingest(window, pairs))
        t.row_count = len(rows)
        if not rows:
            t.mark_failed("no OSV rows ingested")
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=["ecosystem", "package_name", "direct_vuln_count", "highest_severity"]
        )
    )
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
