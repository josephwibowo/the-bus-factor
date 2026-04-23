"""@bruin

name: raw.scorecard
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.scorecard
  - raw.github_repos

description: |
  OpenSSF Scorecard aggregate scores per repository. Live mode hits the
  public Scorecard REST API (api.securityscorecards.dev) per repo. We avoid
  the Scorecard BigQuery dataset (~2 TB, unclustered) because our
  package-universe-sized lookup is cheaper as bounded HTTP.

tags:
  - layer:raw
  - source:scorecard

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: aggregate_score
    type: double
  - name: check_count
    type: bigint
  - name: scorecard_date
    type: date
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
SCORECARD_API = "https://api.securityscorecards.dev/projects"


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "scorecard.csv")
    df["scorecard_date"] = pd.to_datetime(df["scorecard_date"]).dt.date
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _strip_repo_name(url: str) -> str:
    return url.removeprefix("https://github.com/")


def _iso_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None


async def _fetch_repo(url: str, client: HttpClient) -> dict[str, Any] | None:
    path = _strip_repo_name(url).lower()
    if not path or "/" not in path:
        return None
    api_url = f"{SCORECARD_API}/github.com/{path}"
    payload = await client.get_json(api_url, missing_statuses=(404, 410, 451))
    if not isinstance(payload, dict):
        return None
    score = payload.get("score")
    checks = payload.get("checks") or []
    if not isinstance(checks, list):
        checks = []
    return {
        "repo_url": f"https://github.com/{path}",
        "aggregate_score": float(score) if isinstance(score, int | float) else None,
        "check_count": len(checks),
        "scorecard_date": _iso_date(payload.get("date")),
    }


async def _ingest(window: str, urls: list[str]) -> list[dict[str, Any]]:
    async with HttpClient(window=window, concurrency=8) as client:
        results = await asyncio.gather(
            *[_fetch_repo(u, client) for u in urls], return_exceptions=True
        )
    rows: list[dict[str, Any]] = []
    for res in results:
        if isinstance(res, BaseException) or res is None:
            continue
        rows.append(res)
    return rows


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    with live.tracker("openssf_scorecard") as t:
        urls = [
            u
            for u in live.repo_urls_from_duckdb(live.duckdb_path())
            if u.startswith("https://github.com/")
        ]
        rows = asyncio.run(_ingest(window, urls)) if urls else []
        t.row_count = len(rows)
        if not rows:
            t.mark_failed("no scorecard rows ingested")
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(columns=["repo_url", "aggregate_score", "check_count", "scorecard_date"])
    )
    if not df.empty:
        df["scorecard_date"] = pd.to_datetime(df["scorecard_date"]).dt.date
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
