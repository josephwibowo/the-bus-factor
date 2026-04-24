"""@bruin

name: raw.github_releases
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.github_releases
  - raw.github_repos

description: |
  Per-repo release counts covering the last 365 days and the prior 365 days
  window. Drives release_cadence_decay fragility input.

tags:
  - layer:raw
  - source:github

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: releases_last_365d
    type: bigint
  - name: releases_prior_365d
    type: bigint
  - name: latest_release_tag
    type: varchar
  - name: latest_release_date
    type: date
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
GITHUB_REST = "https://api.github.com/repos"
PER_PAGE = 100
MAX_PAGES = 4


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_releases.csv")
    df["latest_release_date"] = pd.to_datetime(df["latest_release_date"]).dt.date
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _parse_date(iso: str | None) -> date | None:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).date()
    except ValueError:
        return None


async def _fetch_releases(
    url: str, client: HttpClient, snapshot_week: date
) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    now = snapshot_week
    last_cutoff = snapshot_week - timedelta(days=365)
    prior_cutoff = now - timedelta(days=730)

    all_releases: list[tuple[date, str]] = []
    for page in range(1, MAX_PAGES + 1):
        payload = await client.get_json(
            f"{GITHUB_REST}/{owner}/{repo}/releases",
            params={"per_page": PER_PAGE, "page": page},
            missing_statuses=(404, 409, 451),
        )
        if not isinstance(payload, list) or not payload:
            break
        for rel in payload:
            if not isinstance(rel, dict) or rel.get("draft"):
                continue
            d = _parse_date(rel.get("published_at") or rel.get("created_at"))
            if d is None:
                continue
            if d >= snapshot_week:
                continue
            all_releases.append((d, str(rel.get("tag_name") or "")))
        if len(payload) < PER_PAGE:
            break

    last = sum(1 for d, _ in all_releases if last_cutoff <= d < now)
    prior = sum(1 for d, _ in all_releases if prior_cutoff <= d < last_cutoff)
    latest = max(all_releases, default=None)
    return {
        "repo_url": url,
        "releases_last_365d": last,
        "releases_prior_365d": prior,
        "latest_release_tag": latest[1] if latest else None,
        "latest_release_date": latest[0] if latest else None,
    }


async def _ingest(
    window: str, urls: list[str], snapshot_week: date
) -> tuple[list[dict[str, Any]], int]:
    async with HttpClient(window=window, concurrency=8) as client:
        results = await asyncio.gather(
            *[_fetch_releases(url, client, snapshot_week) for url in urls],
            return_exceptions=True,
        )
    rows: list[dict[str, Any]] = []
    exception_count = sum(1 for res in results if isinstance(res, BaseException))
    for res in results:
        if isinstance(res, BaseException) or res is None:
            continue
        rows.append(res)
    return rows, exception_count


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    snapshot_week = live.resolve_window_date()
    with live.tracker("github_releases") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count = asyncio.run(_ingest(window, urls, snapshot_week))
        attempted = len(urls)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no github_releases repo urls resolved")
        elif not rows:
            t.mark_failed("no github_releases rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_releases",
                attempted=attempted,
                succeeded=len(rows),
                exception_count=exception_count,
            )
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=[
                "repo_url",
                "releases_last_365d",
                "releases_prior_365d",
                "latest_release_tag",
                "latest_release_date",
            ]
        )
    )
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
