"""@bruin

name: raw.npm_registry
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.npm_registry

description: |
  npm registry package metadata snapshot for the reporting window. Fixture
  mode returns the committed CSV; live mode hits registry.npmjs.org and
  api.npmjs.org/downloads for the top-N package universe resolved by
  pipeline/lib/universe.py.

tags:
  - layer:raw
  - source:npm

columns:
  - name: package_name
    type: varchar
    primary_key: true
    description: Canonical npm package name.
  - name: latest_version
    type: varchar
  - name: first_release_date
    type: date
  - name: latest_release_date
    type: date
  - name: homepage_url
    type: varchar
  - name: repository_url
    type: varchar
  - name: is_deprecated
    type: boolean
  - name: is_archived
    type: boolean
  - name: publisher
    type: varchar
  - name: downloads_90d
    type: bigint
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
logger = logging.getLogger(__name__)

REGISTRY_BASE = "https://registry.npmjs.org"
DOWNLOADS_BASE = "https://api.npmjs.org/downloads/range"


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "npm_registry.csv")
    df["first_release_date"] = pd.to_datetime(df["first_release_date"]).dt.date
    df["latest_release_date"] = pd.to_datetime(df["latest_release_date"]).dt.date
    df["is_deprecated"] = df["is_deprecated"].astype(bool)
    df["is_archived"] = df["is_archived"].astype(bool)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _iso_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_registry(pkg: str, payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    times = payload.get("time") or {}
    versions = payload.get("versions") or {}
    dist_tags = payload.get("dist-tags") or {}
    latest_version = dist_tags.get("latest")
    if not latest_version and versions:
        latest_version = sorted(versions.keys())[-1]
    latest_entry_raw = versions.get(latest_version) if latest_version else {}
    latest_entry: dict[str, Any] = latest_entry_raw if isinstance(latest_entry_raw, dict) else {}
    repo = latest_entry.get("repository") or payload.get("repository") or {}
    repo_url = repo.get("url") if isinstance(repo, dict) else None
    homepage = latest_entry.get("homepage") or payload.get("homepage")
    author = latest_entry.get("author") or payload.get("author") or {}
    publisher: str | None = None
    if isinstance(author, dict):
        publisher = author.get("name")
    elif author:
        publisher = str(author)
    maintainers = payload.get("maintainers") or []
    if not publisher and maintainers:
        publisher = maintainers[0].get("name")
    is_deprecated = bool(latest_entry.get("deprecated")) or bool(payload.get("deprecated"))
    first_release_date = _iso_date(times.get("created"))
    latest_release_date = _iso_date(times.get(latest_version))
    return {
        "package_name": pkg,
        "latest_version": latest_version,
        "first_release_date": first_release_date,
        "latest_release_date": latest_release_date,
        "homepage_url": homepage,
        "repository_url": repo_url,
        "is_deprecated": is_deprecated,
        "is_archived": False,  # npm has no archival flag
        "publisher": publisher,
    }


async def _fetch_package(pkg: str, client: HttpClient) -> dict[str, Any] | None:
    url = f"{REGISTRY_BASE}/{pkg}"
    payload = await client.get_json(url, missing_statuses=(404, 410))
    return _parse_registry(pkg, payload)


async def _fetch_downloads(pkg: str, client: HttpClient) -> int:
    """Sum the last-90-days download count for ``pkg``.

    ``api.npmjs.org/downloads`` doesn't expose a ``last-90-days`` preset; we
    pass an explicit ``start:end`` window ending yesterday (today's data is
    unsettled).
    """

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=89)
    url = f"{DOWNLOADS_BASE}/{start.isoformat()}:{end.isoformat()}/{pkg}"
    payload = await client.get_json(url, missing_statuses=(404,))
    if not isinstance(payload, dict):
        return 0
    downloads = payload.get("downloads") or []
    if not isinstance(downloads, list):
        return 0
    return int(sum(int(d.get("downloads", 0)) for d in downloads))


async def _ingest(window: str, packages: list[str]) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    async with HttpClient(window=window) as client:
        meta_task = [_fetch_package(pkg, client) for pkg in packages]
        metas = await asyncio.gather(*meta_task, return_exceptions=True)
        dl_task = [_fetch_downloads(pkg, client) for pkg in packages]
        downloads = await asyncio.gather(*dl_task, return_exceptions=True)
    meta_errors = sum(1 for meta in metas if isinstance(meta, BaseException))
    download_errors = sum(1 for dl in downloads if isinstance(dl, BaseException))
    for _pkg, meta, dl in zip(packages, metas, downloads, strict=True):
        if isinstance(meta, BaseException) or meta is None:
            continue
        dl_val = 0 if isinstance(dl, BaseException) else int(dl)
        meta_row = dict(meta)
        meta_row["downloads_90d"] = dl_val
        rows.append(meta_row)
    return rows, (meta_errors + download_errors)


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    with live.tracker("npm_registry") as t:
        packages = list(live.resolve_universe("npm", window=window))
        rows, exception_count = asyncio.run(_ingest(window, packages))
        attempted = len(packages)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no npm packages resolved for ingestion")
        elif not rows:
            t.mark_failed("no npm registry rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="npm_registry",
                attempted=attempted,
                succeeded=len(rows),
                exception_count=exception_count,
            )
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=[
                "package_name",
                "latest_version",
                "first_release_date",
                "latest_release_date",
                "homepage_url",
                "repository_url",
                "is_deprecated",
                "is_archived",
                "publisher",
                "downloads_90d",
            ]
        )
    )
    df["is_deprecated"] = df["is_deprecated"].astype(bool) if not df.empty else df["is_deprecated"]
    df["is_archived"] = df["is_archived"].astype(bool) if not df.empty else df["is_archived"]
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
