"""@bruin

name: raw.pypi_registry
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.pypi_registry

description: |
  PyPI project metadata snapshot. Fixture mode returns the committed CSV;
  live mode reads https://pypi.org/pypi/{pkg}/json for the top-N universe
  and (optionally) the pypistats /recent endpoint for 90-day downloads.

tags:
  - layer:raw
  - source:pypi

columns:
  - name: package_name
    type: varchar
    primary_key: true
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
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
logger = logging.getLogger(__name__)

PYPI_BASE = "https://pypi.org/pypi"
PYPISTATS_BASE = "https://pypistats.org/api/packages"


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "pypi_registry.csv")
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


def _extract_urls(info: dict[str, Any]) -> tuple[str | None, str | None]:
    homepage = info.get("home_page") or None
    project_urls = info.get("project_urls") or {}
    repository_url = None
    if isinstance(project_urls, dict):
        for key in ("Source", "Source Code", "Repository", "Homepage", "Code", "GitHub"):
            val = project_urls.get(key)
            if val and "github.com" in str(val).lower():
                repository_url = str(val)
                break
        if repository_url is None:
            for val in project_urls.values():
                if val and "github.com" in str(val).lower():
                    repository_url = str(val)
                    break
    if homepage and "github.com" in homepage.lower() and not repository_url:
        repository_url = homepage
    return homepage, repository_url


def _is_inactive(classifiers: list[str] | None) -> bool:
    if not classifiers:
        return False
    return any("Development Status :: 7" in c for c in classifiers)


def _parse(pkg: str, payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    info = payload.get("info") or {}
    releases = payload.get("releases") or {}
    latest_version = info.get("version")
    all_dates: list[date] = []
    latest_date: date | None = None
    for version, files in releases.items():
        if not files:
            continue
        dates_maybe = [
            _iso_date(f.get("upload_time_iso_8601") or f.get("upload_time")) for f in files
        ]
        dates = [d for d in dates_maybe if d is not None]
        if not dates:
            continue
        version_date = min(dates)
        all_dates.append(version_date)
        if version == latest_version:
            latest_date = max(dates)
    if latest_date is None and all_dates:
        latest_date = max(all_dates)
    first_date: date | None = min(all_dates) if all_dates else None
    homepage, repo = _extract_urls(info)
    return {
        "package_name": pkg,
        "latest_version": latest_version,
        "first_release_date": first_date,
        "latest_release_date": latest_date,
        "homepage_url": homepage,
        "repository_url": repo,
        "is_deprecated": _is_inactive(info.get("classifiers")),
        "is_archived": False,
        "publisher": info.get("author") or info.get("maintainer") or None,
    }


async def _fetch_meta(pkg: str, client: HttpClient) -> dict[str, Any] | None:
    url = f"{PYPI_BASE}/{pkg}/json"
    payload = await client.get_json(url, missing_statuses=(404, 410))
    return _parse(pkg, payload)


async def _fetch_downloads(pkg: str, client: HttpClient) -> int:
    url = f"{PYPISTATS_BASE}/{pkg}/recent"
    payload = await client.get_json(url, params={"period": "month"}, missing_statuses=(404,))
    if not isinstance(payload, dict):
        return 0
    data = payload.get("data") or {}
    if isinstance(data, dict):
        # 30-day window scaled to 90 days for comparability with npm.
        last_month = int(data.get("last_month") or 0)
        return last_month * 3
    return 0


async def _ingest(window: str, packages: list[str]) -> tuple[list[dict[str, Any]], int]:
    async with HttpClient(window=window) as client:
        metas = await asyncio.gather(
            *[_fetch_meta(pkg, client) for pkg in packages], return_exceptions=True
        )
        downloads = await asyncio.gather(
            *[_fetch_downloads(pkg, client) for pkg in packages], return_exceptions=True
        )
    meta_errors = sum(1 for meta in metas if isinstance(meta, BaseException))
    download_errors = sum(1 for dl in downloads if isinstance(dl, BaseException))
    rows: list[dict[str, Any]] = []
    for _pkg, meta, dl in zip(packages, metas, downloads, strict=True):
        if isinstance(meta, BaseException) or meta is None:
            continue
        dl_val = 0 if isinstance(dl, BaseException) else int(dl)
        row = dict(meta)
        row["downloads_90d"] = dl_val
        rows.append(row)
    return rows, (meta_errors + download_errors)


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    with live.tracker("pypi_registry") as t:
        packages = list(live.resolve_universe("pypi", window=window))
        rows, exception_count = asyncio.run(_ingest(window, packages))
        attempted = len(packages)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no PyPI packages resolved for ingestion")
        elif not rows:
            t.mark_failed("no PyPI rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="pypi_registry",
                attempted=attempted,
                succeeded=len(rows),
                exception_count=exception_count,
            )
    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(
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
    if not df.empty:
        df["is_deprecated"] = df["is_deprecated"].astype(bool)
        df["is_archived"] = df["is_archived"].astype(bool)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
