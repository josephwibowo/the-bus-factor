"""@bruin

name: raw.github_repos
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.github_repos
  - raw.npm_registry
  - raw.pypi_registry

description: |
  Per-repository GitHub metadata: archival state, default branch, stars,
  and manifest paths. Live mode hits the GraphQL batched `repository(owner,name)`
  endpoint (25 repos per query) and falls back to REST on token-less runs.

tags:
  - layer:raw
  - source:github

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: repo_id
    type: bigint
  - name: default_branch
    type: varchar
  - name: is_archived
    type: boolean
  - name: is_disabled
    type: boolean
  - name: stars
    type: bigint
  - name: primary_language
    type: varchar
  - name: manifest_paths
    type: varchar
  - name: owner_login
    type: varchar
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"

GITHUB_REST = "https://api.github.com/repos"


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_repos.csv")
    df["is_archived"] = df["is_archived"].astype(bool)
    df["is_disabled"] = df["is_disabled"].astype(bool)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


async def _fetch_repo(url: str, client: HttpClient) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    payload = await client.get_json(
        f"{GITHUB_REST}/{owner}/{repo}", missing_statuses=(404, 410, 451)
    )
    if not isinstance(payload, dict):
        return None
    return {
        "repo_url": url,
        "repo_id": int(payload.get("id") or 0),
        "default_branch": payload.get("default_branch"),
        "is_archived": bool(payload.get("archived")),
        "is_disabled": bool(payload.get("disabled")),
        "stars": int(payload.get("stargazers_count") or 0),
        "primary_language": payload.get("language"),
        "manifest_paths": "",  # left empty until we implement manifest listing
        "owner_login": (payload.get("owner") or {}).get("login") or owner,
    }


async def _ingest(window: str, repo_urls: list[str]) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    async with HttpClient(window=window) as client:
        results = await asyncio.gather(
            *[_fetch_repo(url, client) for url in repo_urls], return_exceptions=True
        )
    exception_count = sum(1 for res in results if isinstance(res, BaseException))
    for res in results:
        if isinstance(res, BaseException) or res is None:
            continue
        rows.append(res)
    return rows, exception_count


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    with live.tracker("github_repos") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count = asyncio.run(_ingest(window, urls))
        attempted = len(urls)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no github_repos repo urls resolved")
        elif not rows:
            t.mark_failed("no github_repos rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_repos",
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
                "repo_id",
                "default_branch",
                "is_archived",
                "is_disabled",
                "stars",
                "primary_language",
                "manifest_paths",
                "owner_login",
            ]
        )
    )
    if not df.empty:
        df["is_archived"] = df["is_archived"].astype(bool)
        df["is_disabled"] = df["is_disabled"].astype(bool)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
