"""@bruin

name: raw.github_commits
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.github_commits
  - raw.github_repos

description: |
  Per-repo aggregated commit activity for the reporting window. Live mode
  paginates GET /repos/{owner}/{repo}/commits over the default branch for
  the last 365 days, capped at a reasonable page budget per repo.

tags:
  - layer:raw
  - source:github

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: last_commit_date
    type: date
  - name: commits_last_365d
    type: bigint
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
GITHUB_REST = "https://api.github.com/repos"
PER_PAGE = 100
MAX_PAGES = 4  # capped at 400 commits per repo in last 365d


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_commits.csv")
    df["last_commit_date"] = pd.to_datetime(df["last_commit_date"]).dt.date
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _parse_commit_date(raw: dict[str, Any]) -> date | None:
    commit = raw.get("commit") or {}
    author = commit.get("author") or {}
    iso = author.get("date") or raw.get("commit", {}).get("committer", {}).get("date")
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).date()
    except ValueError:
        return None


async def _fetch_commits(
    url: str, client: HttpClient, snapshot_week: date
) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    since = datetime.combine(snapshot_week - timedelta(days=365), datetime.min.time(), UTC)
    until = datetime.combine(snapshot_week, datetime.min.time(), UTC)
    total = 0
    latest: date | None = None
    for page in range(1, MAX_PAGES + 1):
        payload = await client.get_json(
            f"{GITHUB_REST}/{owner}/{repo}/commits",
            params={
                "since": since.isoformat(),
                "until": until.isoformat(),
                "per_page": PER_PAGE,
                "page": page,
            },
            missing_statuses=(404, 409, 451),
        )
        if not isinstance(payload, list) or not payload:
            break
        total += len(payload)
        first_date = _parse_commit_date(payload[0])
        if first_date is not None and (latest is None or first_date > latest):
            latest = first_date
        if len(payload) < PER_PAGE:
            break
    return {
        "repo_url": url,
        "last_commit_date": latest,
        "commits_last_365d": total,
    }


async def _ingest(
    window: str, urls: list[str], snapshot_week: date
) -> tuple[list[dict[str, Any]], int]:
    async with HttpClient(window=window, concurrency=8) as client:
        results = await asyncio.gather(
            *[_fetch_commits(url, client, snapshot_week) for url in urls],
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
    with live.tracker("github_commits") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count = asyncio.run(_ingest(window, urls, snapshot_week))
        attempted = len(urls)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no github_commits repo urls resolved")
        elif not rows:
            t.mark_failed("no github_commits rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_commits",
                attempted=attempted,
                succeeded=len(rows),
                exception_count=exception_count,
            )
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(columns=["repo_url", "last_commit_date", "commits_last_365d"])
    )
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
