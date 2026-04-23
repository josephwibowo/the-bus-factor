"""@bruin

name: raw.github_issues
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.github_issues
  - raw.github_repos

description: |
  Per-repo aggregated issue responsiveness: opened-in-180d and median time
  to first maintainer response on eligible issues. Live mode paginates
  `/issues?since=<180d>` and reads the first comment per issue to compute
  time-to-first-maintainer-response in Python (avoids re-reading comments
  in SQL).

tags:
  - layer:raw
  - source:github

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: issues_opened_last_180d
    type: bigint
  - name: median_time_to_first_response_days
    type: double
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
import statistics
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
GITHUB_REST = "https://api.github.com/repos"
PER_PAGE = 100
MAX_ISSUE_PAGES = 3  # up to 300 issues in last 180d per repo
COMMENTS_PAGE_CAP = 1  # first page of comments is enough for first-response


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_issues.csv")
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


async def _first_maintainer_response_days(
    owner: str, repo: str, issue: dict[str, Any], client: HttpClient
) -> float | None:
    """Return days between issue open and first comment from someone other
    than the issue author. Returns ``None`` when we can't determine that.
    """

    number = issue.get("number")
    if not number:
        return None
    author = (issue.get("user") or {}).get("login")
    created = _parse_iso(issue.get("created_at"))
    if not author or not created:
        return None
    payload = await client.get_json(
        f"{GITHUB_REST}/{owner}/{repo}/issues/{number}/comments",
        params={"per_page": PER_PAGE, "page": 1},
        missing_statuses=(404, 410, 451),
    )
    if not isinstance(payload, list):
        return None
    for comment in payload:
        if not isinstance(comment, dict):
            continue
        commenter = (comment.get("user") or {}).get("login")
        when = _parse_iso(comment.get("created_at"))
        if not commenter or when is None:
            continue
        if commenter == author:
            continue
        return max(0.0, (when - created).total_seconds() / 86400.0)
    return None


async def _fetch_repo(url: str, client: HttpClient) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    since_dt = datetime.now(UTC) - timedelta(days=180)
    since = since_dt.isoformat()

    issues: list[dict[str, Any]] = []
    for page in range(1, MAX_ISSUE_PAGES + 1):
        payload = await client.get_json(
            f"{GITHUB_REST}/{owner}/{repo}/issues",
            params={"since": since, "state": "all", "per_page": PER_PAGE, "page": page},
            missing_statuses=(404, 410, 451),
        )
        if not isinstance(payload, list) or not payload:
            break
        for item in payload:
            if not isinstance(item, dict) or "pull_request" in item:
                continue
            created = _parse_iso(item.get("created_at"))
            if created is None or created < since_dt:
                continue
            issues.append(item)
        if len(payload) < PER_PAGE:
            break

    responses: list[float] = []
    for issue in issues:
        days = await _first_maintainer_response_days(owner, repo, issue, client)
        if days is not None:
            responses.append(days)
    median = statistics.median(responses) if responses else None
    return {
        "repo_url": url,
        "issues_opened_last_180d": len(issues),
        "median_time_to_first_response_days": median,
    }


async def _ingest(window: str, urls: list[str]) -> list[dict[str, Any]]:
    async with HttpClient(window=window, concurrency=6) as client:
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
    with live.tracker("github_issues") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows = asyncio.run(_ingest(window, urls))
        t.row_count = len(rows)
        if not rows:
            t.mark_failed("no github_issues rows ingested")
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=["repo_url", "issues_opened_last_180d", "median_time_to_first_response_days"]
        )
    )
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
