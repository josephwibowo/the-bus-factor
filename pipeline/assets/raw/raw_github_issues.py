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
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import HttpClient

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
GITHUB_REST = "https://api.github.com/repos"
PER_PAGE = 100
MAX_ISSUE_PAGES = 3  # up to 300 issues in last 180d per repo
COMMENTS_PAGE_CAP = 3
MAINTAINER_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}
ISSUE_COLUMNS = [
    "repo_url",
    "issues_opened_last_180d",
    "median_time_to_first_response_days",
]


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_issues.csv")
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _empty_issue_row(url: str) -> dict[str, Any]:
    return {
        "repo_url": url,
        "issues_opened_last_180d": 0,
        "median_time_to_first_response_days": 0.0,
    }


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_bot_user(raw_user: object) -> bool:
    if not isinstance(raw_user, dict):
        return False
    login = str(raw_user.get("login") or "").lower()
    user_type = str(raw_user.get("type") or "").lower()
    return user_type == "bot" or login.endswith("[bot]")


def _is_maintainer_like(item: dict[str, Any]) -> bool:
    if _is_bot_user(item.get("user")):
        return False
    association = str(item.get("author_association") or "").upper()
    return association in MAINTAINER_ASSOCIATIONS


def _is_eligible_issue(item: dict[str, Any], *, since_dt: datetime, until_dt: datetime) -> bool:
    if "pull_request" in item:
        return False
    if _is_bot_user(item.get("user")):
        return False
    if _is_maintainer_like(item):
        return False
    created = _parse_iso(item.get("created_at"))
    return created is not None and since_dt <= created < until_dt


async def _first_maintainer_response_days(
    owner: str,
    repo: str,
    issue: dict[str, Any],
    client: HttpClient,
    snapshot_week: date,
) -> float | None:
    """Return days between issue open and first maintainer-like comment.

    Maintainer-like means GitHub marks the comment author as OWNER, MEMBER, or
    COLLABORATOR. Community comments and bots do not count as maintainer
    responses.
    """

    number = issue.get("number")
    if not number:
        return None
    author = (issue.get("user") or {}).get("login")
    created = _parse_iso(issue.get("created_at"))
    if not author or not created:
        return None
    until_dt = datetime.combine(snapshot_week, datetime.min.time(), UTC)
    for page in range(1, COMMENTS_PAGE_CAP + 1):
        payload = await client.get_json(
            f"{GITHUB_REST}/{owner}/{repo}/issues/{number}/comments",
            params={"per_page": PER_PAGE, "page": page},
            missing_statuses=(404, 410, 451),
        )
        if not isinstance(payload, list) or not payload:
            return None
        for comment in payload:
            if not isinstance(comment, dict):
                continue
            commenter = (comment.get("user") or {}).get("login")
            when = _parse_iso(comment.get("created_at"))
            if not commenter or when is None or when >= until_dt:
                continue
            if commenter == author:
                continue
            if not _is_maintainer_like(comment):
                continue
            return max(0.0, (when - created).total_seconds() / 86400.0)
        if len(payload) < PER_PAGE:
            return None
    return None


async def _fetch_repo(url: str, client: HttpClient, snapshot_week: date) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    until_dt = datetime.combine(snapshot_week, datetime.min.time(), UTC)
    since_dt = until_dt - timedelta(days=180)
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
            if not isinstance(item, dict):
                continue
            if not _is_eligible_issue(item, since_dt=since_dt, until_dt=until_dt):
                continue
            issues.append(item)
        if len(payload) < PER_PAGE:
            break

    responses: list[float] = []
    for issue in issues:
        days = await _first_maintainer_response_days(owner, repo, issue, client, snapshot_week)
        if days is not None:
            responses.append(days)
    median = statistics.median(responses) if responses else (0.0 if not issues else None)
    return {
        "repo_url": url,
        "issues_opened_last_180d": len(issues),
        "median_time_to_first_response_days": median,
    }


async def _ingest(
    window: str, urls: list[str], snapshot_week: date
) -> tuple[list[dict[str, Any]], int]:
    async with HttpClient(window=window, concurrency=6) as client:
        results = await asyncio.gather(
            *[_fetch_repo(u, client, snapshot_week) for u in urls], return_exceptions=True
        )
    rows: list[dict[str, Any]] = []
    exception_count = 0
    for url, res in zip(urls, results, strict=True):
        if isinstance(res, BaseException):
            exception_count += 1
            rows.append(_empty_issue_row(url))
            continue
        if res is None:
            rows.append(_empty_issue_row(url))
            continue
        rows.append(res)
    return rows, exception_count


def _rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for column in ISSUE_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[ISSUE_COLUMNS]
    df["issues_opened_last_180d"] = (
        pd.to_numeric(df["issues_opened_last_180d"], errors="coerce").fillna(0).astype("int64")
    )
    df["median_time_to_first_response_days"] = pd.to_numeric(
        df["median_time_to_first_response_days"], errors="coerce"
    )
    df.loc[
        df["issues_opened_last_180d"].eq(0) & df["median_time_to_first_response_days"].isna(),
        "median_time_to_first_response_days",
    ] = 0.0
    df["median_time_to_first_response_days"] = df["median_time_to_first_response_days"].astype(
        "float64"
    )
    return df


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    snapshot_week = live.resolve_window_date()
    with live.tracker("github_issues") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count = asyncio.run(_ingest(window, urls, snapshot_week))
        attempted = len(urls)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no github_issues repo urls resolved")
        elif not rows:
            t.mark_failed("no github_issues rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_issues",
                attempted=attempted,
                succeeded=len(rows),
                exception_count=exception_count,
            )
    df = _rows_to_frame(rows)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
