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
  - name: top_contributor_share_365d
    type: double
  - name: unique_contributors_last_365d
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
COMMIT_COLUMNS = [
    "repo_url",
    "last_commit_date",
    "commits_last_365d",
    "top_contributor_share_365d",
    "unique_contributors_last_365d",
]


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_commits.csv")
    df["last_commit_date"] = pd.to_datetime(df["last_commit_date"]).dt.date
    missing_share = "top_contributor_share_365d" not in df.columns
    missing_unique = "unique_contributors_last_365d" not in df.columns
    if missing_share or missing_unique:
        contributor_fixture = pd.read_csv(FIXTURE_ROOT / "github_contributors.csv")
        fallback = contributor_fixture[["repo_url"]].copy()
        if missing_share:
            if "top_contributor_share_365d" in contributor_fixture.columns:
                fallback["top_contributor_share_365d"] = pd.to_numeric(
                    contributor_fixture["top_contributor_share_365d"], errors="coerce"
                )
            else:
                fallback["top_contributor_share_365d"] = None
        if missing_unique:
            if "contributors_last_365d" in contributor_fixture.columns:
                fallback["unique_contributors_last_365d"] = pd.to_numeric(
                    contributor_fixture["contributors_last_365d"], errors="coerce"
                )
            else:
                fallback["unique_contributors_last_365d"] = 0
        df = df.merge(fallback, on="repo_url", how="left")
    df["top_contributor_share_365d"] = pd.to_numeric(
        df["top_contributor_share_365d"], errors="coerce"
    ).astype("float64")
    df["unique_contributors_last_365d"] = (
        pd.to_numeric(df["unique_contributors_last_365d"], errors="coerce")
        .fillna(0)
        .astype("int64")
    )
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _empty_commit_row(url: str) -> dict[str, Any]:
    return {
        "repo_url": url,
        "last_commit_date": None,
        "commits_last_365d": 0,
        "top_contributor_share_365d": 0.0,
        "unique_contributors_last_365d": 0,
    }


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


def _commit_author_key(raw: dict[str, Any]) -> str | None:
    author_login = (raw.get("author") or {}).get("login")
    if author_login:
        return f"user:{str(author_login).lower()}"
    commit = raw.get("commit") or {}
    author = commit.get("author") or {}
    email = author.get("email")
    if email:
        return f"email:{str(email).lower()}"
    name = author.get("name")
    if name:
        return f"name:{str(name).strip().lower()}"
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
    contributor_counts: dict[str, int] = {}
    truncated = False
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
        for commit in payload:
            if not isinstance(commit, dict):
                continue
            commit_date = _parse_commit_date(commit)
            if commit_date is not None and (latest is None or commit_date > latest):
                latest = commit_date
            author_key = _commit_author_key(commit)
            if author_key is None:
                continue
            contributor_counts[author_key] = contributor_counts.get(author_key, 0) + 1
        if len(payload) < PER_PAGE:
            break
        if page == MAX_PAGES:
            truncated = True
    top_share: float | None = None
    unique_contributors = len(contributor_counts)
    # The commit endpoint is intentionally capped; when capped, concentration
    # from a partial sample can inflate fragility, so treat it as unavailable.
    if truncated:
        unique_contributors = 0
    elif total > 0 and contributor_counts:
        top_share = round(max(contributor_counts.values()) / total, 4)
    return {
        "repo_url": url,
        "last_commit_date": latest,
        "commits_last_365d": total,
        "top_contributor_share_365d": top_share,
        "unique_contributors_last_365d": unique_contributors,
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
    exception_count = 0
    for url, res in zip(urls, results, strict=True):
        if isinstance(res, BaseException):
            exception_count += 1
            rows.append(_empty_commit_row(url))
            continue
        if res is None:
            rows.append(_empty_commit_row(url))
            continue
        rows.append(res)
    return rows, exception_count


def _rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for column in COMMIT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[COMMIT_COLUMNS]
    df["last_commit_date"] = pd.to_datetime(df["last_commit_date"], errors="coerce").dt.date
    df["commits_last_365d"] = (
        pd.to_numeric(df["commits_last_365d"], errors="coerce").fillna(0).astype("int64")
    )
    df["top_contributor_share_365d"] = pd.to_numeric(
        df["top_contributor_share_365d"], errors="coerce"
    ).astype("float64")
    df["unique_contributors_last_365d"] = (
        pd.to_numeric(df["unique_contributors_last_365d"], errors="coerce")
        .fillna(0)
        .astype("int64")
    )
    return df


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
    df = _rows_to_frame(rows)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
