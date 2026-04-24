"""@bruin

name: raw.github_contributors
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.github_contributors
  - raw.github_repos

description: |
  Per-repo all-time contributor concentration from GitHub
  `/contributors?anon=1`, plus distinct contributor count. This replaces
  `/stats/contributors` as the primary source because it is operationally
  stable for weekly batch ingestion.

tags:
  - layer:raw
  - source:github

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: top_contributor_share_all_time
    type: double
  - name: contributors_all_time
    type: bigint
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
GITHUB_REST = "https://api.github.com/repos"
PER_PAGE = 100
MAX_PAGES = 10

logger = logging.getLogger(__name__)
CONTRIBUTOR_COLUMNS = [
    "repo_url",
    "top_contributor_share_all_time",
    "contributors_all_time",
]


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_contributors.csv")
    if "top_contributor_share_all_time" not in df.columns:
        df = df.rename(columns={"top_contributor_share_365d": "top_contributor_share_all_time"})
    if "contributors_all_time" not in df.columns:
        df = df.rename(columns={"contributors_last_365d": "contributors_all_time"})
    df = _rows_to_frame(df.to_dict(orient="records"))  # type: ignore[arg-type]
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _empty_contributor_row(url: str) -> dict[str, Any]:
    return {
        "repo_url": url,
        "top_contributor_share_all_time": None,
        "contributors_all_time": 0,
    }


def _row_from_contributors(url: str, contributors: list[dict[str, Any]] | None) -> dict[str, Any]:
    if not contributors:
        return _empty_contributor_row(url)

    totals: list[int] = []
    for entry in contributors:
        if not isinstance(entry, dict):
            continue
        try:
            count = int(entry.get("contributions") or 0)
        except (TypeError, ValueError):
            count = 0
        if count > 0:
            totals.append(count)

    if not totals:
        return _empty_contributor_row(url)

    total = sum(totals)
    top = max(totals)
    return {
        "repo_url": url,
        "top_contributor_share_all_time": round(top / total, 4) if total else None,
        "contributors_all_time": len(totals),
    }


async def _fetch_repo(
    url: str,
    client: HttpClient,
) -> tuple[dict[str, Any], bool] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_

    all_rows: list[dict[str, Any]] = []
    truncated = False

    for page in range(1, MAX_PAGES + 1):
        payload = await client.get_json(
            f"{GITHUB_REST}/{owner}/{repo}/contributors",
            params={"per_page": PER_PAGE, "page": page, "anon": "1"},
            missing_statuses=(204, 404, 409, 410, 451),
        )
        if not isinstance(payload, list) or not payload:
            break
        all_rows.extend([entry for entry in payload if isinstance(entry, dict)])
        if len(payload) < PER_PAGE:
            break
        if page == MAX_PAGES:
            truncated = True

    # A truncated contributor list can overstate concentration because the
    # denominator is incomplete. Treat it as missing instead of partial.
    if truncated:
        return _empty_contributor_row(url), True
    return _row_from_contributors(url, all_rows), False


async def _ingest(window: str, urls: list[str]) -> tuple[list[dict[str, Any]], int, int]:
    async with HttpClient(window=window, concurrency=6) as client:
        results = await asyncio.gather(
            *[_fetch_repo(url, client) for url in urls],
            return_exceptions=True,
        )

    rows: list[dict[str, Any]] = []
    exception_count = 0
    truncated_count = 0
    for idx, res in enumerate(results):
        url = urls[idx]
        if isinstance(res, BaseException):
            exception_count += 1
            rows.append(_empty_contributor_row(url))
            continue
        if res is None:
            rows.append(_empty_contributor_row(url))
            continue
        row, truncated = res
        rows.append(row)
        if truncated:
            truncated_count += 1
    return rows, exception_count, truncated_count


def _rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for column in CONTRIBUTOR_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[CONTRIBUTOR_COLUMNS]
    df["top_contributor_share_all_time"] = pd.to_numeric(
        df["top_contributor_share_all_time"], errors="coerce"
    ).astype("float64")
    df["contributors_all_time"] = (
        pd.to_numeric(df["contributors_all_time"], errors="coerce").fillna(0).astype("int64")
    )
    return df


def _usable_contributor_signal_count(rows: list[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        share = row.get("top_contributor_share_all_time")
        contributors = row.get("contributors_all_time")
        if share is None:
            continue
        try:
            contributor_count = int(contributors or 0)
        except (TypeError, ValueError):
            contributor_count = 0
        if contributor_count > 0:
            count += 1
    return count


def _live() -> pd.DataFrame:
    window = live.resolve_window()
    with live.tracker("github_contributors") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count, truncated_count = asyncio.run(_ingest(window, urls))
        attempted = len(urls)
        usable_signals = _usable_contributor_signal_count(rows)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no github_contributors repo urls resolved")
        elif not rows:
            t.mark_failed("no github_contributors rows ingested")
        else:
            live.log_event(
                logger,
                logging.INFO,
                "github_contributors_signal_summary",
                attempted=attempted,
                rows=len(rows),
                usable_signals=usable_signals,
                truncated_repos=truncated_count,
                mode="contributors_all_time",
            )
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_contributors",
                attempted=attempted,
                succeeded=usable_signals,
                exception_count=exception_count,
                emitted_rows=len(rows),
            )
            if truncated_count > 0:
                t.mark_degraded(
                    f"github_contributors contributor pages truncated for {truncated_count}/{attempted} repos"
                )

    df = _rows_to_frame(rows)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
