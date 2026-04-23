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
  Per-repo contributor concentration: top-1 contributor share of commits
  over the last 365 days and distinct contributor count. Live mode hits
  `/stats/contributors` which returns HTTP 202 while GitHub backfills the
  stats cache; we poll briefly with a hard per-request timeout so one
  stuck repo cannot stall the live run.

tags:
  - layer:raw
  - source:github

columns:
  - name: repo_url
    type: varchar
    primary_key: true
  - name: top_contributor_share_365d
    type: double
  - name: contributors_last_365d
    type: bigint
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import _auth_headers

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
GITHUB_REST = "https://api.github.com/repos"
STATS_POLL_MAX = 4
STATS_POLL_WAIT_SECONDS = 3.0
STATS_REQUEST_TIMEOUT_SECONDS = 20.0
STATS_SECONDARY_LIMIT_DEFAULT_WAIT_SECONDS = 60.0
STATS_MAX_WAIT_SECONDS = 90.0
STATS_MAX_CONCURRENCY = 1
logger = logging.getLogger(__name__)
CONTRIBUTOR_COLUMNS = [
    "repo_url",
    "top_contributor_share_365d",
    "contributors_last_365d",
]


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "github_contributors.csv")
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    parts = url.removeprefix("https://github.com/").split("/")
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


async def _stats_contributors(
    owner: str, repo: str, client: httpx.AsyncClient
) -> list[dict[str, Any]] | None:
    url = f"{GITHUB_REST}/{owner}/{repo}/stats/contributors"
    for attempt in range(STATS_POLL_MAX):
        attempt_num = attempt + 1
        repo_ref = f"{owner}/{repo}"
        headers = _auth_headers(url)
        token_present = "Authorization" in headers
        try:
            resp = await asyncio.wait_for(
                client.get(url, headers=headers),
                timeout=STATS_REQUEST_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            live.log_event(
                logger,
                logging.WARNING,
                "github_contributors_stats_timeout",
                repo=repo_ref,
                attempt=attempt_num,
                max_attempts=STATS_POLL_MAX,
            )
            return None
        except httpx.HTTPError as exc:
            live.log_event(
                logger,
                logging.WARNING,
                "github_contributors_stats_request_failed",
                repo=repo_ref,
                attempt=attempt_num,
                max_attempts=STATS_POLL_MAX,
                error=exc,
            )
            return None
        if resp.status_code == 202:
            wait_seconds = min(STATS_POLL_WAIT_SECONDS * (2**attempt), STATS_MAX_WAIT_SECONDS)
            live.log_event(
                logger,
                logging.INFO,
                "github_contributors_stats_pending",
                repo=repo_ref,
                attempt=attempt_num,
                max_attempts=STATS_POLL_MAX,
                wait_seconds=wait_seconds,
                token_present=token_present,
            )
            await asyncio.sleep(wait_seconds)
            continue
        if resp.status_code in (204, 404, 410, 451):
            if resp.status_code == 204:
                live.log_event(
                    logger,
                    logging.INFO,
                    "github_contributors_stats_no_content",
                    repo=repo_ref,
                    attempt=attempt_num,
                    max_attempts=STATS_POLL_MAX,
                    token_present=token_present,
                )
            return None
        if resp.status_code == 200:
            payload = resp.json()
            if isinstance(payload, list):
                return payload
            return None
        rate_limit_remaining = resp.headers.get("x-ratelimit-remaining")
        rate_limit_reset = resp.headers.get("x-ratelimit-reset")
        rate_limit_resource = resp.headers.get("x-ratelimit-resource")
        retry_after = _parse_retry_after(resp)
        message = _response_message(resp)
        category = _classify_status(resp.status_code, message, rate_limit_remaining)
        wait_seconds = _compute_wait_seconds(
            attempt_num=attempt_num,
            category=category,
            retry_after=retry_after,
            rate_limit_reset=rate_limit_reset,
        )
        retryable = category in {
            "primary_rate_limit",
            "secondary_rate_limit",
            "rate_limited",
            "transient_server_error",
        }
        will_retry = retryable and attempt_num < STATS_POLL_MAX
        if category in {
            "primary_rate_limit",
            "secondary_rate_limit",
            "rate_limited",
            "transient_server_error",
        }:
            live.log_event(
                logger,
                logging.WARNING,
                "github_contributors_stats_rate_limited",
                repo=repo_ref,
                attempt=attempt_num,
                max_attempts=STATS_POLL_MAX,
                status_code=resp.status_code,
                category=category,
                wait_seconds=wait_seconds,
                will_retry=will_retry,
                retry_after=retry_after,
                rate_limit_remaining=rate_limit_remaining,
                rate_limit_reset=rate_limit_reset,
                rate_limit_resource=rate_limit_resource,
                token_present=token_present,
                message=message,
            )
            if will_retry:
                await asyncio.sleep(wait_seconds)
                continue
            return None
        live.log_event(
            logger,
            logging.WARNING,
            "github_contributors_stats_unexpected_status",
            repo=repo_ref,
            attempt=attempt_num,
            max_attempts=STATS_POLL_MAX,
            status_code=resp.status_code,
            category=category,
            retry_after=retry_after,
            rate_limit_remaining=rate_limit_remaining,
            rate_limit_reset=rate_limit_reset,
            rate_limit_resource=rate_limit_resource,
            token_present=token_present,
            message=message,
        )
        return None
    return None


def _parse_retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _response_message(resp: httpx.Response) -> str:
    try:
        payload = resp.json()
    except (ValueError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return _clip_message(message)
    text = (resp.text or "").strip()
    if text:
        return _clip_message(text)
    return "<empty>"


def _clip_message(message: str, *, max_len: int = 220) -> str:
    text = " ".join(message.split())
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 3]}..."


def _classify_status(status_code: int, message: str, rate_limit_remaining: str | None) -> str:
    message_lower = message.lower()
    if status_code == 429:
        return "rate_limited"
    if status_code == 403:
        if "secondary rate limit" in message_lower or "abuse detection" in message_lower:
            return "secondary_rate_limit"
        if rate_limit_remaining == "0" or "api rate limit exceeded" in message_lower:
            return "primary_rate_limit"
        if "resource not accessible by integration" in message_lower:
            return "integration_forbidden"
        if "history or contributor list is too large" in message_lower:
            return "repo_too_large"
        return "forbidden"
    if status_code in (500, 502, 503, 504):
        return "transient_server_error"
    return "unexpected_status"


def _compute_wait_seconds(
    *,
    attempt_num: int,
    category: str,
    retry_after: float | None,
    rate_limit_reset: str | None,
) -> float:
    backoff = min(STATS_POLL_WAIT_SECONDS * (2 ** (attempt_num - 1)), STATS_MAX_WAIT_SECONDS)
    if retry_after is not None:
        return min(max(retry_after, 0.0), STATS_MAX_WAIT_SECONDS)
    if category == "primary_rate_limit" and rate_limit_reset:
        try:
            seconds_until_reset = float(rate_limit_reset) - time.time()
        except ValueError:
            seconds_until_reset = 0.0
        if seconds_until_reset > 0:
            return min(seconds_until_reset, STATS_MAX_WAIT_SECONDS)
    if category == "secondary_rate_limit":
        return min(STATS_SECONDARY_LIMIT_DEFAULT_WAIT_SECONDS, STATS_MAX_WAIT_SECONDS)
    return backoff


async def _fetch_repo(url: str, client: httpx.AsyncClient) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    stats = await _stats_contributors(owner, repo, client)
    if not stats:
        return {
            "repo_url": url,
            "top_contributor_share_365d": None,
            "contributors_last_365d": 0,
        }
    cutoff_ts = int((datetime.now(UTC) - timedelta(days=365)).timestamp())
    totals: dict[str, int] = {}
    for entry in stats:
        if not isinstance(entry, dict):
            continue
        author = (entry.get("author") or {}).get("login")
        if not author:
            continue
        weekly = entry.get("weeks") or []
        commit_count = sum(
            int(w.get("c") or 0)
            for w in weekly
            if isinstance(w, dict) and int(w.get("w") or 0) >= cutoff_ts
        )
        if commit_count > 0:
            totals[author] = totals.get(author, 0) + commit_count
    if not totals:
        return {
            "repo_url": url,
            "top_contributor_share_365d": None,
            "contributors_last_365d": 0,
        }
    total = sum(totals.values())
    top = max(totals.values())
    return {
        "repo_url": url,
        "top_contributor_share_365d": round(top / total, 4) if total else None,
        "contributors_last_365d": len(totals),
    }


async def _ingest(urls: list[str]) -> tuple[list[dict[str, Any]], int]:
    # The /stats endpoint is polling-heavy, so we skip the HttpClient disk
    # cache for this asset and talk straight to httpx with bounded concurrency.
    sem = asyncio.Semaphore(STATS_MAX_CONCURRENCY)

    async def _worker(url: str, client: httpx.AsyncClient) -> dict[str, Any] | None:
        async with sem:
            return await _fetch_repo(url, client)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0), follow_redirects=True
    ) as client:
        results = await asyncio.gather(*[_worker(u, client) for u in urls], return_exceptions=True)
    rows: list[dict[str, Any]] = []
    exception_count = sum(1 for res in results if isinstance(res, BaseException))
    for res in results:
        if isinstance(res, BaseException) or res is None:
            continue
        rows.append(res)
    return rows, exception_count


def _rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for column in CONTRIBUTOR_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[CONTRIBUTOR_COLUMNS]
    df["top_contributor_share_365d"] = pd.to_numeric(
        df["top_contributor_share_365d"], errors="coerce"
    ).astype("float64")
    df["contributors_last_365d"] = (
        pd.to_numeric(df["contributors_last_365d"], errors="coerce").fillna(0).astype("int64")
    )
    return df


def _live() -> pd.DataFrame:
    with live.tracker("github_contributors") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count = asyncio.run(_ingest(urls))
        attempted = len(urls)
        t.row_count = len(rows)
        if attempted == 0:
            t.mark_failed("no github_contributors repo urls resolved")
        elif not rows:
            t.mark_failed("no github_contributors rows ingested")
        else:
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_contributors",
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
