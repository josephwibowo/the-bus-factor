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
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from pipeline.lib import live
from pipeline.lib.http import _auth_headers

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
GITHUB_REST = "https://api.github.com/repos"
STATS_POLL_MAX = 3
STATS_POLL_WAIT_SECONDS = 60.0
STATS_REQUEST_TIMEOUT_SECONDS = 20.0
STATS_SECONDARY_LIMIT_DEFAULT_WAIT_SECONDS = 60.0
STATS_MAX_WAIT_SECONDS = 120.0
STATS_MAX_CONCURRENCY = 2
STATS_TRANSIENT_ERROR_BASE_WAIT_SECONDS = 2.0
STATS_TRANSIENT_ERROR_MAX_WAIT_SECONDS = 15.0
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


@dataclass(frozen=True)
class StatsFetchResult:
    status: str
    stats: list[dict[str, Any]] | None = None
    wait_seconds: float | None = None


async def _request_stats_contributors(
    owner: str,
    repo: str,
    client: httpx.AsyncClient,
    *,
    attempt_num: int,
    max_attempts: int,
) -> StatsFetchResult:
    url = f"{GITHUB_REST}/{owner}/{repo}/stats/contributors"
    repo_ref = f"{owner}/{repo}"
    headers = _auth_headers(url)
    token_present = "Authorization" in headers
    try:
        resp = await asyncio.wait_for(
            client.get(url, headers=headers),
            timeout=STATS_REQUEST_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        wait_seconds = _transient_error_wait_seconds(attempt_num)
        will_retry = attempt_num < max_attempts
        live.log_event(
            logger,
            logging.WARNING,
            "github_contributors_stats_timeout",
            repo=repo_ref,
            attempt=attempt_num,
            max_attempts=max_attempts,
            wait_seconds=wait_seconds,
            will_retry=will_retry,
            token_present=token_present,
        )
        return StatsFetchResult(
            "retryable_error" if will_retry else "error",
            wait_seconds=wait_seconds,
        )
    except httpx.HTTPError as exc:
        wait_seconds = _transient_error_wait_seconds(attempt_num)
        will_retry = attempt_num < max_attempts
        live.log_event(
            logger,
            logging.WARNING,
            "github_contributors_stats_request_failed",
            repo=repo_ref,
            attempt=attempt_num,
            max_attempts=max_attempts,
            error=exc,
            wait_seconds=wait_seconds,
            will_retry=will_retry,
            token_present=token_present,
        )
        return StatsFetchResult(
            "retryable_error" if will_retry else "error",
            wait_seconds=wait_seconds,
        )
    if resp.status_code == 202:
        wait_seconds = _stats_wait_seconds(attempt_num)
        will_retry = attempt_num < max_attempts
        live.log_event(
            logger,
            logging.INFO,
            "github_contributors_stats_pending",
            repo=repo_ref,
            attempt=attempt_num,
            max_attempts=max_attempts,
            wait_seconds=wait_seconds,
            will_retry=will_retry,
            token_present=token_present,
        )
        return StatsFetchResult("pending", wait_seconds=wait_seconds)
    if resp.status_code in (204, 404, 410, 451):
        if resp.status_code == 204:
            live.log_event(
                logger,
                logging.INFO,
                "github_contributors_stats_no_content",
                repo=repo_ref,
                attempt=attempt_num,
                max_attempts=max_attempts,
                token_present=token_present,
            )
        return StatsFetchResult("missing")
    if resp.status_code == 200:
        payload = resp.json()
        if isinstance(payload, list):
            return StatsFetchResult("ok", payload)
        return StatsFetchResult("missing")
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
    will_retry = retryable and attempt_num < max_attempts
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
            max_attempts=max_attempts,
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
        return StatsFetchResult(
            "retryable_error" if will_retry else "error",
            wait_seconds=wait_seconds,
        )
    live.log_event(
        logger,
        logging.WARNING,
        "github_contributors_stats_unexpected_status",
        repo=repo_ref,
        attempt=attempt_num,
        max_attempts=max_attempts,
        status_code=resp.status_code,
        category=category,
        retry_after=retry_after,
        rate_limit_remaining=rate_limit_remaining,
        rate_limit_reset=rate_limit_reset,
        rate_limit_resource=rate_limit_resource,
        token_present=token_present,
        message=message,
    )
    return StatsFetchResult("error")


async def _stats_contributors(
    owner: str, repo: str, client: httpx.AsyncClient
) -> list[dict[str, Any]] | None:
    for attempt_num in range(1, STATS_POLL_MAX + 1):
        result = await _request_stats_contributors(
            owner,
            repo,
            client,
            attempt_num=attempt_num,
            max_attempts=STATS_POLL_MAX,
        )
        if result.status == "ok":
            return result.stats
        if result.status == "pending":
            if attempt_num >= STATS_POLL_MAX:
                return None
            await asyncio.sleep(_stats_wait_seconds(attempt_num))
            continue
        if result.status == "retryable_error":
            if attempt_num >= STATS_POLL_MAX:
                return None
            retry_wait = result.wait_seconds
            if retry_wait is None:
                retry_wait = STATS_SECONDARY_LIMIT_DEFAULT_WAIT_SECONDS
            await asyncio.sleep(retry_wait)
            continue
        return None
    return None


def _stats_wait_seconds(attempt_num: int) -> float:
    return min(STATS_POLL_WAIT_SECONDS * (2 ** max(0, attempt_num - 1)), STATS_MAX_WAIT_SECONDS)


def _transient_error_wait_seconds(attempt_num: int) -> float:
    return min(
        STATS_TRANSIENT_ERROR_BASE_WAIT_SECONDS * (2 ** max(0, attempt_num - 1)),
        STATS_TRANSIENT_ERROR_MAX_WAIT_SECONDS,
    )


def _empty_contributor_row(url: str) -> dict[str, Any]:
    return {
        "repo_url": url,
        "top_contributor_share_365d": None,
        "contributors_last_365d": 0,
    }


def _row_from_stats(
    url: str, stats: list[dict[str, Any]] | None, snapshot_week: date
) -> dict[str, Any]:
    if not stats:
        return _empty_contributor_row(url)
    cutoff_ts = int(
        datetime.combine(snapshot_week - timedelta(days=365), datetime.min.time(), UTC).timestamp()
    )
    until_ts = int(datetime.combine(snapshot_week, datetime.min.time(), UTC).timestamp())
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
            if isinstance(w, dict) and cutoff_ts <= int(w.get("w") or 0) < until_ts
        )
        if commit_count > 0:
            totals[author] = totals.get(author, 0) + commit_count
    if not totals:
        return _empty_contributor_row(url)
    total = sum(totals.values())
    top = max(totals.values())
    return {
        "repo_url": url,
        "top_contributor_share_365d": round(top / total, 4) if total else None,
        "contributors_last_365d": len(totals),
    }


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
    backoff: float = min(STATS_POLL_WAIT_SECONDS * (2 ** (attempt_num - 1)), STATS_MAX_WAIT_SECONDS)
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


async def _fetch_repo(
    url: str, client: httpx.AsyncClient, snapshot_week: date
) -> dict[str, Any] | None:
    or_ = _parse_owner_repo(url)
    if or_ is None:
        return None
    owner, repo = or_
    stats = await _stats_contributors(owner, repo, client)
    return _row_from_stats(url, stats, snapshot_week)


async def _ingest(urls: list[str], snapshot_week: date) -> tuple[list[dict[str, Any]], int]:
    # Trigger GitHub's stats jobs for the whole batch, then retry pending repos
    # after a global wait. Per-repo sleeps make cold stats caches unusably slow.
    sem = asyncio.Semaphore(STATS_MAX_CONCURRENCY)
    parsed = [
        (url, owner_repo[0], owner_repo[1])
        for url in urls
        if (owner_repo := _parse_owner_repo(url)) is not None
    ]
    rows_by_url: dict[str, dict[str, Any]] = {}
    pending = parsed
    exception_count = 0
    run_started = time.perf_counter()

    async def _worker(
        item: tuple[str, str, str], client: httpx.AsyncClient, attempt_num: int
    ) -> tuple[tuple[str, str, str], StatsFetchResult]:
        _, owner, repo = item
        async with sem:
            result = await _request_stats_contributors(
                owner,
                repo,
                client,
                attempt_num=attempt_num,
                max_attempts=STATS_POLL_MAX,
            )
        return item, result

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0), follow_redirects=True
    ) as client:
        for attempt_num in range(1, STATS_POLL_MAX + 1):
            if not pending:
                break
            attempt_started = time.perf_counter()
            results = await asyncio.gather(
                *[_worker(item, client, attempt_num) for item in pending],
                return_exceptions=True,
            )
            next_pending: list[tuple[str, str, str]] = []
            retry_wait_seconds = 0.0
            status_counts: dict[str, int] = {}
            round_exceptions = 0
            for res in results:
                if isinstance(res, BaseException):
                    exception_count += 1
                    round_exceptions += 1
                    continue
                item, fetch_result = res
                url, _, _ = item
                status_counts[fetch_result.status] = status_counts.get(fetch_result.status, 0) + 1
                if fetch_result.status == "ok":
                    rows_by_url[url] = _row_from_stats(url, fetch_result.stats, snapshot_week)
                elif fetch_result.status in {"pending", "retryable_error"}:
                    if attempt_num < STATS_POLL_MAX:
                        next_pending.append(item)
                        if fetch_result.wait_seconds is not None:
                            retry_wait_seconds = max(retry_wait_seconds, fetch_result.wait_seconds)
                    else:
                        rows_by_url[url] = _empty_contributor_row(url)
                else:
                    if fetch_result.status == "error":
                        exception_count += 1
                    rows_by_url[url] = _empty_contributor_row(url)
            live.log_event(
                logger,
                logging.INFO,
                "github_contributors_stats_batch",
                attempt=attempt_num,
                max_attempts=STATS_POLL_MAX,
                attempted=len(pending),
                pending=len(next_pending),
                resolved=len(rows_by_url),
                status_ok=status_counts.get("ok", 0),
                status_pending=status_counts.get("pending", 0),
                status_retryable_error=status_counts.get("retryable_error", 0),
                status_missing=status_counts.get("missing", 0),
                status_error=status_counts.get("error", 0),
                worker_exceptions=round_exceptions,
                attempt_elapsed_ms=round((time.perf_counter() - attempt_started) * 1000.0, 2),
                total_elapsed_ms=round((time.perf_counter() - run_started) * 1000.0, 2),
            )
            pending = next_pending
            if pending and attempt_num < STATS_POLL_MAX:
                immediate_retry = retry_wait_seconds <= 0.0
                live.log_event(
                    logger,
                    logging.INFO,
                    "github_contributors_stats_batch_wait",
                    attempt=attempt_num,
                    pending=len(pending),
                    wait_seconds=retry_wait_seconds,
                    immediate_retry=immediate_retry,
                )
                if not immediate_retry:
                    await asyncio.sleep(retry_wait_seconds)

    rows = [rows_by_url.get(url, _empty_contributor_row(url)) for url, _, _ in parsed]
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


def _usable_contributor_signal_count(rows: list[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        share = row.get("top_contributor_share_365d")
        contributors = row.get("contributors_last_365d")
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
    snapshot_week = live.resolve_window_date()
    with live.tracker("github_contributors") as t:
        urls = live.repo_urls_from_duckdb(live.duckdb_path())
        rows, exception_count = asyncio.run(_ingest(urls, snapshot_week))
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
            )
            live.mark_degraded_if_low_success(
                tracker=t,
                source_name="github_contributors",
                attempted=attempted,
                succeeded=usable_signals,
                exception_count=exception_count,
            )
    df = _rows_to_frame(rows)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
