"""Shared HTTP client for live raw assets.

Responsibilities:
  * Issue async GET/POST against public APIs with bounded concurrency.
  * Respect ``Retry-After`` on 429/503 responses (tenacity-backed retries).
  * Inject a bearer token for GitHub calls when ``GITHUB_INGEST_TOKEN`` is set.
  * Cache JSON responses on disk under ``.cache/http/`` keyed on
    (url, auth-scope, reporting-window) so re-runs within the same week
    don't re-hit live APIs.

The cache is intentionally coarse: one JSON blob per request, sharded by
the first two chars of the sha1 key. It's designed for correctness, not
throughput - cycle 2 volumes are ~2500 HTTP calls per weekly snapshot,
well under the scale where LRU or content-addressed stores would matter.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
DEFAULT_CONCURRENCY = 20
DEFAULT_TTL_HOURS = 168  # one reporting window
CACHE_ROOT = Path(os.environ.get("BUS_FACTOR_HTTP_CACHE", ".cache/http"))


class HttpError(RuntimeError):
    """Raised when a live HTTP call fails after all retries."""


@dataclass(frozen=True)
class CacheKey:
    """Composite cache key for a single request.

    ``auth_scope`` distinguishes anonymous vs authenticated GitHub calls so
    a cached anonymous 403 doesn't get served when a token is present.
    ``window`` is the ISO Monday of the reporting week so stale entries
    roll over automatically; pass an empty string to opt out.
    """

    method: str
    url: str
    body_hash: str
    auth_scope: str
    window: str

    def digest(self) -> str:
        raw = f"{self.method}|{self.url}|{self.body_hash}|{self.auth_scope}|{self.window}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_path(key: CacheKey, root: Path = CACHE_ROOT) -> Path:
    digest = key.digest()
    return root / digest[:2] / f"{digest}.json"


def _read_cache(key: CacheKey, ttl_hours: int, root: Path = CACHE_ROOT) -> Any | None:
    path = _cache_path(key, root)
    if not path.exists():
        return None
    if ttl_hours > 0:
        age_hours = (time.time() - path.stat().st_mtime) / 3600
        if age_hours > ttl_hours:
            return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            envelope = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    return envelope.get("body")


def _write_cache(key: CacheKey, body: Any, root: Path = CACHE_ROOT) -> None:
    path = _cache_path(key, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    envelope = {"url": key.url, "method": key.method, "body": body}
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(envelope, fh)
    tmp.replace(path)


def _auth_scope() -> str:
    token = os.environ.get("GITHUB_INGEST_TOKEN") or os.environ.get("GITHUB_TOKEN")
    return "gh-auth" if token else "anon"


def _auth_headers(url: str) -> dict[str, str]:
    headers: dict[str, str] = {"User-Agent": "the-bus-factor/0.1 (+https://github.com)"}
    if "api.github.com" in url or "https://github.com/" in url:
        token = os.environ.get("GITHUB_INGEST_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["X-GitHub-Api-Version"] = "2022-11-28"
    return headers


def _body_hash(body: Any | None) -> str:
    if body is None:
        return ""
    return hashlib.sha1(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


class HttpClient:
    """Async HTTP wrapper with caching, retries, and bounded concurrency.

    Intended to be used as a context manager inside ``materialize()``::

        async with HttpClient(window="2026-04-20") as client:
            data = await client.get_json("https://api.example.com/x")
    """

    def __init__(
        self,
        *,
        window: str = "",
        concurrency: int = DEFAULT_CONCURRENCY,
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
        cache_root: Path | None = None,
        ttl_hours: int = DEFAULT_TTL_HOURS,
    ) -> None:
        self._window = window
        self._semaphore = asyncio.Semaphore(concurrency)
        self._timeout = timeout
        self._cache_root = cache_root or CACHE_ROOT
        self._ttl_hours = ttl_hours
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> HttpClient:
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=True,
            http2=False,
            limits=httpx.Limits(max_connections=DEFAULT_CONCURRENCY * 2),
        )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
        missing_statuses: tuple[int, ...] = (404, 410),
    ) -> Any:
        """GET ``url`` and return parsed JSON, or ``None`` on ``missing_statuses``."""

        return await self._request_json(
            "GET",
            url,
            body=None,
            params=params,
            ok_statuses=ok_statuses,
            missing_statuses=missing_statuses,
        )

    async def post_json(
        self,
        url: str,
        *,
        json_body: dict[str, Any],
        ok_statuses: tuple[int, ...] = (200,),
        missing_statuses: tuple[int, ...] = (),
    ) -> Any:
        return await self._request_json(
            "POST",
            url,
            body=json_body,
            params=None,
            ok_statuses=ok_statuses,
            missing_statuses=missing_statuses,
        )

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: Any | None,
        params: dict[str, Any] | None,
        ok_statuses: tuple[int, ...],
        missing_statuses: tuple[int, ...],
    ) -> Any:
        if self._client is None:
            raise RuntimeError("HttpClient must be used as an async context manager")

        key = CacheKey(
            method=method,
            url=url + ("?" + _stable_params(params) if params else ""),
            body_hash=_body_hash(body),
            auth_scope=_auth_scope(),
            window=self._window,
        )
        cached = _read_cache(key, self._ttl_hours, self._cache_root)
        if cached is not None:
            return cached

        async with self._semaphore:
            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    wait=wait_exponential(multiplier=1, min=1, max=30),
                    retry=retry_if_exception_type((httpx.TransportError, _RetryableStatusError)),
                    reraise=True,
                ):
                    with attempt:
                        resp = await self._client.request(
                            method,
                            url,
                            params=params,
                            json=body,
                            headers=_auth_headers(url),
                        )
                        if resp.status_code in missing_statuses:
                            _write_cache(key, None, self._cache_root)
                            return None
                        if resp.status_code in ok_statuses:
                            try:
                                payload: Any = resp.json()
                            except (ValueError, json.JSONDecodeError):
                                payload = resp.text
                            _write_cache(key, payload, self._cache_root)
                            return payload
                        if resp.status_code in (429, 502, 503, 504):
                            sleep = _retry_after_seconds(resp) or 0.0
                            if sleep > 0:
                                await asyncio.sleep(min(sleep, 60.0))
                            raise _RetryableStatusError(resp.status_code, resp.text[:200])
                        raise HttpError(f"{method} {url} -> {resp.status_code}: {resp.text[:200]}")
            except RetryError as exc:  # pragma: no cover - tenacity reraise above
                raise HttpError(str(exc)) from exc
        return None


class _RetryableStatusError(Exception):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"retryable status {status}: {body}")
        self.status = status


_RetryableStatus = _RetryableStatusError  # backwards-compat alias


def _stable_params(params: dict[str, Any] | None) -> str:
    if not params:
        return ""
    return "&".join(f"{k}={params[k]}" for k in sorted(params))
