"""Tests for pipeline.lib.http: cache round-trip, TTL, and Retry-After."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from pipeline.lib import http as http_lib
from pipeline.lib.http import CacheKey, HttpClient, _read_cache, _write_cache


@pytest.fixture
def tmp_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(http_lib, "CACHE_ROOT", tmp_path)
    monkeypatch.delenv("INGEST_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_INGEST_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    return tmp_path


def test_cache_roundtrip_reads_back_identical_payload(tmp_cache: Path) -> None:
    key = CacheKey(
        method="GET",
        url="https://example.com/api?x=1",
        body_hash="",
        auth_scope="anon",
        window="2026-04-20",
    )
    payload = {"hello": "world", "values": [1, 2, 3]}
    _write_cache(key, payload, tmp_cache)
    assert _read_cache(key, ttl_hours=168, root=tmp_cache) == payload


def test_cache_miss_when_entry_absent(tmp_cache: Path) -> None:
    key = CacheKey(
        method="GET",
        url="https://example.com/missing",
        body_hash="",
        auth_scope="anon",
        window="",
    )
    assert _read_cache(key, ttl_hours=168, root=tmp_cache) is None


def test_cache_ttl_expires(tmp_cache: Path) -> None:
    key = CacheKey(
        method="GET",
        url="https://example.com/x",
        body_hash="",
        auth_scope="anon",
        window="",
    )
    _write_cache(key, {"ok": True}, tmp_cache)
    path = http_lib._cache_path(key, tmp_cache)
    stale = time.time() - 200 * 3600
    import os as _os

    _os.utime(path, (stale, stale))
    assert _read_cache(key, ttl_hours=168, root=tmp_cache) is None


def test_cache_ttl_zero_never_expires(tmp_cache: Path) -> None:
    key = CacheKey(
        method="GET",
        url="https://example.com/x",
        body_hash="",
        auth_scope="anon",
        window="",
    )
    _write_cache(key, {"ok": True}, tmp_cache)
    assert _read_cache(key, ttl_hours=0, root=tmp_cache) == {"ok": True}


def test_auth_scope_distinguishes_anon_vs_token(
    tmp_cache: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://api.github.com/repos/foo/bar"
    key_anon = CacheKey(
        method="GET",
        url=url,
        body_hash="",
        auth_scope="anon",
        window="w",
    )
    _write_cache(key_anon, {"source": "anon"}, tmp_cache)

    monkeypatch.setenv("GITHUB_INGEST_TOKEN", "ghp_test")
    assert http_lib._auth_scope() == "gh-auth"
    key_auth = CacheKey(
        method="GET",
        url=url,
        body_hash="",
        auth_scope="gh-auth",
        window="w",
    )
    assert _read_cache(key_auth, ttl_hours=168, root=tmp_cache) is None
    _write_cache(key_auth, {"source": "auth"}, tmp_cache)
    assert _read_cache(key_anon, ttl_hours=168, root=tmp_cache) == {"source": "anon"}
    assert _read_cache(key_auth, ttl_hours=168, root=tmp_cache) == {"source": "auth"}


def test_github_token_injected_into_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_TOKEN", "ghp_test")
    headers = http_lib._auth_headers("https://api.github.com/repos/foo/bar")
    assert headers["Authorization"] == "Bearer ghp_test"
    assert "X-GitHub-Api-Version" in headers


def test_legacy_github_ingest_token_still_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_INGEST_TOKEN", "ghp_legacy")
    headers = http_lib._auth_headers("https://api.github.com/repos/foo/bar")
    assert headers["Authorization"] == "Bearer ghp_legacy"


def test_non_github_url_does_not_inject_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_INGEST_TOKEN", "ghp_test")
    headers = http_lib._auth_headers("https://registry.npmjs.org/lodash")
    assert "Authorization" not in headers


def _mock_transport(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


def test_client_caches_live_response(tmp_cache: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={"ok": True})

    async def run() -> None:
        async with HttpClient(window="2026-04-20", cache_root=tmp_cache) as client:
            client._client = httpx.AsyncClient(transport=_mock_transport(handler))
            got1 = await client.get_json("https://example.com/api")
            got2 = await client.get_json("https://example.com/api")
        assert got1 == {"ok": True}
        assert got2 == {"ok": True}

    asyncio.run(run())
    assert len(calls) == 1  # second call served from disk cache


def test_client_returns_none_on_missing_status(tmp_cache: Path) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async def run() -> None:
        async with HttpClient(window="w", cache_root=tmp_cache) as client:
            client._client = httpx.AsyncClient(transport=_mock_transport(handler))
            got = await client.get_json("https://example.com/missing", missing_statuses=(404,))
            assert got is None

    asyncio.run(run())


def test_client_retries_429_then_succeeds(tmp_cache: Path) -> None:
    attempts = {"n": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"err": "slow"})
        return httpx.Response(200, json={"ok": True})

    async def run() -> None:
        async with HttpClient(window="w", cache_root=tmp_cache) as client:
            client._client = httpx.AsyncClient(transport=_mock_transport(handler))
            got = await client.get_json("https://example.com/flaky")
            assert got == {"ok": True}

    asyncio.run(run())
    assert attempts["n"] == 3


def test_body_hash_differentiates_post_payloads() -> None:
    a = http_lib._body_hash({"packages": ["a"]})
    b = http_lib._body_hash({"packages": ["b"]})
    assert a != b
    assert http_lib._body_hash(None) == ""


def test_cache_payload_encoding_is_readable(tmp_cache: Path) -> None:
    key = CacheKey(
        method="GET",
        url="https://example.com",
        body_hash="",
        auth_scope="anon",
        window="",
    )
    _write_cache(key, {"x": 1}, tmp_cache)
    path = http_lib._cache_path(key, tmp_cache)
    envelope = json.loads(path.read_text())
    assert envelope["body"] == {"x": 1}
    assert envelope["url"] == "https://example.com"
