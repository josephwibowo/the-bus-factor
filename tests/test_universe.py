"""Tests for pipeline.lib.universe: overlap gate, seed I/O, refresh path."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipeline.lib import http as http_lib
from pipeline.lib import universe as universe_lib
from pipeline.lib.http import HttpClient
from pipeline.lib.universe import (
    UniversePackage,
    apply_overlap_gate,
    compute_overlap,
)


def _pkgs(names: list[str]) -> list[UniversePackage]:
    return [UniversePackage(name=n) for n in names]


# --- Overlap gate --------------------------------------------------------


def test_compute_overlap_identical() -> None:
    assert compute_overlap(["a", "b", "c"], ["a", "b", "c"]) == 1.0


def test_compute_overlap_disjoint() -> None:
    assert compute_overlap(["a", "b"], ["c", "d"]) == 0.0


def test_compute_overlap_partial() -> None:
    assert compute_overlap(["a", "b", "c", "d"], ["a", "b", "e", "f"]) == 0.5


def test_overlap_gate_returns_fresh_when_no_previous() -> None:
    fresh = _pkgs(["a", "b", "c"])
    final, overlap = apply_overlap_gate(None, fresh)
    assert final == fresh
    assert overlap is None


def test_overlap_gate_returns_fresh_when_above_threshold() -> None:
    previous = _pkgs([f"pkg{i}" for i in range(100)])
    fresh_names = [f"pkg{i}" for i in range(100)]
    fresh_names[0] = "new-kid"
    fresh = _pkgs(fresh_names)
    final, overlap = apply_overlap_gate(previous, fresh, threshold=0.95)
    assert final == fresh
    assert overlap is not None and overlap >= 0.95


def test_overlap_gate_keeps_previous_when_below_threshold() -> None:
    previous = _pkgs([f"pkg{i}" for i in range(100)])
    fresh = _pkgs([f"new{i}" for i in range(100)])
    final, overlap = apply_overlap_gate(previous, fresh, threshold=0.95)
    assert final == previous
    assert overlap == 0.0


# --- Seed I/O ------------------------------------------------------------


def _seed_payload(packages: list[UniversePackage] | list[str]) -> dict[str, Any]:
    return {
        "ecosystem": "npm",
        "source": "test",
        "refreshed_at": "2026-04-22T00:00:00Z",
        "packages": packages,
    }


def test_seed_roundtrip(tmp_path: Path) -> None:
    pkgs = [
        UniversePackage(name="a", dependent_count=100),
        UniversePackage(name="b", dependent_count=50),
        UniversePackage(name="c"),
    ]
    universe_lib._write_seed("npm", _seed_payload(pkgs), tmp_path)
    got = universe_lib._read_seed("npm", tmp_path)
    assert got is not None
    assert got["packages"] == pkgs


def test_seed_accepts_legacy_string_list(tmp_path: Path) -> None:
    """Back-compat: seeds written as ``["a", "b"]`` still load."""

    path = universe_lib._seed_path("npm", tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"ecosystem": "npm", "packages": ["a", "b"]}))
    got = universe_lib._read_seed("npm", tmp_path)
    assert got is not None
    assert got["packages"] == [UniversePackage(name="a"), UniversePackage(name="b")]


def test_seed_missing_returns_none(tmp_path: Path) -> None:
    assert universe_lib._read_seed("pypi", tmp_path) is None


def test_seed_malformed_returns_none(tmp_path: Path) -> None:
    path = universe_lib._seed_path("npm", tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json")
    assert universe_lib._read_seed("npm", tmp_path) is None


def test_seed_serialises_dependent_count(tmp_path: Path) -> None:
    pkgs = [
        UniversePackage(name="react", dependent_count=12345),
        UniversePackage(name="orphan"),
    ]
    universe_lib._write_seed("npm", _seed_payload(pkgs), tmp_path)
    raw = json.loads((tmp_path / "npm.json").read_text())
    assert raw["packages"] == [
        {"name": "react", "dependent_count": 12345},
        {"name": "orphan"},
    ]


# --- top_packages (hot path) ---------------------------------------------


def test_top_packages_reads_committed_seed(tmp_path: Path) -> None:
    pkgs = _pkgs(["a", "b", "c", "d", "e"])
    universe_lib._write_seed("npm", _seed_payload(pkgs), tmp_path)
    got = universe_lib.top_packages("npm", limit=3, seed_dir=tmp_path)
    assert [p.name for p in got.packages] == ["a", "b", "c"]
    assert got.source == "test"
    assert got.refreshed_at == "2026-04-22T00:00:00Z"


def test_top_packages_preserves_dependent_count(tmp_path: Path) -> None:
    pkgs = [
        UniversePackage(name="react", dependent_count=12345),
        UniversePackage(name="lodash", dependent_count=67890),
    ]
    universe_lib._write_seed("npm", _seed_payload(pkgs), tmp_path)
    got = universe_lib.top_packages("npm", limit=2, seed_dir=tmp_path)
    assert got.packages[0].dependent_count == 12345
    assert got.packages[1].dependent_count == 67890


def test_top_packages_raises_when_seed_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="seed for 'npm' not found"):
        universe_lib.top_packages("npm", limit=5, seed_dir=tmp_path)


def test_top_packages_rejects_unknown_ecosystem(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown ecosystem"):
        universe_lib.top_packages("rubygems", limit=10, seed_dir=tmp_path)


def test_top_packages_rejects_non_positive_limit(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="limit must be positive"):
        universe_lib.top_packages("npm", limit=0, seed_dir=tmp_path)


# --- refresh path (cold) -------------------------------------------------


def test_fetch_pypi_top_parses_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(http_lib, "CACHE_ROOT", tmp_path / "http")

    def handler(request: httpx.Request) -> httpx.Response:
        assert "top-pypi-packages" in str(request.url)
        return httpx.Response(200, json={"rows": [{"project": "requests"}, {"project": "urllib3"}]})

    async def run() -> list[UniversePackage]:
        async with HttpClient(window="w", cache_root=tmp_path / "http") as client:
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            return await universe_lib._fetch_pypi_top(client, limit=5)

    got = asyncio.run(run())
    assert [p.name for p in got] == ["requests", "urllib3"]
    assert all(p.dependent_count is None for p in got)


def test_fetch_npm_top_from_deps_dev_queries_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_query(sql: str, *, job_description: str = "") -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["job_description"] = job_description
        return [
            {"package_name": "lodash", "dependent_count": 500000},
            {"package_name": "react", "dependent_count": 400000},
            {"package_name": "express", "dependent_count": 300000},
        ]

    def fake_snapshot() -> str:
        return "2026-04-14"

    import pipeline.lib.bq as bq_mod

    monkeypatch.setattr(bq_mod, "run_query", fake_run_query)
    monkeypatch.setattr(bq_mod, "latest_deps_dev_snapshot_date", fake_snapshot)

    got = universe_lib._fetch_npm_top_from_deps_dev(limit=5)
    assert [p.name for p in got] == ["lodash", "react", "express"]
    assert got[0].dependent_count == 500000
    sql = captured["sql"]
    # Must query the partition-pinned base table, not the Latest view.
    assert "deps_dev_v1.Dependents`" in sql
    assert "DependentsLatest" not in sql
    assert "DATE(SnapshotAt) = DATE '2026-04-14'" in sql
    assert "System = 'NPM'" in sql
    assert "MinimumDepth = 1" in sql
    assert "LIMIT 5" in sql
    # Must project dependent_count so it can be persisted in the seed.
    assert "dependent_count" in sql


def test_refresh_seed_writes_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_query(sql: str, *, job_description: str = "") -> list[dict[str, Any]]:
        return [{"package_name": f"pkg-{i}", "dependent_count": 1000 - i} for i in range(10)]

    def fake_snapshot() -> str:
        return "2026-04-14"

    import pipeline.lib.bq as bq_mod

    monkeypatch.setattr(bq_mod, "run_query", fake_run_query)
    monkeypatch.setattr(bq_mod, "latest_deps_dev_snapshot_date", fake_snapshot)

    result = universe_lib.refresh_seed("npm", limit=5, seed_dir=tmp_path)
    assert result.ecosystem == "npm"
    assert len(result.packages) == 5
    assert result.packages[0].name == "pkg-0"
    assert result.packages[0].dependent_count == 1000
    assert "deps.dev" in result.source
    written = json.loads((tmp_path / "npm.json").read_text())
    # Persisted form is a list of dicts with the count preserved.
    assert written["packages"][0] == {"name": "pkg-0", "dependent_count": 1000}
    assert written["source"] == result.source


def test_refresh_seed_pypi_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    http_cache = tmp_path / "http"
    monkeypatch.setattr(http_lib, "CACHE_ROOT", http_cache)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"rows": [{"project": f"pkg-{i}"} for i in range(10)]})

    original_enter = HttpClient.__aenter__

    async def patched_enter(self: HttpClient) -> HttpClient:
        client = await original_enter(self)
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return client

    monkeypatch.setattr(HttpClient, "__aenter__", patched_enter)

    result = universe_lib.refresh_seed("pypi", limit=5, seed_dir=tmp_path)
    assert result.ecosystem == "pypi"
    assert result.source == "hugovk/top-pypi-packages"
    assert result.packages[0].name == "pkg-0"
    assert result.packages[0].dependent_count is None
    written = json.loads((tmp_path / "pypi.json").read_text())
    # PyPI rows omit ``dependent_count`` entirely, not set it to null.
    assert written["packages"][0] == {"name": "pkg-0"}
