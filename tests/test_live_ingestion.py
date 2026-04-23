"""Tests for live-ingestion helpers and GitHub contributor polling guards."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from pathlib import Path

import duckdb
import httpx
import pandas as pd
import pytest

from pipeline.assets.raw import raw_github_contributors as contributors
from pipeline.lib import live
from pipeline.lib import sources as sources_lib


@pytest.fixture
def reset_bruin_logging() -> Iterator[None]:
    pipeline_logger = logging.getLogger("pipeline")
    try:
        _clear_bruin_log_handlers(pipeline_logger)
        yield
    finally:
        _clear_bruin_log_handlers(pipeline_logger)


def _clear_bruin_log_handlers(pipeline_logger: logging.Logger) -> None:
    for handler in list(pipeline_logger.handlers):
        if getattr(handler, sources_lib.BUS_FACTOR_HANDLER_ATTR, False):
            pipeline_logger.removeHandler(handler)
            handler.close()
    pipeline_logger.propagate = True


def test_repo_urls_from_duckdb_reads_all_canonical_registry_urls(tmp_path: Path) -> None:
    db_path = tmp_path / "live.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.npm_registry (repository_url VARCHAR)")
        conn.execute("CREATE TABLE raw.pypi_registry (repository_url VARCHAR)")
        conn.executemany(
            "INSERT INTO raw.npm_registry VALUES (?)",
            [
                ("git+https://github.com/Owner/Repo.git",),
                ("https://example.com/not-github",),
                ("https://github.com/Shared/Project",),
            ],
        )
        conn.executemany(
            "INSERT INTO raw.pypi_registry VALUES (?)",
            [
                ("https://github.com/shared/project/issues",),
                ("https://github.com/Other/Lib#readme",),
            ],
        )
    finally:
        conn.close()

    assert live.repo_urls_from_duckdb(db_path) == [
        "https://github.com/owner/repo",
        "https://github.com/shared/project",
        "https://github.com/other/lib",
    ]


def test_stats_contributors_times_out_stuck_request(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    monkeypatch.setattr(contributors, "STATS_REQUEST_TIMEOUT_SECONDS", 0.001)

    async def handler(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)
        return httpx.Response(200, json=[])

    async def run() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=None
        ) as client:
            got = await contributors._stats_contributors("owner", "repo", client)
        assert got is None

    asyncio.run(run())
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_timeout" in output
    assert "repo=owner/repo" in output


def test_stats_contributors_retries_pending_stats(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    responses = [
        httpx.Response(202),
        httpx.Response(200, json=[{"author": {"login": "alice"}, "weeks": []}]),
    ]
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    def handler(_: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def run() -> list[dict[str, object]] | None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await contributors._stats_contributors("owner", "repo", client)

    got = asyncio.run(run())
    assert got == [{"author": {"login": "alice"}, "weeks": []}]
    assert sleeps == [contributors.STATS_POLL_WAIT_SECONDS]
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_pending" in output
    assert "repo=owner/repo" in output


def test_github_contributors_frame_preserves_all_null_share_column() -> None:
    df = contributors._rows_to_frame(
        [
            {
                "repo_url": "https://github.com/owner/repo",
                "top_contributor_share_365d": None,
                "contributors_last_365d": 0,
            }
        ]
    )

    assert list(df.columns) == [
        "repo_url",
        "top_contributor_share_365d",
        "contributors_last_365d",
    ]
    assert str(df["top_contributor_share_365d"].dtype) == "float64"
    assert pd.isna(df.loc[0, "top_contributor_share_365d"])


def test_mark_degraded_if_low_success_marks_tracker_degraded(tmp_path: Path) -> None:
    with sources_lib.SourceHealthTracker("github_repos", window="w", root=tmp_path) as t:
        live.mark_degraded_if_low_success(
            tracker=t,
            source_name="github_repos",
            attempted=100,
            succeeded=80,
            exception_count=0,
        )

    rows = sources_lib.read_buffer("w", root=tmp_path)
    assert rows[0].status == "degraded"
    assert "success_ratio=0.800" in rows[0].note


def test_mark_degraded_if_low_success_keeps_ok_when_above_threshold(tmp_path: Path) -> None:
    with sources_lib.SourceHealthTracker("github_repos", window="w", root=tmp_path) as t:
        live.mark_degraded_if_low_success(
            tracker=t,
            source_name="github_repos",
            attempted=100,
            succeeded=95,
            exception_count=2,
        )

    rows = sources_lib.read_buffer("w", root=tmp_path)
    assert rows[0].status == "ok"
