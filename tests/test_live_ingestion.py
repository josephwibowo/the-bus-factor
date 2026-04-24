"""Tests for live-ingestion helpers and GitHub contributor polling guards."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import duckdb
import httpx
import pandas as pd
import pytest

from pipeline.assets.raw import raw_github_commits as commits
from pipeline.assets.raw import raw_github_contributors as contributors
from pipeline.assets.raw import raw_github_issues as issues
from pipeline.assets.raw import raw_scorecard as scorecard
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
    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def handler(_: httpx.Request) -> httpx.Response:
        await real_sleep(0.05)
        return httpx.Response(200, json=[])

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def run() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), timeout=None
        ) as client:
            got = await contributors._stats_contributors("owner", "repo", client)
        assert got is None

    asyncio.run(run())
    assert sleeps == [2.0, 4.0]
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_timeout" in output
    assert "repo=owner/repo" in output
    assert "will_retry=true" in output


def test_stats_contributors_retries_pending_stats_after_wait(
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
    assert "will_retry=true" in output


def test_stats_contributors_does_not_sleep_after_final_pending_attempt(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(202)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            got = await contributors._stats_contributors("owner", "repo", client)
        assert got is None

    asyncio.run(run())
    assert sleeps == [60.0, 120.0]
    output = capsys.readouterr().out
    assert f"attempt={contributors.STATS_POLL_MAX}" in output
    assert "will_retry=false" in output


def test_github_contributors_ingest_uses_batch_warmup_wait(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    calls_by_repo: dict[str, int] = {}
    sleeps: list[float] = []
    week_ts = int(pd.Timestamp("2026-04-01T00:00:00Z").timestamp())

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        repo = request.url.path.split("/repos/", 1)[1].split("/stats/", 1)[0]
        calls_by_repo[repo] = calls_by_repo.get(repo, 0) + 1
        if calls_by_repo[repo] == 1:
            return httpx.Response(202)
        return httpx.Response(
            200,
            json=[
                {"author": {"login": "alice"}, "weeks": [{"w": week_ts, "c": 7}]},
                {"author": {"login": "bob"}, "weeks": [{"w": week_ts, "c": 3}]},
            ],
        )

    original_async_client = contributors.httpx.AsyncClient
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        contributors.httpx,
        "AsyncClient",
        lambda **_: original_async_client(transport=httpx.MockTransport(handler)),
    )

    rows, exception_count = asyncio.run(
        contributors._ingest(
            ["https://github.com/owner/one", "https://github.com/owner/two"],
            date(2026, 4, 20),
        )
    )

    assert exception_count == 0
    assert sleeps == [contributors.STATS_POLL_WAIT_SECONDS]
    assert calls_by_repo == {"owner/one": 2, "owner/two": 2}
    assert {row["repo_url"] for row in rows} == {
        "https://github.com/owner/one",
        "https://github.com/owner/two",
    }
    assert all(row["top_contributor_share_365d"] == 0.7 for row in rows)
    assert all(row["contributors_last_365d"] == 2 for row in rows)
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_batch" in output
    assert "status_pending=2" in output
    assert "attempt_elapsed_ms=" in output
    assert "total_elapsed_ms=" in output


def test_github_contributors_ingest_immediate_retry_when_wait_is_zero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    calls_by_repo: dict[str, int] = {}
    sleeps: list[float] = []
    week_ts = int(pd.Timestamp("2026-04-01T00:00:00Z").timestamp())

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        repo = request.url.path.split("/repos/", 1)[1].split("/stats/", 1)[0]
        calls_by_repo[repo] = calls_by_repo.get(repo, 0) + 1
        if calls_by_repo[repo] == 1:
            return httpx.Response(
                403,
                headers={"Retry-After": "0"},
                json={"message": "You have exceeded a secondary rate limit. Please wait."},
            )
        return httpx.Response(
            200,
            json=[{"author": {"login": "alice"}, "weeks": [{"w": week_ts, "c": 5}]}],
        )

    original_async_client = contributors.httpx.AsyncClient
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        contributors.httpx,
        "AsyncClient",
        lambda **_: original_async_client(transport=httpx.MockTransport(handler)),
    )

    rows, exception_count = asyncio.run(
        contributors._ingest(["https://github.com/owner/one"], date(2026, 4, 20))
    )

    assert exception_count == 0
    assert sleeps == []
    assert calls_by_repo == {"owner/one": 2}
    assert rows == [
        {
            "repo_url": "https://github.com/owner/one",
            "top_contributor_share_365d": 1.0,
            "contributors_last_365d": 1,
        }
    ]
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_batch_wait" in output
    assert "wait_seconds=0.0" in output
    assert "immediate_retry=true" in output


def test_stats_contributors_retries_secondary_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    response = httpx.Response(
        403,
        headers={"Retry-After": "0"},
        json={"message": "You have exceeded a secondary rate limit. Please wait."},
    )
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    def handler(_: httpx.Request) -> httpx.Response:
        return response

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def run() -> list[dict[str, object]] | None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await contributors._stats_contributors("owner", "repo", client)

    got = asyncio.run(run())
    assert got is None
    assert sleeps == [0.0, 0.0]
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_rate_limited" in output
    assert "category=secondary_rate_limit" in output
    assert "will_retry=true" in output


def test_stats_contributors_handles_204_no_content(
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            got = await contributors._stats_contributors("owner", "repo", client)
        assert got is None

    asyncio.run(run())
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_no_content" in output
    assert "repo=owner/repo" in output


def test_stats_contributors_logs_primary_rate_limit_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    monkeypatch.setattr(contributors, "STATS_POLL_MAX", 1)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "999"},
            json={"message": "API rate limit exceeded for 1.2.3.4"},
        )

    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            got = await contributors._stats_contributors("owner", "repo", client)
        assert got is None

    asyncio.run(run())
    output = capsys.readouterr().out
    assert "event=github_contributors_stats_rate_limited" in output
    assert "category=primary_rate_limit" in output
    assert "rate_limit_remaining=0" in output
    assert "status_code=403" in output


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


def test_github_contributors_usable_signal_count_excludes_null_placeholders() -> None:
    assert (
        contributors._usable_contributor_signal_count(
            [
                {
                    "repo_url": "https://github.com/owner/unknown",
                    "top_contributor_share_365d": None,
                    "contributors_last_365d": 0,
                },
                {
                    "repo_url": "https://github.com/owner/working",
                    "top_contributor_share_365d": 0.42,
                    "contributors_last_365d": 7,
                },
                {
                    "repo_url": "https://github.com/owner/no-commits",
                    "top_contributor_share_365d": None,
                    "contributors_last_365d": 0,
                },
            ]
        )
        == 1
    )


def test_github_commits_uses_snapshot_week_window() -> None:
    calls: list[dict[str, object]] = []

    class Client:
        async def get_json(self, _url: str, **kwargs: object) -> list[dict[str, object]]:
            calls.append(kwargs)
            return []

    got = asyncio.run(
        commits._fetch_commits(
            "https://github.com/owner/repo",
            Client(),  # type: ignore[arg-type]
            date(2026, 4, 20),
        )
    )

    assert got == {
        "repo_url": "https://github.com/owner/repo",
        "last_commit_date": None,
        "commits_last_365d": 0,
    }
    params = calls[0]["params"]
    assert isinstance(params, dict)
    assert str(params["since"]).startswith("2025-04-20T00:00:00")
    assert str(params["until"]).startswith("2026-04-20T00:00:00")


def test_scorecard_missing_response_counts_as_null_score_row() -> None:
    class Client:
        async def get_json(self, _url: str, **_kwargs: object) -> None:
            return None

    got = asyncio.run(
        scorecard._fetch_repo(
            "https://github.com/Owner/Repo",
            Client(),  # type: ignore[arg-type]
        )
    )

    assert got == {
        "repo_url": "https://github.com/owner/repo",
        "aggregate_score": None,
        "check_count": 0,
        "scorecard_date": None,
    }


def test_issue_response_counts_only_maintainer_like_non_bot_comments() -> None:
    payloads = {
        "https://api.github.com/repos/owner/repo/issues/1/comments": [
            [
                {
                    "user": {"login": "community"},
                    "author_association": "NONE",
                    "created_at": "2026-04-02T00:00:00Z",
                },
                {
                    "user": {"login": "ci[bot]", "type": "Bot"},
                    "author_association": "MEMBER",
                    "created_at": "2026-04-03T00:00:00Z",
                },
                {
                    "user": {"login": "maintainer"},
                    "author_association": "MEMBER",
                    "created_at": "2026-04-04T12:00:00Z",
                },
            ]
        ],
    }

    class Client:
        async def get_json(self, url: str, **_kwargs: object) -> object:
            return payloads[url].pop(0)

    issue = {
        "number": 1,
        "user": {"login": "reporter"},
        "created_at": "2026-04-01T00:00:00Z",
    }

    days = asyncio.run(
        issues._first_maintainer_response_days(
            "owner",
            "repo",
            issue,
            Client(),  # type: ignore[arg-type]
            date(2026, 4, 20),
        )
    )

    assert days == pytest.approx(3.5)


def test_issue_eligibility_excludes_maintainer_authored_and_bots() -> None:
    since = pd.Timestamp("2026-01-01T00:00:00Z").to_pydatetime()
    until = pd.Timestamp("2026-04-20T00:00:00Z").to_pydatetime()

    assert not issues._is_eligible_issue(
        {
            "created_at": "2026-04-01T00:00:00Z",
            "user": {"login": "maintainer"},
            "author_association": "OWNER",
        },
        since_dt=since,
        until_dt=until,
    )
    assert not issues._is_eligible_issue(
        {
            "created_at": "2026-04-01T00:00:00Z",
            "user": {"login": "bot[bot]", "type": "Bot"},
            "author_association": "NONE",
        },
        since_dt=since,
        until_dt=until,
    )
    assert issues._is_eligible_issue(
        {
            "created_at": "2026-04-01T00:00:00Z",
            "user": {"login": "reporter"},
            "author_association": "NONE",
        },
        since_dt=since,
        until_dt=until,
    )


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
