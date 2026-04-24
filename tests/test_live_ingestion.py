"""Tests for live-ingestion helpers and GitHub contributor polling guards."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from pipeline.assets.raw import raw_github_commits as commits
from pipeline.assets.raw import raw_github_contributors as contributors
from pipeline.assets.raw import raw_github_issues as issues
from pipeline.assets.raw import raw_npm_registry as npm_registry
from pipeline.assets.raw import raw_pypi_registry as pypi_registry
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


def test_github_contributors_row_from_contributors_computes_share() -> None:
    row = contributors._row_from_contributors(
        "https://github.com/owner/repo",
        [
            {"contributions": 70},
            {"contributions": 30},
        ],
    )
    assert row["repo_url"] == "https://github.com/owner/repo"
    assert row["top_contributor_share_all_time"] == pytest.approx(0.7)
    assert row["contributors_all_time"] == 2


def test_github_contributors_row_from_contributors_empty_returns_null_share() -> None:
    row = contributors._row_from_contributors("https://github.com/owner/repo", [])
    assert row["top_contributor_share_all_time"] is None
    assert row["contributors_all_time"] == 0


def test_github_contributors_fetch_repo_truncated_returns_empty_row() -> None:
    class Client:
        async def get_json(
            self,
            _url: str,
            **kwargs: object,
        ) -> list[dict[str, object]]:
            params = kwargs.get("params")
            assert isinstance(params, dict)
            page = int(params["page"])
            if page <= contributors.MAX_PAGES:
                return [{"contributions": 1}] * contributors.PER_PAGE
            return []

    got = asyncio.run(
        contributors._fetch_repo(
            "https://github.com/owner/repo",
            Client(),  # type: ignore[arg-type]
        )
    )
    assert got == (
        {
            "repo_url": "https://github.com/owner/repo",
            "top_contributor_share_all_time": None,
            "contributors_all_time": 0,
        },
        True,
    )


def test_github_contributors_ingest_keeps_rows_when_one_repo_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyHttpClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def __aenter__(self) -> DummyHttpClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    async def fake_fetch_repo(
        url: str,
        _client: DummyHttpClient,
    ) -> tuple[dict[str, object], bool]:
        if url.endswith("/boom"):
            raise RuntimeError("boom")
        if url.endswith("/truncated"):
            return contributors._empty_contributor_row(url), True
        return (
            {
                "repo_url": url,
                "top_contributor_share_all_time": 0.4,
                "contributors_all_time": 5,
            },
            False,
        )

    monkeypatch.setattr(contributors, "HttpClient", DummyHttpClient)
    monkeypatch.setattr(contributors, "_fetch_repo", fake_fetch_repo)

    urls = [
        "https://github.com/owner/ok",
        "https://github.com/owner/boom",
        "https://github.com/owner/truncated",
    ]
    rows, exception_count, truncated_count = asyncio.run(contributors._ingest("w", urls))

    assert exception_count == 1
    assert truncated_count == 1
    assert len(rows) == len(urls)
    assert {row["repo_url"] for row in rows} == set(urls)
    errored = next(row for row in rows if row["repo_url"].endswith("/boom"))
    assert errored["top_contributor_share_all_time"] is None
    assert errored["contributors_all_time"] == 0


def test_github_contributors_frame_preserves_all_null_share_column() -> None:
    df = contributors._rows_to_frame(
        [
            {
                "repo_url": "https://github.com/owner/repo",
                "top_contributor_share_all_time": None,
                "contributors_all_time": 0,
            }
        ]
    )

    assert list(df.columns) == [
        "repo_url",
        "top_contributor_share_all_time",
        "contributors_all_time",
    ]
    assert str(df["top_contributor_share_all_time"].dtype) == "float64"
    assert pd.isna(df.loc[0, "top_contributor_share_all_time"])


def test_github_contributors_usable_signal_count_excludes_null_placeholders() -> None:
    assert (
        contributors._usable_contributor_signal_count(
            [
                {
                    "repo_url": "https://github.com/owner/unknown",
                    "top_contributor_share_all_time": None,
                    "contributors_all_time": 0,
                },
                {
                    "repo_url": "https://github.com/owner/working",
                    "top_contributor_share_all_time": 0.42,
                    "contributors_all_time": 7,
                },
                {
                    "repo_url": "https://github.com/owner/no-commits",
                    "top_contributor_share_all_time": None,
                    "contributors_all_time": 0,
                },
            ]
        )
        == 1
    )


def test_github_issues_ingest_keeps_rows_when_repos_error_or_parse_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyHttpClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def __aenter__(self) -> DummyHttpClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    async def fake_fetch_repo(
        url: str,
        _client: DummyHttpClient,
        _snapshot_week: date,
    ) -> dict[str, object] | None:
        if url.endswith("/boom"):
            raise RuntimeError("boom")
        if url.endswith("/invalid"):
            return None
        return {
            "repo_url": url,
            "issues_opened_last_180d": 2,
            "median_time_to_first_response_days": 1.5,
        }

    monkeypatch.setattr(issues, "HttpClient", DummyHttpClient)
    monkeypatch.setattr(issues, "_fetch_repo", fake_fetch_repo)

    urls = [
        "https://github.com/owner/ok",
        "https://github.com/owner/boom",
        "https://github.com/owner/invalid",
    ]
    rows, exception_count = asyncio.run(issues._ingest("w", urls, date(2026, 4, 20)))

    assert exception_count == 1
    assert len(rows) == len(urls)
    assert {row["repo_url"] for row in rows} == set(urls)
    errored = next(row for row in rows if row["repo_url"].endswith("/boom"))
    assert errored["issues_opened_last_180d"] == 0
    assert errored["median_time_to_first_response_days"] is None
    invalid = next(row for row in rows if row["repo_url"].endswith("/invalid"))
    assert invalid["issues_opened_last_180d"] == 0
    assert invalid["median_time_to_first_response_days"] is None


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
        "top_contributor_share_365d": None,
        "unique_contributors_last_365d": 0,
    }
    params = calls[0]["params"]
    assert isinstance(params, dict)
    assert str(params["since"]).startswith("2025-04-20T00:00:00")
    assert str(params["until"]).startswith("2026-04-20T00:00:00")


def test_github_commits_truncated_pages_drop_concentration_signal() -> None:
    class Client:
        async def get_json(
            self,
            _url: str,
            **kwargs: object,
        ) -> list[dict[str, object]]:
            params = kwargs.get("params")
            assert isinstance(params, dict)
            page = int(params["page"])
            if page <= commits.MAX_PAGES:
                return [
                    {
                        "commit": {
                            "author": {
                                "date": "2026-04-18T00:00:00Z",
                                "email": "alice@example.com",
                            }
                        },
                        "author": {"login": "alice"},
                    }
                ] * commits.PER_PAGE
            return []

    got = asyncio.run(
        commits._fetch_commits(
            "https://github.com/owner/repo",
            Client(),  # type: ignore[arg-type]
            date(2026, 4, 20),
        )
    )
    assert got is not None
    assert got["commits_last_365d"] == commits.PER_PAGE * commits.MAX_PAGES
    assert got["top_contributor_share_365d"] is None
    assert got["unique_contributors_last_365d"] == 0


def test_github_commits_fixture_fallback_handles_new_contributor_fixture_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commits_fixture = pd.DataFrame(
        [
            {
                "repo_url": "https://github.com/owner/repo",
                "last_commit_date": "2026-04-01",
                "commits_last_365d": 10,
            }
        ]
    )
    contributors_fixture = pd.DataFrame(
        [
            {
                "repo_url": "https://github.com/owner/repo",
                "top_contributor_share_all_time": 0.95,
                "contributors_all_time": 2,
            }
        ]
    )

    def fake_read_csv(path: object, *args: object, **kwargs: object) -> pd.DataFrame:
        del args, kwargs
        path_str = str(path)
        if path_str.endswith("github_commits.csv"):
            return commits_fixture.copy()
        if path_str.endswith("github_contributors.csv"):
            return contributors_fixture.copy()
        raise AssertionError(f"unexpected fixture path: {path_str}")

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    got = commits._fixture()
    assert "top_contributor_share_365d" in got.columns
    assert "unique_contributors_last_365d" in got.columns
    assert pd.isna(got.loc[0, "top_contributor_share_365d"])
    assert got.loc[0, "unique_contributors_last_365d"] == 0


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


def test_npm_download_errors_do_not_count_as_registry_metadata_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyHttpClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def __aenter__(self) -> DummyHttpClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    async def fake_fetch_package(pkg: str, _client: DummyHttpClient) -> dict[str, object]:
        return {
            "package_name": pkg,
            "latest_version": "1.0.0",
            "first_release_date": date(2020, 1, 1),
            "latest_release_date": date(2026, 1, 1),
            "homepage_url": None,
            "repository_url": None,
            "is_deprecated": False,
            "is_archived": False,
            "publisher": None,
        }

    async def fake_fetch_downloads(
        _pkg: str,
        _client: DummyHttpClient,
        _snapshot_week: date,
    ) -> int:
        raise RuntimeError("downloads unavailable")

    monkeypatch.setattr(npm_registry, "HttpClient", DummyHttpClient)
    monkeypatch.setattr(npm_registry, "_fetch_package", fake_fetch_package)
    monkeypatch.setattr(npm_registry, "_fetch_downloads", fake_fetch_downloads)

    rows, meta_errors, download_errors = asyncio.run(
        npm_registry._ingest("w", ["left-pad", "chalk"], date(2026, 4, 20))
    )

    assert meta_errors == 0
    assert download_errors == 2
    assert len(rows) == 2
    assert {row["downloads_90d"] for row in rows} == {None}


def test_pypi_download_errors_do_not_count_as_registry_metadata_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyHttpClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def __aenter__(self) -> DummyHttpClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    async def fake_fetch_meta(pkg: str, _client: DummyHttpClient) -> dict[str, object]:
        return {
            "package_name": pkg,
            "latest_version": "1.0.0",
            "first_release_date": date(2020, 1, 1),
            "latest_release_date": date(2026, 1, 1),
            "homepage_url": None,
            "repository_url": None,
            "is_deprecated": False,
            "is_archived": False,
            "publisher": None,
        }

    async def fake_fetch_downloads(_pkg: str, _client: DummyHttpClient) -> int:
        raise RuntimeError("downloads unavailable")

    monkeypatch.setattr(pypi_registry, "HttpClient", DummyHttpClient)
    monkeypatch.setattr(pypi_registry, "_fetch_meta", fake_fetch_meta)
    monkeypatch.setattr(pypi_registry, "_fetch_downloads", fake_fetch_downloads)

    rows, meta_errors, download_errors = asyncio.run(
        pypi_registry._ingest("w", ["requests", "flask"])
    )

    assert meta_errors == 0
    assert download_errors == 2
    assert len(rows) == 2
    assert {row["downloads_90d"] for row in rows} == {None}


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
