"""Tests for pipeline.lib.sources: buffer IO, tracker, dedupe."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from pipeline.lib import sources as sources_lib
from pipeline.lib.sources import (
    SourceHealthRow,
    SourceHealthTracker,
    dedupe_latest_per_source,
    new_row,
    read_buffer,
    record,
)


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


def test_new_row_defaults_to_ok() -> None:
    row = new_row("npm_registry", window="2026-04-20")
    assert row.source_name == "npm_registry"
    assert row.status == "ok"
    assert row.window == "2026-04-20"


def test_source_health_row_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="status"):
        SourceHealthRow(source_name="x", status="whatever")


def test_record_and_read_roundtrip(tmp_path: Path) -> None:
    row = SourceHealthRow(
        source_name="osv",
        status="ok",
        last_success_at="2026-04-20T00:00:00+00:00",
        stale=False,
        failure_count=0,
        note="",
        latency_ms=123.4,
        row_count=250,
        window="2026-04-20",
    )
    record(row, root=tmp_path)
    got = read_buffer("2026-04-20", root=tmp_path)
    assert len(got) == 1
    assert got[0].source_name == "osv"
    assert got[0].row_count == 250


def test_read_buffer_missing_returns_empty(tmp_path: Path) -> None:
    assert read_buffer("nope", root=tmp_path) == []


def test_dedupe_latest_keeps_last_entry_per_source() -> None:
    rows = [
        SourceHealthRow(source_name="npm_registry", status="failed", failure_count=1),
        SourceHealthRow(source_name="pypi_registry", status="ok"),
        SourceHealthRow(source_name="npm_registry", status="ok"),
    ]
    unique = dedupe_latest_per_source(rows)
    by_name = {r.source_name: r for r in unique}
    assert by_name["npm_registry"].status == "ok"
    assert by_name["pypi_registry"].status == "ok"


def test_tracker_records_ok_on_clean_exit(tmp_path: Path) -> None:
    with SourceHealthTracker("npm_registry", window="w", root=tmp_path) as t:
        t.row_count = 42
    rows = read_buffer("w", root=tmp_path)
    assert len(rows) == 1
    assert rows[0].status == "ok"
    assert rows[0].row_count == 42
    assert rows[0].last_success_at is not None
    assert rows[0].stale is False


def test_tracker_emits_bruin_visible_structured_logs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    monkeypatch.setenv("BRUIN_ASSET", "raw.npm_registry")
    monkeypatch.setenv("BRUIN_RUN_ID", "run-123")
    with SourceHealthTracker("npm_registry", window="w", root=tmp_path) as t:
        t.row_count = 42

    output = capsys.readouterr().out
    assert "asset=raw.npm_registry" in output
    assert "run_id=run-123" in output
    assert "event=source_start source=npm_registry window=w" in output
    assert "event=source_finish source=npm_registry window=w status=ok rows=42" in output


def test_log_event_quotes_values_with_spaces(
    capsys: pytest.CaptureFixture[str],
    reset_bruin_logging: None,
) -> None:
    del reset_bruin_logging
    sources_lib.log_event(
        logging.getLogger("pipeline.tests"),
        logging.INFO,
        "sample",
        note="contains spaces",
        stale=False,
    )
    output = capsys.readouterr().out
    assert 'event=sample note="contains spaces" stale=false' in output


def test_tracker_records_failed_on_exception(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError), SourceHealthTracker("osv", window="w", root=tmp_path):
        raise RuntimeError("boom")
    rows = read_buffer("w", root=tmp_path)
    assert len(rows) == 1
    assert rows[0].status == "failed"
    assert rows[0].stale is True
    assert "boom" in rows[0].note


def test_tracker_mark_degraded(tmp_path: Path) -> None:
    with SourceHealthTracker("github_commits", window="w", root=tmp_path) as t:
        t.mark_degraded("rate-limited on repo baz/qux")
    rows = read_buffer("w", root=tmp_path)
    assert rows[0].status == "degraded"
    assert "rate-limited" in rows[0].note


def test_record_swallows_oserror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_oserror(self: Path, *a: object, **kw: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(Path, "mkdir", raise_oserror)
    # Should not raise even though mkdir fails.
    row = SourceHealthRow(source_name="test", status="ok", window="w")
    record(row, root=tmp_path)


def test_buffer_path_safe_when_window_empty(tmp_path: Path) -> None:
    path = sources_lib._buffer_path("", tmp_path)
    assert path.name == "default.jsonl"
