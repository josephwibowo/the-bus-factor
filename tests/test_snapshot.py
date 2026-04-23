"""Tests for snapshot helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pipeline.lib.snapshot import (
    days_between,
    most_recent_monday_utc,
    resolve_snapshot,
)


def test_most_recent_monday_on_a_wednesday() -> None:
    now = datetime(2026, 4, 22, 10, 0, tzinfo=UTC)  # Wednesday
    monday = most_recent_monday_utc(now=now)
    assert monday.isoformat() == "2026-04-20"
    assert monday.weekday() == 0


def test_most_recent_monday_on_a_monday_is_same_day() -> None:
    now = datetime(2026, 4, 20, 0, 1, tzinfo=UTC)
    monday = most_recent_monday_utc(now=now)
    assert monday.isoformat() == "2026-04-20"


def test_resolve_snapshot_default() -> None:
    now = datetime(2026, 4, 22, 10, 0, tzinfo=UTC)
    snapshot = resolve_snapshot(now=now)
    assert snapshot.iso == "2026-04-20"
    assert snapshot.week_label == "2026-W17"


def test_resolve_snapshot_override_must_be_monday() -> None:
    with pytest.raises(ValueError):
        resolve_snapshot(override="2026-04-21")  # Tuesday


def test_resolve_snapshot_override_iso_week() -> None:
    snapshot = resolve_snapshot(override="2026-01-05")
    assert snapshot.week_label == "2026-W02"


def test_days_between_is_signed() -> None:
    a = datetime(2026, 1, 10).date()
    b = datetime(2026, 1, 1).date()
    assert days_between(a, b) == 9
    assert days_between(b, a) == -9
