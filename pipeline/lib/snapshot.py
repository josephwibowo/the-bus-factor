"""Reporting-window helpers.

Every snapshot uses a deterministic *Monday 00:00 UTC* anchor so the pipeline
produces identical output when re-run on the same inputs within the same ISO
week. The public artefacts identify snapshots by ISO-week label (``YYYY-Www``)
and the ISO date (``YYYY-MM-DD``) of that Monday.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta


@dataclass(frozen=True)
class Snapshot:
    week_label: str  # e.g. "2026-W17"
    monday: date  # e.g. date(2026, 4, 20)

    @property
    def iso(self) -> str:
        return self.monday.isoformat()


def _last_monday(anchor: date) -> date:
    # Monday is weekday 0.
    return anchor - timedelta(days=anchor.weekday())


def most_recent_monday_utc(now: datetime | None = None) -> date:
    """Return the ISO date of the most recent Monday 00:00 UTC (inclusive)."""

    current = now if now is not None else datetime.now(UTC)
    current = current.replace(tzinfo=UTC) if current.tzinfo is None else current.astimezone(UTC)
    return _last_monday(current.date())


def resolve_snapshot(
    override: str | None = None,
    now: datetime | None = None,
) -> Snapshot:
    """Return a :class:`Snapshot` for ``override`` (YYYY-MM-DD) or today.

    ``override`` must be a Monday; otherwise :class:`ValueError` is raised so
    that misconfigurations surface early in CI rather than silently snapping
    to a different week.
    """

    if override:
        parsed = date.fromisoformat(override)
        if parsed.weekday() != 0:
            raise ValueError(
                f"snapshot_week {override!r} is not a Monday (weekday={parsed.weekday()})"
            )
        monday = parsed
    else:
        monday = most_recent_monday_utc(now=now)
    iso_year, iso_week, _ = monday.isocalendar()
    return Snapshot(week_label=f"{iso_year}-W{iso_week:02d}", monday=monday)


def days_between(end: date, start: date) -> int:
    """Signed day delta (end - start). Negative means future."""

    return (end - start).days
