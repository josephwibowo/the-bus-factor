"""Cross-process buffer for per-source ingestion telemetry.

Bruin runs each raw asset in its own Python process, so an in-memory
collector can't work. Each live raw asset calls :func:`record` on entry
and exit; ``record`` appends one JSON line to
``.cache/source_health/<snapshot_week>.jsonl``. The ``raw.source_health``
asset reads the file back and returns it as a DataFrame at the end of
the run.

Columns match ``seed.source_health`` so fixture and live modes share a
single downstream SQL surface.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

BUFFER_ROOT = Path(os.environ.get("BUS_FACTOR_SOURCE_HEALTH_BUFFER", ".cache/source_health"))

VALID_STATUSES: tuple[str, ...] = ("ok", "degraded", "failed")
DEFAULT_LOG_LEVEL = "INFO"
BUS_FACTOR_HANDLER_ATTR = "_bus_factor_bruin_handler"
LOG_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:/@+-]+$")


class BruinContextFilter(logging.Filter):
    """Inject Bruin run context into every pipeline log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.bruin_asset = os.environ.get("BRUIN_ASSET") or os.environ.get("BRUIN_THIS") or "-"
        record.bruin_run_id = os.environ.get("BRUIN_RUN_ID") or "-"
        return True


def _log_level() -> int:
    raw = os.environ.get("BUS_FACTOR_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    level = logging.getLevelName(raw)
    return level if isinstance(level, int) else logging.INFO


def configure_bruin_logging() -> None:
    """Configure pipeline loggers for Bruin-visible, unbuffered stdout output.

    Bruin sets ``PYTHONUNBUFFERED=1`` for Python assets, and its asset log
    stream captures process stdout. We attach one idempotent handler to the
    ``pipeline`` logger so module-level ``logging.getLogger(__name__)`` calls
    remain standard Python logging while still showing up in Bruin logs.
    """

    pipeline_logger = logging.getLogger("pipeline")
    if not any(
        getattr(handler, BUS_FACTOR_HANDLER_ATTR, False) for handler in pipeline_logger.handlers
    ):
        handler = logging.StreamHandler(sys.stdout)
        setattr(handler, BUS_FACTOR_HANDLER_ATTR, True)
        handler.addFilter(BruinContextFilter())
        handler.setFormatter(
            logging.Formatter(
                "level=%(levelname)s asset=%(bruin_asset)s "
                "run_id=%(bruin_run_id)s logger=%(name)s %(message)s"
            )
        )
        pipeline_logger.addHandler(handler)
        pipeline_logger.propagate = False
    pipeline_logger.setLevel(_log_level())


def _format_log_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    text = str(value)
    if LOG_VALUE_RE.fullmatch(text):
        return text
    return json.dumps(text)


def log_event(
    event_logger: logging.Logger,
    level: int,
    event: str,
    **fields: object,
) -> None:
    """Emit one structured, Bruin-visible log event.

    Messages use stable ``key=value`` fields so local runbooks can grep them
    reliably without requiring a JSON log collector.
    """

    configure_bruin_logging()
    kv_pairs = [f"event={event}"]
    kv_pairs.extend(f"{key}={_format_log_value(value)}" for key, value in fields.items())
    event_logger.log(level, " ".join(kv_pairs))


@dataclass
class SourceHealthRow:
    source_name: str
    status: str
    last_success_at: str | None = None
    stale: bool = False
    failure_count: int = 0
    note: str = ""
    latency_ms: float = 0.0
    row_count: int = 0
    window: str = field(default_factory=str)

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"status {self.status!r} not in {VALID_STATUSES}")


def _buffer_path(window: str, root: Path = BUFFER_ROOT) -> Path:
    safe_window = window or "default"
    return root / f"{safe_window}.jsonl"


def record(row: SourceHealthRow, *, root: Path | None = None) -> None:
    """Append ``row`` to the reporting-window buffer.

    Swallows IO errors so a telemetry hiccup can never break ingestion.
    """

    target_root = root or BUFFER_ROOT
    path = _buffer_path(row.window, target_root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(asdict(row), sort_keys=True) + "\n")
    except OSError as exc:
        logger.warning("failed to record source_health row: %s", exc)


def read_buffer(window: str, *, root: Path | None = None) -> list[SourceHealthRow]:
    """Read all rows for a given reporting window, most-recent-first."""

    target_root = root or BUFFER_ROOT
    path = _buffer_path(window, target_root)
    if not path.exists():
        return []
    out: list[SourceHealthRow] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                out.append(SourceHealthRow(**data))
            except (TypeError, ValueError):
                continue
    return out


def dedupe_latest_per_source(rows: list[SourceHealthRow]) -> list[SourceHealthRow]:
    """Keep only the most recent row per source_name, preserving insertion order.

    We define "most recent" as the last entry with the same ``source_name`` in
    the input list (append order mirrors wall-clock order).
    """

    latest: dict[str, SourceHealthRow] = {}
    for row in rows:
        latest[row.source_name] = row
    return list(latest.values())


def clear_buffer(window: str, *, root: Path | None = None) -> None:
    target_root = root or BUFFER_ROOT
    path = _buffer_path(window, target_root)
    if path.exists():
        path.unlink()


def new_row(source_name: str, *, window: str) -> SourceHealthRow:
    """Start a new row; helpers below finalize it to ok/degraded/failed."""

    return SourceHealthRow(
        source_name=source_name,
        status="ok",
        last_success_at=None,
        stale=False,
        failure_count=0,
        note="",
        latency_ms=0.0,
        row_count=0,
        window=window,
    )


class SourceHealthTracker:
    """Context manager that records start + end telemetry for a single source.

    Example::

        with SourceHealthTracker("npm_registry", window="2026-04-20") as t:
            df = fetch()
            t.row_count = len(df)
        # on exit: writes ok / failed to the buffer
    """

    def __init__(
        self,
        source_name: str,
        *,
        window: str,
        root: Path | None = None,
    ) -> None:
        self.source_name = source_name
        self.window = window
        self.root = root
        self.row_count: int = 0
        self.note: str = ""
        self._started_at: float = 0.0
        self._status: str = "ok"
        self._failures: int = 0

    def __enter__(self) -> SourceHealthTracker:
        configure_bruin_logging()
        self._started_at = time.perf_counter()
        log_event(
            logger,
            logging.INFO,
            "source_start",
            source=self.source_name,
            window=self.window,
        )
        return self

    def mark_degraded(self, note: str) -> None:
        self._status = "degraded"
        self._failures += 1
        self.note = note
        log_event(
            logger,
            logging.WARNING,
            "source_degraded",
            source=self.source_name,
            window=self.window,
            note=note,
        )

    def mark_failed(self, note: str) -> None:
        self._status = "failed"
        self._failures += 1
        self.note = note
        log_event(
            logger,
            logging.ERROR,
            "source_failed",
            source=self.source_name,
            window=self.window,
            note=note,
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        latency_ms = max(0.0, (time.perf_counter() - self._started_at) * 1000.0)
        if exc is not None:
            self._status = "failed"
            self._failures += 1
            self.note = self.note or f"{type(exc).__name__}: {exc}"[:200]
            logger.exception(
                "event=source_exception source=%s window=%s error_type=%s error=%s",
                self.source_name,
                self.window,
                type(exc).__name__,
                exc,
            )
        row = SourceHealthRow(
            source_name=self.source_name,
            status=self._status,
            last_success_at=(
                datetime.now(UTC).isoformat(timespec="seconds") if self._status == "ok" else None
            ),
            stale=self._status != "ok",
            failure_count=self._failures,
            note=self.note,
            latency_ms=round(latency_ms, 2),
            row_count=int(self.row_count),
            window=self.window,
        )
        record(row, root=self.root)
        level = logging.INFO if row.status == "ok" else logging.ERROR
        if row.status == "degraded":
            level = logging.WARNING
        log_event(
            logger,
            level,
            "source_finish",
            source=row.source_name,
            window=row.window,
            status=row.status,
            rows=row.row_count,
            latency_ms=row.latency_ms,
            failures=row.failure_count,
            note=row.note,
        )
        # We do not suppress the exception.
