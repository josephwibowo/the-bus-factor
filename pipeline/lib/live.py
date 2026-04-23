"""Shared helpers for live raw-asset ingestion.

Centralises the bits that every ``_live`` branch needs so each asset stays
focused on its source-specific parsing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path

from pipeline.lib import sources
from pipeline.lib.http import HttpClient
from pipeline.lib.snapshot import resolve_snapshot
from pipeline.lib.universe import top_packages

logger = logging.getLogger(__name__)

GITHUB_URL_RE = re.compile(
    r"github\.com[/:](?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+?)(?:\.git)?(?:$|[/#?])"
)


def repo_url_canonical(raw: str | None) -> str | None:
    """Normalise any GitHub URL form to ``https://github.com/{owner}/{repo}``.

    Returns ``None`` when the input does not point at github.com or can't be
    parsed. This mirrors the DuckDB normalisation in ``int.repo_mapping``.
    """

    if not raw:
        return None
    match = GITHUB_URL_RE.search(str(raw) + "/")
    if not match:
        return None
    owner = match.group("owner").strip()
    repo = match.group("repo").strip().rstrip(".")
    if not owner or not repo:
        return None
    return f"https://github.com/{owner.lower()}/{repo.lower()}"


def repo_urls_from_duckdb(db_path: Path | str) -> list[str]:
    """Read normalised GitHub repo URLs from the already-materialised registries.

    Called by the GitHub raw assets so they know which repos to hit without
    re-fetching the registry data.
    """

    import duckdb

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT repository_url FROM raw.npm_registry
            UNION ALL
            SELECT repository_url FROM raw.pypi_registry
            """
        ).fetchall()
    finally:
        conn.close()
    seen: dict[str, None] = {}
    for (raw,) in rows:
        canonical = repo_url_canonical(raw)
        if canonical is not None:
            seen.setdefault(canonical, None)
    urls = list(seen)
    sources.log_event(
        logger,
        logging.INFO,
        "repo_urls_resolved",
        db_path=Path(db_path),
        registry_rows=len(rows),
        canonical_urls=len(urls),
    )
    return urls


def _bruin_vars() -> dict[str, object]:
    """Parse ``BRUIN_VARS`` JSON injected by Bruin into Python asset runs.

    Bruin passes pipeline-level ``--var`` overrides as a single JSON blob on
    ``BRUIN_VARS`` (not as individual env vars), so we consult that first and
    fall back to discrete env vars for local testing.
    """

    blob = os.environ.get("BRUIN_VARS", "")
    if not blob:
        return {}
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _var(name: str, default: str = "") -> str:
    env_val = os.environ.get(name)
    if env_val is not None and env_val != "":
        return env_val
    parsed = _bruin_vars().get(name)
    if parsed is None:
        return default
    return str(parsed)


def duckdb_path() -> Path:
    """Resolve the DuckDB path from Bruin's connection JSON env."""

    blob = os.environ.get("duckdb_default")
    if blob:
        try:
            cfg = json.loads(blob)
            path_val = cfg.get("path") or blob
        except json.JSONDecodeError:
            path_val = blob
    else:
        path_val = "./data/fixture.duckdb"
    resolved = Path(path_val)
    if not resolved.is_absolute():
        repo_root = Path(__file__).resolve().parents[2]
        resolved = repo_root / resolved
    return resolved


def resolve_window() -> str:
    """Return the reporting-window ISO date (Monday)."""

    return resolve_snapshot(_var("snapshot_week") or None).iso


def resolve_limit(env_var: str, default: int = 250) -> int:
    raw = _var(env_var, str(default))
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return default
    return max(1, limit)


def resolve_universe(ecosystem: str, *, window: str) -> tuple[str, ...]:
    """Read the committed universe seed (hot path, zero external cost).

    ``window`` is accepted for call-site symmetry with other helpers; it is
    unused because the seed is fetched offline via
    ``scripts/refresh_universe.py``.
    """

    del window  # accepted for symmetry; seed is pre-computed
    env_var = "npm_package_limit" if ecosystem == "npm" else "pypi_package_limit"
    limit = resolve_limit(env_var)
    result = top_packages(ecosystem, limit=limit)
    sources.log_event(
        logger,
        logging.INFO,
        "universe_resolved",
        ecosystem=ecosystem,
        source=result.source,
        packages=len(result.packages),
        refreshed_at=result.refreshed_at,
        limit=limit,
    )
    return tuple(p.name for p in result.packages)


async def gather_bounded[T](
    items: Iterable[T],
    worker: Callable[[T, HttpClient], Awaitable[object]],
    *,
    client: HttpClient,
    concurrency: int = 16,
) -> list[object]:
    """Run ``worker`` for every item concurrently, bounded by semaphore."""

    sem = asyncio.Semaphore(concurrency)
    items_list = list(items)

    async def _run(item: T) -> object:
        async with sem:
            return await worker(item, client)

    return await asyncio.gather(*(_run(i) for i in items_list))


def live_mode() -> bool:
    return _var("source_mode", "fixture").lower() == "live"


def tracker(source_name: str) -> sources.SourceHealthTracker:
    return sources.SourceHealthTracker(source_name, window=resolve_window())


def log_event(
    event_logger: logging.Logger,
    level: int,
    event: str,
    **fields: object,
) -> None:
    sources.log_event(event_logger, level, event, **fields)
