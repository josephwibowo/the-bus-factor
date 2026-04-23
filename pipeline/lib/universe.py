"""Top-N package universe selection for npm and PyPI.

The universe is the canonical set of packages we score each week. We anchor
it on **dependency reach** rather than raw downloads because the project is
about "critical packages holding up the ecosystem," and dependency reach is
a stronger, less noisy signal than download counts.

### Two-tier architecture

* **Hot path (every run) — read a committed seed file.**
  The weekly pipeline reads ``pipeline/data/universe/{npm,pypi}.json`` which
  ships with the repo. This path has **zero** external API cost, makes CI
  reproducible, and decouples the weekly cadence from deps.dev's BigQuery
  billing.

* **Cold path (manual) — refresh the seed.**
  ``scripts/refresh_universe.py`` re-runs the expensive rankings (deps.dev
  BigQuery for npm, hugovk HTTP for PyPI) and rewrites the seed files. Run
  it every few weeks at most; the top-500 rankings change <1% week-over-week
  so we don't need to pay ~$0.50-$3 per weekly rerun for a near-static list.

### Sources (cold-path only)

* **npm** — ``bigquery-public-data.deps_dev_v1.Dependents`` (base table,
  partition-pinned to the latest snapshot, filtered to ``System = 'NPM'``
  and ``MinimumDepth = 1``) ranked by distinct direct dependents.
* **PyPI** — hugovk/top-pypi-packages daily JSON
  (https://hugovk.github.io/top-pypi-packages/top-pypi-packages.min.json),
  generated from PyPI's official BigQuery download statistics via pypinfo.

Libraries.io is explicitly **not** used: its own docs say the data is
scraped and "not validated, corrected, or curated for accuracy," which
makes it unsuitable as the canonical universe source.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.lib.http import HttpClient

logger = logging.getLogger(__name__)

ECOSYSTEMS: tuple[str, ...] = ("npm", "pypi")

# Committed seed directory.  Checked into git; refreshed via
# ``scripts/refresh_universe.py``.
UNIVERSE_SEED_DIR = Path(__file__).resolve().parents[1] / "data" / "universe"

PYPI_TOP_URL = "https://hugovk.github.io/top-pypi-packages/top-pypi-packages.min.json"


@dataclass(frozen=True)
class UniversePackage:
    """One entry in the committed universe seed.

    ``dependent_count`` is ``None`` when the seed wasn't produced by a source
    that exposes that signal (e.g. the ``--bootstrap`` npm-high-impact
    fallback, or the PyPI hugovk list). Downstream scoring treats ``None``
    as "unknown" and falls back to the next available importance signal.
    """

    name: str
    dependent_count: int | None = None


@dataclass(frozen=True)
class UniverseResult:
    ecosystem: str
    packages: tuple[UniversePackage, ...]
    source: str
    refreshed_at: str | None
    overlap_with_previous: float | None


# -------- Seed I/O --------------------------------------------------------


def _seed_path(ecosystem: str, seed_dir: Path) -> Path:
    return seed_dir / f"{ecosystem}.json"


def _coerce_package(entry: object) -> UniversePackage | None:
    """Accept either ``"name"`` or ``{"name": ..., "dependent_count": ...}``."""

    if isinstance(entry, str):
        return UniversePackage(name=entry)
    if isinstance(entry, dict):
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            return None
        count = entry.get("dependent_count")
        if isinstance(count, bool) or not isinstance(count, int):
            count = None
        return UniversePackage(name=name, dependent_count=count)
    return None


def _read_seed(ecosystem: str, seed_dir: Path) -> dict[str, Any] | None:
    path = _seed_path(ecosystem, seed_dir)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError):
        logger.warning("universe seed %s is unreadable; treating as missing", path)
        return None
    if not isinstance(payload, dict) or "packages" not in payload:
        return None
    raw_entries = payload.get("packages") or []
    coerced = [pkg for pkg in (_coerce_package(e) for e in raw_entries) if pkg]
    payload["packages"] = coerced
    return payload


def _serialise_packages(pkgs: list[UniversePackage]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for p in pkgs:
        entry: dict[str, object] = {"name": p.name}
        if p.dependent_count is not None:
            entry["dependent_count"] = int(p.dependent_count)
        out.append(entry)
    return out


def _write_seed(ecosystem: str, payload: dict[str, Any], seed_dir: Path) -> None:
    seed_dir.mkdir(parents=True, exist_ok=True)
    path = _seed_path(ecosystem, seed_dir)
    tmp = path.with_suffix(".tmp")
    serialisable = dict(payload)
    packages = serialisable.get("packages")
    if packages:
        first = packages[0]
        if isinstance(first, UniversePackage):
            serialisable["packages"] = _serialise_packages(packages)
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(serialisable, fh, indent=2, sort_keys=True)
        fh.write("\n")
    tmp.replace(path)


# -------- Cold-path fetchers (paid / live HTTP) --------------------------


def _fetch_npm_top_from_deps_dev(limit: int) -> list[UniversePackage]:
    """Return the top-N npm packages ranked by distinct direct dependents.

    Queries the **base** ``Dependents`` table (not ``DependentsLatest``; the
    ``*Latest`` relations are views that full-scan history). The base table
    is ``PARTITION BY DATE(SnapshotAt)`` and clustered on ``(System, Name,
    Version)``, so pinning the partition and filtering to ``System = 'NPM'``
    keeps the scan manageable.

    ``MinimumDepth = 1`` restricts to direct dependents.
    """

    from pipeline.lib import bq

    snapshot = bq.latest_deps_dev_snapshot_date()
    sql = f"""
    WITH totals AS (
        SELECT
            Name AS package_name,
            COUNT(DISTINCT Dependent.Name) AS dependent_count
        FROM `bigquery-public-data.deps_dev_v1.Dependents`
        WHERE DATE(SnapshotAt) = DATE '{snapshot}'
          AND System = 'NPM'
          AND MinimumDepth = 1
        GROUP BY Name
    )
    SELECT package_name, dependent_count
    FROM totals
    WHERE package_name IS NOT NULL
    ORDER BY dependent_count DESC, package_name ASC
    LIMIT {int(limit)}
    """
    rows = bq.run_query(sql, job_description=f"universe npm top-{limit}")
    result: list[UniversePackage] = []
    for r in rows:
        name = r.get("package_name")
        if not isinstance(name, str) or not name:
            continue
        count = r.get("dependent_count")
        count_i = int(count) if isinstance(count, int | float) else None
        result.append(UniversePackage(name=name, dependent_count=count_i))
    return result


async def _fetch_pypi_top(client: HttpClient, limit: int) -> list[UniversePackage]:
    """PyPI ranking from hugovk/top-pypi-packages (downloads-based).

    This list does not expose a per-package dependent count, so the
    ``dependent_count`` field is left unset and downstream scoring will fall
    back to downloads as the importance signal.
    """

    payload = await client.get_json(PYPI_TOP_URL)
    if not isinstance(payload, dict) or "rows" not in payload:
        raise RuntimeError("unexpected PyPI top-packages payload shape")
    rows = payload["rows"][:limit]
    return [UniversePackage(name=str(row["project"])) for row in rows]


# -------- Overlap gate ----------------------------------------------------


def compute_overlap(previous: list[str], fresh: list[str]) -> float:
    if not previous:
        return 1.0
    prev_set = set(previous)
    fresh_set = set(fresh)
    inter = len(prev_set & fresh_set)
    return inter / max(len(prev_set), 1)


def _names(pkgs: list[UniversePackage]) -> list[str]:
    return [p.name for p in pkgs]


def apply_overlap_gate(
    previous: list[UniversePackage] | None,
    fresh: list[UniversePackage],
    *,
    threshold: float = 0.95,
) -> tuple[list[UniversePackage], float | None]:
    if previous is None:
        return fresh, None
    overlap = compute_overlap(_names(previous), _names(fresh))
    if overlap >= threshold:
        return fresh, overlap
    logger.warning(
        "universe overlap %.3f below threshold %.3f; keeping previous list",
        overlap,
        threshold,
    )
    return previous, overlap


# -------- Public API ------------------------------------------------------


async def _refresh_async(
    ecosystem: str, limit: int, *, window: str
) -> tuple[list[UniversePackage], str]:
    if ecosystem == "npm":
        packages = await asyncio.to_thread(_fetch_npm_top_from_deps_dev, limit)
        return packages, "deps.dev Dependents (System=NPM, MinimumDepth=1)"
    async with HttpClient(window=window) as client:
        packages = await _fetch_pypi_top(client, limit)
    return packages, "hugovk/top-pypi-packages"


def refresh_seed(
    ecosystem: str,
    limit: int,
    *,
    window: str = "",
    seed_dir: Path | None = None,
    threshold: float = 0.95,
) -> UniverseResult:
    """Refresh the committed seed file by fetching the upstream ranking.

    Used by ``scripts/refresh_universe.py`` — NOT the hot path. The npm
    refresh hits BigQuery and is billable; the PyPI refresh is a single
    free HTTP call.
    """

    if ecosystem not in ECOSYSTEMS:
        raise ValueError(f"unknown ecosystem {ecosystem!r}")
    if limit <= 0:
        raise ValueError("limit must be positive")

    target = seed_dir or UNIVERSE_SEED_DIR
    previous_payload = _read_seed(ecosystem, target)
    previous = list(previous_payload["packages"]) if previous_payload else None

    fresh, source = asyncio.run(_refresh_async(ecosystem, limit, window=window))
    final, overlap = apply_overlap_gate(previous, fresh, threshold=threshold)

    refreshed_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "ecosystem": ecosystem,
        "source": source,
        "refreshed_at": refreshed_at,
        "packages": final[:limit],
    }
    _write_seed(ecosystem, payload, target)
    return UniverseResult(
        ecosystem=ecosystem,
        packages=tuple(final[:limit]),
        source=source,
        refreshed_at=refreshed_at,
        overlap_with_previous=overlap,
    )


def top_packages(
    ecosystem: str,
    limit: int,
    *,
    seed_dir: Path | None = None,
) -> UniverseResult:
    """Read the committed universe seed (hot path; zero external cost).

    Raises ``FileNotFoundError`` if the seed is missing — run
    ``python scripts/refresh_universe.py`` once to populate it.
    """

    if ecosystem not in ECOSYSTEMS:
        raise ValueError(f"unknown ecosystem {ecosystem!r}")
    if limit <= 0:
        raise ValueError("limit must be positive")

    target = seed_dir or UNIVERSE_SEED_DIR
    payload = _read_seed(ecosystem, target)
    if payload is None:
        raise FileNotFoundError(
            f"universe seed for {ecosystem!r} not found at "
            f"{_seed_path(ecosystem, target)}. Run: "
            "python scripts/refresh_universe.py"
        )
    # ``_read_seed`` already coerced entries to ``UniversePackage``
    pkgs: list[UniversePackage] = list(payload.get("packages", []))[:limit]
    return UniverseResult(
        ecosystem=ecosystem,
        packages=tuple(pkgs),
        source=str(payload.get("source", "unknown")),
        refreshed_at=(str(payload["refreshed_at"]) if payload.get("refreshed_at") else None),
        overlap_with_previous=None,
    )
