"""@bruin

name: raw.deps_dev
type: python
connection: duckdb_default

materialization:
  type: table

depends:
  - seed.deps_dev
  - raw.npm_registry
  - raw.pypi_registry

description: |
  deps.dev-derived signals per package: downstream dependent count and
  SOURCE_REPO mapping. This asset is a **zero-cost adapter** in the weekly
  hot path:

  * ``dependent_count`` is copied from the committed universe seed
    (``pipeline/data/universe/{npm,pypi}.json``). The seed is refreshed out
    of band via ``scripts/refresh_universe.py`` (which runs the BigQuery
    query against the deps.dev public dataset). Refreshing monthly is
    sufficient because top-500 rankings change <1% week-over-week.
  * ``source_repo_url`` is joined in from the already-materialised
    ``raw.npm_registry`` / ``raw.pypi_registry`` tables, which parse the
    ``repository.url`` field from the per-package registry response.
  * ``transitive_vuln_count`` is left at zero; we do not yet compute a
    transitive vulnerability roll-up.

  The weekly pipeline therefore performs **no BigQuery calls** for this
  asset — only a local DuckDB read and a JSON read.

tags:
  - layer:raw
  - source:deps_dev

columns:
  - name: ecosystem
    type: varchar
  - name: package_name
    type: varchar
  - name: dependent_count
    type: bigint
  - name: source_repo_url
    type: varchar
  - name: transitive_vuln_count
    type: bigint
  - name: ingested_at
    type: timestamp

@bruin"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.lib import live
from pipeline.lib.universe import top_packages

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"


def _fixture() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE_ROOT / "deps_dev.csv")
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def _source_repo_map(db_path: Path) -> dict[tuple[str, str], str | None]:
    """Return ``{(ecosystem, package_name): repository_url}`` from DuckDB.

    Reads the already-materialised ``raw.npm_registry`` and
    ``raw.pypi_registry`` so we don't fetch registry data twice.
    """

    import duckdb

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        npm_rows = conn.execute(
            "SELECT package_name, repository_url FROM raw.npm_registry"
        ).fetchall()
        pypi_rows = conn.execute(
            "SELECT package_name, repository_url FROM raw.pypi_registry"
        ).fetchall()
    finally:
        conn.close()

    out: dict[tuple[str, str], str | None] = {}
    for name, url in npm_rows:
        out[("npm", str(name))] = live.repo_url_canonical(url) if url else None
    for name, url in pypi_rows:
        out[("pypi", str(name))] = live.repo_url_canonical(url) if url else None
    return out


def _live() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    with live.tracker("deps_dev") as t:
        source_repos = _source_repo_map(live.duckdb_path())
        for ecosystem in ("npm", "pypi"):
            limit_var = "npm_package_limit" if ecosystem == "npm" else "pypi_package_limit"
            limit = live.resolve_limit(limit_var)
            try:
                result = top_packages(ecosystem, limit=limit)
            except FileNotFoundError as exc:
                t.mark_degraded(f"{ecosystem}: {exc}")
                continue
            for pkg in result.packages:
                rows.append(
                    {
                        "ecosystem": ecosystem,
                        "package_name": pkg.name,
                        "dependent_count": (
                            pkg.dependent_count if pkg.dependent_count is not None else 0
                        ),
                        "source_repo_url": source_repos.get((ecosystem, pkg.name)),
                        "transitive_vuln_count": 0,
                    }
                )
        t.row_count = len(rows)
        if not rows:
            t.mark_failed("no deps_dev rows ingested")
    columns = [
        "ecosystem",
        "package_name",
        "dependent_count",
        "source_repo_url",
        "transitive_vuln_count",
    ]
    df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def materialize() -> pd.DataFrame:
    if not live.live_mode():
        return _fixture()
    return _live()
