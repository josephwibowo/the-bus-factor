"""Run a small BigQuery validation over a Bruin live-smoke DuckDB snapshot.

Bruin currently registers the DuckDB SQL asset tree for normal local and CI
runs while the BigQuery SQL siblings live under ``pipeline/assets_bq``. This
script bridges that Cycle-2 gap by:

1. loading the live smoke ``seed`` and ``raw`` DuckDB tables into BigQuery,
2. rendering every BigQuery SQL sibling with Bruin, and
3. executing the rendered SQL plus mart custom checks in BigQuery.

The default ``bf_smoke`` dataset prefix avoids clobbering future production
datasets. For example, ``mart.source_health`` becomes
``bf_smoke_mart.source_health``.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.lib import bq  # noqa: E402
from pipeline.lib.sources import log_event  # noqa: E402

logger = logging.getLogger("pipeline.scripts.run_bigquery_smoke")
warnings.filterwarnings(
    "ignore",
    message="Loading pandas DataFrame into BigQuery will require pandas-gbq package version.*",
    category=FutureWarning,
    module="google.cloud.bigquery._pandas_helpers",
)

DATASET_LAYERS = ("seed", "raw", "stg", "int", "mart")
UPLOAD_LAYERS = ("seed", "raw")
BQ_ASSETS_ROOT = ROOT / "pipeline" / "assets_bq"
FRONTMATTER_RE = re.compile(r"/\*\s*@bruin(.*?)@bruin\s*\*/", re.DOTALL)
DATASET_REF_RE = re.compile(r"(?<![A-Za-z0-9_.-])(?P<tick>`?)(?P<layer>seed|raw|stg|int|mart)\.")


@dataclass(frozen=True)
class CustomCheck:
    asset_name: str
    check_name: str
    query: str


@dataclass(frozen=True)
class BqAsset:
    name: str
    path: Path
    depends: tuple[str, ...]
    custom_checks: tuple[CustomCheck, ...]


def dataset_id(layer: str, dataset_prefix: str) -> str:
    """Return the BigQuery dataset id for a logical pipeline layer."""

    if layer not in DATASET_LAYERS:
        raise ValueError(f"unknown dataset layer: {layer}")
    prefix = dataset_prefix.strip("_")
    return f"{prefix}_{layer}" if prefix else layer


def rewrite_dataset_refs(sql: str, dataset_prefix: str) -> str:
    """Rewrite two-part dataset references for prefixed smoke datasets."""

    if not dataset_prefix.strip("_"):
        return sql

    def repl(match: re.Match[str]) -> str:
        layer = match.group("layer")
        return f"{match.group('tick')}{dataset_id(layer, dataset_prefix)}."

    return DATASET_REF_RE.sub(repl, sql)


def render_custom_check_query(query: str, *, source_mode: str, dataset_prefix: str) -> str:
    """Render the small subset of Bruin variables used by custom checks."""

    rendered = query.replace("{{ var.source_mode }}", source_mode)
    rendered = rendered.replace("{{ var('source_mode') }}", source_mode)
    return rewrite_dataset_refs(rendered, dataset_prefix)


def _frontmatter(path: Path) -> dict[str, Any]:
    match = FRONTMATTER_RE.search(path.read_text())
    if not match:
        raise ValueError(f"{path.relative_to(ROOT)} missing Bruin frontmatter")
    loaded = yaml.safe_load(match.group(1)) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path.relative_to(ROOT)} frontmatter is not a mapping")
    return loaded


def _load_asset(path: Path) -> BqAsset:
    frontmatter = _frontmatter(path)
    name = str(frontmatter.get("name") or "")
    if not name:
        raise ValueError(f"{path.relative_to(ROOT)} missing asset name")

    depends = tuple(str(dep) for dep in frontmatter.get("depends", []) or [])
    checks: list[CustomCheck] = []
    for raw_check in frontmatter.get("custom_checks", []) or []:
        if not isinstance(raw_check, dict):
            continue
        check_name = str(raw_check.get("name") or "")
        query = str(raw_check.get("query") or "")
        if check_name and query:
            checks.append(CustomCheck(asset_name=name, check_name=check_name, query=query))

    return BqAsset(
        name=name,
        path=path,
        depends=depends,
        custom_checks=tuple(checks),
    )


def discover_bq_assets() -> list[BqAsset]:
    """Discover BigQuery SQL siblings under ``pipeline/assets_bq``."""

    paths = sorted(
        path
        for path in BQ_ASSETS_ROOT.rglob("*.bq.sql")
        if path.parts[-2] in {"staging", "intermediate", "marts"}
    )
    return [_load_asset(path) for path in paths]


def topological_sort_assets(assets: list[BqAsset]) -> list[BqAsset]:
    """Sort BigQuery assets so internal dependencies run before dependents."""

    by_name = {asset.name: asset for asset in assets}
    visiting: set[str] = set()
    visited: set[str] = set()
    ordered: list[BqAsset] = []

    def visit(asset: BqAsset) -> None:
        if asset.name in visited:
            return
        if asset.name in visiting:
            raise ValueError(f"cycle detected in BigQuery assets at {asset.name}")
        visiting.add(asset.name)
        for dep in asset.depends:
            dep_asset = by_name.get(dep)
            if dep_asset is not None:
                visit(dep_asset)
        visiting.remove(asset.name)
        visited.add(asset.name)
        ordered.append(asset)

    for asset in sorted(assets, key=lambda item: item.name):
        visit(asset)
    return ordered


def _duckdb_tables(
    con: duckdb.DuckDBPyConnection, layers: tuple[str, ...]
) -> list[tuple[str, str]]:
    placeholders = ", ".join("?" for _ in layers)
    rows = con.execute(
        f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ({placeholders})
          AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE '\\_dlt%' ESCAPE '\\'
        ORDER BY table_schema, table_name
        """,
        list(layers),
    ).fetchall()
    return [(str(schema), str(table)) for schema, table in rows]


def _create_dataset(client: Any, *, layer: str, dataset_prefix: str, location: str) -> None:
    from google.cloud import bigquery

    target_dataset = dataset_id(layer, dataset_prefix)
    dataset = bigquery.Dataset(f"{client.project}.{target_dataset}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    log_event(logger, logging.INFO, "bq_dataset_ready", dataset=target_dataset, location=location)


def upload_duckdb_sources(
    client: Any,
    *,
    duckdb_path: Path,
    dataset_prefix: str,
    location: str,
) -> None:
    """Upload ``seed`` and ``raw`` DuckDB tables to BigQuery."""

    from google.cloud import bigquery

    if not duckdb_path.exists():
        raise FileNotFoundError(
            f"{duckdb_path} does not exist; run the live Bruin smoke before BigQuery validation"
        )

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        tables = _duckdb_tables(con, UPLOAD_LAYERS)
        if not tables:
            raise RuntimeError(f"no seed/raw tables found in {duckdb_path}")

        for schema, table in tables:
            df = con.sql(f'SELECT * FROM "{schema}"."{table}"').fetchdf()
            target = f"{client.project}.{dataset_id(schema, dataset_prefix)}.{table}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            job = client.load_table_from_dataframe(
                df,
                target,
                job_config=job_config,
                location=location,
            )
            job.result()
            log_event(
                logger,
                logging.INFO,
                "bq_table_uploaded",
                table=target,
                rows=len(df),
            )
    finally:
        con.close()


def render_asset_sql(
    asset: BqAsset,
    *,
    source_mode: str,
    warehouse: str,
    snapshot_week: str,
) -> str:
    """Render one BigQuery asset through Bruin."""

    cmd = [
        "bruin",
        "render",
        str(asset.path.relative_to(ROOT)),
        "--var",
        f'source_mode="{source_mode}"',
        "--var",
        f'warehouse="{warehouse}"',
    ]
    if snapshot_week:
        cmd.extend(["--var", f'snapshot_week="{snapshot_week}"'])
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def execute_bq_assets(
    client: Any,
    assets: list[BqAsset],
    *,
    dataset_prefix: str,
    source_mode: str,
    warehouse: str,
    snapshot_week: str,
    location: str,
) -> None:
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        maximum_bytes_billed=bq.max_bytes_billed(),
    )
    for asset in assets:
        rendered = render_asset_sql(
            asset,
            source_mode=source_mode,
            warehouse=warehouse,
            snapshot_week=snapshot_week,
        )
        sql = rewrite_dataset_refs(rendered, dataset_prefix)
        log_event(logger, logging.INFO, "bq_asset_start", asset=asset.name)
        client.query(sql, job_config=job_config, location=location).result()
        log_event(logger, logging.INFO, "bq_asset_finish", asset=asset.name)


def _first_count(row: Any) -> int:
    value = row[0]
    if value is None:
        raise ValueError("custom check returned NULL")
    return int(value)


def run_custom_checks(
    client: Any,
    assets: list[BqAsset],
    *,
    dataset_prefix: str,
    source_mode: str,
    location: str,
) -> None:
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        maximum_bytes_billed=bq.max_bytes_billed(),
    )
    failures: list[str] = []
    checks = [check for asset in assets for check in asset.custom_checks]
    for check in checks:
        sql = render_custom_check_query(
            check.query,
            source_mode=source_mode,
            dataset_prefix=dataset_prefix,
        )
        rows = list(client.query(sql, job_config=job_config, location=location).result())
        if not rows:
            failures.append(f"{check.asset_name}.{check.check_name}: returned no rows")
            continue
        count = _first_count(rows[0])
        log_event(
            logger,
            logging.INFO,
            "bq_custom_check",
            asset=check.asset_name,
            check=check.check_name,
            failures=count,
        )
        if count != 0:
            failures.append(f"{check.asset_name}.{check.check_name}: {count}")

    if failures:
        raise RuntimeError("BigQuery custom checks failed: " + "; ".join(failures))


def validate_key_outputs(
    client: Any,
    *,
    dataset_prefix: str,
    location: str,
) -> dict[str, int]:
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        maximum_bytes_billed=bq.max_bytes_billed(),
    )
    tables = (
        ("raw", "npm_registry"),
        ("raw", "pypi_registry"),
        ("raw", "source_health"),
        ("mart", "source_health"),
        ("mart", "packages_current"),
        ("mart", "package_scores"),
        ("mart", "coverage_summary"),
    )
    counts: dict[str, int] = {}
    for layer, table in tables:
        table_ref = f"{client.project}.{dataset_id(layer, dataset_prefix)}.{table}"
        rows = list(
            client.query(
                f"SELECT COUNT(*) AS row_count FROM `{table_ref}`",
                job_config=job_config,
                location=location,
            ).result()
        )
        count = int(rows[0]["row_count"])
        counts[f"{dataset_id(layer, dataset_prefix)}.{table}"] = count
        log_event(logger, logging.INFO, "bq_row_count", table=table_ref, rows=count)

    health_ref = f"{client.project}.{dataset_id('mart', dataset_prefix)}.source_health"
    health_rows = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS total_sources,
                COUNTIF(status = 'ok' AND stale = FALSE AND failure_count = 0) AS ok_sources,
                COUNTIF(status != 'ok' OR stale = TRUE OR failure_count != 0) AS bad_sources
            FROM `{health_ref}`
            """,
            job_config=job_config,
            location=location,
        ).result()
    )
    health = health_rows[0]
    bad_sources = int(health["bad_sources"])
    total_sources = int(health["total_sources"])
    if total_sources == 0 or bad_sources != 0:
        raise RuntimeError(
            f"{health_ref} is unhealthy: total_sources={total_sources}, bad_sources={bad_sources}"
        )

    if counts[f"{dataset_id('mart', dataset_prefix)}.packages_current"] == 0:
        raise RuntimeError("mart.packages_current produced zero rows")
    if counts[f"{dataset_id('mart', dataset_prefix)}.package_scores"] == 0:
        raise RuntimeError("mart.package_scores produced zero rows")

    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=ROOT / "data" / "local_live_bq.duckdb",
        help="Bruin live-smoke DuckDB file to use as the source snapshot.",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="BigQuery project id. Defaults to GCP_PROJECT_ID/GOOGLE_CLOUD_PROJECT/ADC.",
    )
    parser.add_argument(
        "--location",
        default=os.environ.get("BQ_LOCATION", "US"),
        help="BigQuery dataset/query location.",
    )
    parser.add_argument(
        "--dataset-prefix",
        default=os.environ.get("BQ_SMOKE_DATASET_PREFIX", "bf_smoke"),
        help="Dataset prefix. Default writes bf_smoke_raw, bf_smoke_mart, etc.",
    )
    parser.add_argument(
        "--source-mode",
        default="live",
        choices=["fixture", "live"],
        help="Bruin source_mode variable used when rendering BQ SQL.",
    )
    parser.add_argument(
        "--warehouse",
        default="bigquery",
        help="Bruin warehouse variable used when rendering BQ SQL.",
    )
    parser.add_argument(
        "--snapshot-week",
        default=os.environ.get("SNAPSHOT_WEEK", ""),
        help="Optional snapshot_week Bruin variable (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Reuse existing prefixed seed/raw BigQuery tables and only rebuild SQL assets/checks.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.project_id:
        os.environ.setdefault("GCP_PROJECT_ID", args.project_id)
    client = bq.make_client()
    for layer in DATASET_LAYERS:
        _create_dataset(
            client,
            layer=layer,
            dataset_prefix=args.dataset_prefix,
            location=args.location,
        )

    if args.skip_upload:
        log_event(logger, logging.INFO, "bq_upload_skipped", dataset_prefix=args.dataset_prefix)
    else:
        upload_duckdb_sources(
            client,
            duckdb_path=args.duckdb_path,
            dataset_prefix=args.dataset_prefix,
            location=args.location,
        )

    assets = topological_sort_assets(discover_bq_assets())
    log_event(logger, logging.INFO, "bq_asset_plan", assets=len(assets))
    execute_bq_assets(
        client,
        assets,
        dataset_prefix=args.dataset_prefix,
        source_mode=args.source_mode,
        warehouse=args.warehouse,
        snapshot_week=args.snapshot_week,
        location=args.location,
    )
    run_custom_checks(
        client,
        assets,
        dataset_prefix=args.dataset_prefix,
        source_mode=args.source_mode,
        location=args.location,
    )
    counts = validate_key_outputs(
        client,
        dataset_prefix=args.dataset_prefix,
        location=args.location,
    )
    print("BigQuery smoke complete")
    print(f"project_id={client.project}")
    print(f"dataset_prefix={args.dataset_prefix}")
    for table, count in counts.items():
        print(f"{table}={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
