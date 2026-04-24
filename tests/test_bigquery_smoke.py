"""Tests for the BigQuery smoke harness."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.run_bigquery_smoke import (
    BqAsset,
    _exporter_env,
    asset_target_table_sql,
    dataset_id,
    render_custom_check_query,
    rewrite_dataset_refs,
    source_health_failure_query,
    topological_sort_assets,
)


def test_dataset_id_uses_prefix() -> None:
    assert dataset_id("mart", "bf_smoke") == "bf_smoke_mart"
    assert dataset_id("raw", "") == "raw"


def test_rewrite_dataset_refs_handles_plain_and_backticked_refs() -> None:
    sql = """
    CREATE OR REPLACE TABLE mart.package_scores AS
    SELECT *
    FROM raw.npm_registry
    JOIN `seed.known_states` USING (package_name)
    JOIN project.raw.external_name USING (package_name)
    """

    rewritten = rewrite_dataset_refs(sql, "bf_smoke")

    assert "TABLE bf_smoke_mart.package_scores" in rewritten
    assert "FROM bf_smoke_raw.npm_registry" in rewritten
    assert "JOIN `bf_smoke_seed.known_states`" in rewritten
    assert "project.raw.external_name" in rewritten


def test_asset_target_table_sql_uses_prefixed_dataset_and_project() -> None:
    asset = BqAsset(
        "mart.package_scores",
        Path("pipeline/assets_bq/marts/mart_package_scores.bq.sql"),
        (),
        (),
    )

    target = asset_target_table_sql(
        asset,
        project_id="bus-factor-494119",
        dataset_prefix="bf_smoke",
    )

    assert target == "`bus-factor-494119.bf_smoke_mart.package_scores`"


def test_render_custom_check_query_rewrites_source_mode_and_datasets() -> None:
    query = """
    SELECT COUNT(*)
    FROM mart.package_scores
    WHERE '{{ var.source_mode }}' = 'fixture'
      AND risk_score < {{ var.flagged_risk_score_min }}
    """

    rendered = render_custom_check_query(query, source_mode="live", dataset_prefix="bf_smoke")

    assert "FROM bf_smoke_mart.package_scores" in rendered
    assert "'live' = 'fixture'" in rendered
    assert "risk_score < 30" in rendered


def test_source_health_failure_query_rewrites_dataset() -> None:
    query = source_health_failure_query(dataset_prefix="bf_smoke")

    assert "FROM bf_smoke_stg.source_health" in query
    assert "status != 'ok'" in query
    assert "COALESCE(row_count, 0) = 0" in query


def test_exporter_env_marks_bigquery_bundle_as_live(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "bundle.duckdb"

    env = _exporter_env(
        duckdb_path=duckdb_path,
        source_mode="live",
        warehouse="bigquery",
        snapshot_week="2026-04-20",
    )

    assert json.loads(env["duckdb_default"]) == {"path": str(duckdb_path)}
    assert env["source_mode"] == "live"
    assert env["warehouse"] == "bigquery"
    assert env["snapshot_week"] == "2026-04-20"
    bruin_vars = json.loads(env["BRUIN_VARS"])
    assert bruin_vars["source_mode"] == "live"
    assert bruin_vars["warehouse"] == "bigquery"
    assert bruin_vars["snapshot_week"] == "2026-04-20"


def test_topological_sort_assets_places_dependencies_first() -> None:
    root = Path("pipeline/assets_bq")
    assets = [
        BqAsset(
            "mart.package_scores",
            root / "marts" / "mart_package_scores.bq.sql",
            ("int.snapshot",),
            (),
        ),
        BqAsset("int.snapshot", root / "intermediate" / "int_snapshot.bq.sql", (), ()),
        BqAsset(
            "stg.npm_registry",
            root / "staging" / "stg_npm_registry.bq.sql",
            ("raw.npm_registry",),
            (),
        ),
    ]

    ordered = [asset.name for asset in topological_sort_assets(assets)]

    assert ordered.index("int.snapshot") < ordered.index("mart.package_scores")
    assert "stg.npm_registry" in ordered
