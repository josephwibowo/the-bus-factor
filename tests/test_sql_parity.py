"""Parity tests between DuckDB assets (pipeline/assets/) and BigQuery siblings (pipeline/assets_bq/).

Every ``.sql`` asset under ``pipeline/assets/{staging,intermediate,marts}/`` must have a
``.bq.sql`` sibling at the matching path under ``pipeline/assets_bq/``.  Both files must:

* Declare the same asset ``name:`` field in the Bruin frontmatter.
* Produce the same top-level ``SELECT`` column list (order-insensitive).
* Declare matching ``depends:`` blocks (set equality, order-insensitive).

We parse columns from the frontmatter ``columns:`` block rather than the SQL body: every
asset in this project already types every output column in frontmatter for Bruin checks,
and that list is the externally observable contract callers downstream rely on.

Why this matters
    BigQuery and DuckDB have different SQL dialects (DATE_DIFF arg order, DATE_TRUNC unit,
    REGEXP_REPLACE 'g' flag, VARCHAR vs STRING, ...).  If the two siblings drift in either
    their output columns or their dependency graph, the weekly live run will produce a
    public-data bundle whose schema mismatches the fixture tests.  This parity test is
    the guardrail.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
DUCKDB_ROOT = REPO / "pipeline" / "assets"
BQ_ROOT = REPO / "pipeline" / "assets_bq"
LAYERS = ("staging", "intermediate", "marts")


def _find_duckdb_assets() -> list[Path]:
    out: list[Path] = []
    for layer in LAYERS:
        folder = DUCKDB_ROOT / layer
        for p in sorted(folder.glob("*.sql")):
            if p.name.endswith(".bq.sql"):
                continue
            out.append(p)
    return out


def _frontmatter(path: Path) -> str:
    text = path.read_text()
    m = re.search(r"/\*\s*@bruin(.*?)@bruin\s*\*/", text, flags=re.DOTALL)
    if not m:
        raise AssertionError(f"{path} missing Bruin frontmatter")
    return m.group(1)


def _name(frontmatter: str) -> str:
    m = re.search(r"^\s*name:\s*(\S+)", frontmatter, flags=re.MULTILINE)
    if not m:
        raise AssertionError("frontmatter missing `name:`")
    return m.group(1).strip()


def _columns(frontmatter: str) -> list[str]:
    m = re.search(r"^columns:\s*\n(.*?)(?=^\w|\Z)", frontmatter, flags=re.MULTILINE | re.DOTALL)
    if not m:
        return []
    block = m.group(1)
    return [cm.group(1) for cm in re.finditer(r"^\s*-\s*name:\s*(\S+)", block, flags=re.MULTILINE)]


def _depends(frontmatter: str) -> set[str]:
    m = re.search(
        r"^depends:\s*\n(?P<body>(?:[\t ]+-[^\n]*\n)+)",
        frontmatter,
        flags=re.MULTILINE,
    )
    if not m:
        return set()
    return {dm.group(1).strip() for dm in re.finditer(r"-\s*(\S+)", m.group("body"))}


@pytest.fixture(scope="module")
def duckdb_assets() -> list[Path]:
    return _find_duckdb_assets()


def test_every_duckdb_asset_has_bq_sibling(duckdb_assets: list[Path]) -> None:
    assert duckdb_assets, "expected at least one DuckDB SQL asset under pipeline/assets/"
    missing: list[str] = []
    for duckdb_path in duckdb_assets:
        rel = duckdb_path.relative_to(DUCKDB_ROOT)
        bq_path = (BQ_ROOT / rel).with_suffix(".bq.sql")
        if not bq_path.exists():
            missing.append(str(bq_path.relative_to(REPO)))
    assert not missing, (
        "BigQuery siblings missing for:\n  - "
        + "\n  - ".join(missing)
        + "\n\nRun `uv run python scripts/generate_bq_siblings.py` to regenerate."
    )


def test_bq_siblings_declare_bq_sql_type(duckdb_assets: list[Path]) -> None:
    for duckdb_path in duckdb_assets:
        rel = duckdb_path.relative_to(DUCKDB_ROOT)
        bq_path = (BQ_ROOT / rel).with_suffix(".bq.sql")
        if not bq_path.exists():
            continue
        body = bq_path.read_text()
        assert "type: bq.sql" in body, (
            f"{bq_path.relative_to(REPO)} must declare `type: bq.sql` in frontmatter"
        )
        assert "type: duckdb.sql" not in body, (
            f"{bq_path.relative_to(REPO)} must not carry `type: duckdb.sql`"
        )


def test_bq_siblings_match_name_and_columns(duckdb_assets: list[Path]) -> None:
    mismatches: list[str] = []
    for duckdb_path in duckdb_assets:
        rel = duckdb_path.relative_to(DUCKDB_ROOT)
        bq_path = (BQ_ROOT / rel).with_suffix(".bq.sql")
        if not bq_path.exists():
            continue
        dfm = _frontmatter(duckdb_path)
        bfm = _frontmatter(bq_path)
        dname = _name(dfm)
        bname = _name(bfm)
        if dname != bname:
            mismatches.append(f"{rel}: name differs ({dname!r} vs {bname!r})")
        dcols = _columns(dfm)
        bcols = _columns(bfm)
        if sorted(dcols) != sorted(bcols):
            only_duck = sorted(set(dcols) - set(bcols))
            only_bq = sorted(set(bcols) - set(dcols))
            mismatches.append(
                f"{rel}: column lists differ. DuckDB-only={only_duck}, BigQuery-only={only_bq}"
            )
    assert not mismatches, "Dual-dialect drift:\n  - " + "\n  - ".join(mismatches)


def test_bq_siblings_match_depends(duckdb_assets: list[Path]) -> None:
    mismatches: list[str] = []
    for duckdb_path in duckdb_assets:
        rel = duckdb_path.relative_to(DUCKDB_ROOT)
        bq_path = (BQ_ROOT / rel).with_suffix(".bq.sql")
        if not bq_path.exists():
            continue
        dd = _depends(_frontmatter(duckdb_path))
        bd = _depends(_frontmatter(bq_path))
        if dd != bd:
            mismatches.append(
                f"{rel}: depends differ. DuckDB-only={sorted(dd - bd)}, "
                f"BigQuery-only={sorted(bd - dd)}"
            )
    assert not mismatches, "Dual-dialect dependency drift:\n  - " + "\n  - ".join(mismatches)


def test_github_release_latest_date_is_nullable_in_staging() -> None:
    """Some active GitHub repos publish no release objects; that signal is nullable."""
    for path in [
        DUCKDB_ROOT / "staging" / "stg_github_releases.sql",
        BQ_ROOT / "staging" / "stg_github_releases.bq.sql",
    ]:
        block_match = re.search(
            r"- name: latest_release_date\b.*?(?=\n\s+- name:|\n\s*@bruin|\Z)",
            _frontmatter(path),
            flags=re.DOTALL,
        )
        assert block_match is not None
        assert "name: not_null" not in block_match.group(0)


def test_known_state_checks_are_fixture_only() -> None:
    for path in [
        DUCKDB_ROOT / "marts" / "mart_package_scores.sql",
        BQ_ROOT / "marts" / "mart_package_scores.bq.sql",
    ]:
        body = path.read_text()
        assert body.count("'{{ var.source_mode }}' = 'fixture'") == 2
        assert "'{{ var.source_mode }}' = 'live'" in body


def test_live_scored_ecosystem_floor_requires_missing_ecosystems() -> None:
    for path in [
        DUCKDB_ROOT / "marts" / "mart_package_scores.sql",
        BQ_ROOT / "marts" / "mart_package_scores.bq.sql",
    ]:
        body = path.read_text()
        assert "SELECT 'npm' AS ecosystem" in body
        assert "SELECT 'pypi' AS ecosystem" in body
        assert "LEFT JOIN scored_counts c USING (ecosystem)" in body
        assert "COALESCE(c.n, 0) < 50" in body


def test_live_source_health_publish_gate_only_blocks_critical_sources() -> None:
    for path in [
        DUCKDB_ROOT / "marts" / "mart_package_scores.sql",
        BQ_ROOT / "marts" / "mart_package_scores.bq.sql",
    ]:
        body = path.read_text()
        assert (
            "source_name IN ('npm_registry', 'pypi_registry', 'deps_dev', 'github_repos')" in body
        )


def test_marts_carry_partition_and_cluster() -> None:
    marts = sorted((BQ_ROOT / "marts").glob("*.bq.sql"))
    assert marts, "No BQ mart files discovered"
    missing: list[str] = []
    for path in marts:
        fm = _frontmatter(path)
        if "materialization:" not in fm:
            continue
        if "partition_by: snapshot_week" not in fm:
            missing.append(f"{path.relative_to(REPO)}: missing partition_by: snapshot_week")
        cluster_match = re.search(r"cluster_by:\s*\[([^\]]+)\]", fm)
        if cluster_match is None:
            missing.append(f"{path.relative_to(REPO)}: missing cluster_by")
            continue
        cluster_cols = {col.strip() for col in cluster_match.group(1).split(",")}
        output_cols = set(_columns(fm))
        invalid_cols = sorted(cluster_cols - output_cols)
        if invalid_cols:
            missing.append(
                f"{path.relative_to(REPO)}: cluster_by references non-output columns {invalid_cols}"
            )
    assert not missing, "BigQuery mart frontmatter drift:\n  - " + "\n  - ".join(missing)


def test_bq_siblings_use_bigquery_date_diff_arg_order() -> None:
    """DuckDB DATE_DIFF('unit', start, end) must not survive in BigQuery siblings."""
    for layer in LAYERS:
        folder = BQ_ROOT / layer
        for path in sorted(folder.glob("*.bq.sql")):
            body = path.read_text()
            offenders = re.findall(r"DATE_DIFF\(\s*'[a-zA-Z]+'\s*,", body)
            assert not offenders, (
                f"{path.relative_to(REPO)} contains DuckDB-style DATE_DIFF "
                f"('unit', start, end) - must be DATE_DIFF(end, start, UNIT) in BigQuery"
            )


def test_bq_siblings_use_bigquery_date_trunc_arg_order() -> None:
    for layer in LAYERS:
        folder = BQ_ROOT / layer
        for path in sorted(folder.glob("*.bq.sql")):
            body = path.read_text()
            offenders = re.findall(r"DATE_TRUNC\(\s*'[a-zA-Z]+'\s*,", body)
            assert not offenders, (
                f"{path.relative_to(REPO)} contains DuckDB-style DATE_TRUNC "
                f"('unit', x) - must be DATE_TRUNC(x, UNIT) in BigQuery"
            )


def test_bq_siblings_strip_regex_replace_g_flag() -> None:
    for layer in LAYERS:
        folder = BQ_ROOT / layer
        for path in sorted(folder.glob("*.bq.sql")):
            body = path.read_text()
            offenders = re.findall(r"REGEXP_REPLACE[\s\S]{0,250},\s*'g'\s*\)", body)
            assert not offenders, (
                f"{path.relative_to(REPO)} contains a 4-arg REGEXP_REPLACE (..., 'g') - "
                f"BigQuery only accepts 3 args"
            )


def test_bq_siblings_use_bigquery_split_function() -> None:
    for layer in LAYERS:
        folder = BQ_ROOT / layer
        for path in sorted(folder.glob("*.bq.sql")):
            body = path.read_text()
            assert "STRING_SPLIT(" not in body.upper(), (
                f"{path.relative_to(REPO)} contains DuckDB STRING_SPLIT; "
                "BigQuery uses SPLIT(value, delimiter)"
            )


def test_bq_siblings_do_not_use_duckdb_only_aggregates() -> None:
    for layer in LAYERS:
        folder = BQ_ROOT / layer
        for path in sorted(folder.glob("*.bq.sql")):
            body = path.read_text().upper()
            assert "CONCAT_WS(" not in body, (
                f"{path.relative_to(REPO)} contains DuckDB CONCAT_WS; "
                "BigQuery uses ARRAY_TO_STRING/CONCAT"
            )
            assert " FILTER (WHERE " not in body, (
                f"{path.relative_to(REPO)} contains SQL FILTER syntax; "
                "use BigQuery COUNTIF/SUM(IF(...))"
            )


def test_bq_siblings_do_not_use_duckdb_trim_both_syntax() -> None:
    for layer in LAYERS:
        folder = BQ_ROOT / layer
        for path in sorted(folder.glob("*.bq.sql")):
            body = path.read_text().upper()
            assert "TRIM(BOTH" not in body, (
                f"{path.relative_to(REPO)} contains DuckDB TRIM(BOTH ... FROM ...); "
                "BigQuery uses TRIM(value, characters)"
            )


def test_bq_staging_casts_raw_date_columns() -> None:
    required_casts = {
        "stg_npm_registry.bq.sql": [
            "CAST(first_release_date AS DATE) AS first_release_date",
            "CAST(latest_release_date AS DATE) AS latest_release_date",
        ],
        "stg_pypi_registry.bq.sql": [
            "CAST(first_release_date AS DATE) AS first_release_date",
            "CAST(latest_release_date AS DATE) AS latest_release_date",
        ],
        "stg_github_commits.bq.sql": [
            "CAST(last_commit_date AS DATE) AS last_commit_date",
        ],
        "stg_github_releases.bq.sql": [
            "CAST(latest_release_date AS DATE) AS latest_release_date",
        ],
    }
    for filename, snippets in required_casts.items():
        body = (BQ_ROOT / "staging" / filename).read_text()
        for snippet in snippets:
            assert snippet in body, (
                f"pipeline/assets_bq/staging/{filename} must cast raw DuckDB-loaded "
                "date/timestamp values before downstream BigQuery DATE_DIFF"
            )


def test_bq_partitioned_marts_do_not_end_with_order_by() -> None:
    for path in sorted((BQ_ROOT / "marts").glob("*.bq.sql")):
        body = path.read_text().rstrip()
        assert not re.search(r"\nORDER BY\s+[A-Za-z0-9_,. ]+$", body), (
            f"{path.relative_to(REPO)} ends with ORDER BY; BigQuery partitioned CTAS "
            "requires ordering at read time"
        )
