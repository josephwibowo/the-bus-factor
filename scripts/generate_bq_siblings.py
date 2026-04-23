"""Generate BigQuery SQL siblings (*.bq.sql) from DuckDB SQL assets.

Single-shot helper used to bootstrap the dual-dialect graph.  Re-run after
editing a DuckDB asset to keep the BigQuery copy mechanically in sync.

BigQuery siblings live under ``pipeline/assets_bq/<layer>/`` (not
``pipeline/assets/``) so Bruin does not see them as duplicate-name assets
inside the DuckDB-flavoured pipeline.  ``tests/test_sql_parity.py``
enforces that the two trees stay column-for-column in sync; the Cycle 2
live-run path promotes the sidecar tree at run time.

Translations applied (all non-speculative, safe rewrites):

* ``type: duckdb.sql`` -> ``type: bq.sql``
* Inject ``tags: [- dialect:bigquery]`` (duckdb siblings get
  ``dialect:duckdb`` added by this script too for symmetric gating)
* ``DATE_DIFF('unit', a, b)`` -> ``DATE_DIFF(b, a, UNIT)``
* ``DATE_TRUNC('week', x)`` -> ``DATE_TRUNC(x, ISOWEEK)`` (and other units)
* ``EXTRACT(WEEK FROM x)`` -> ``EXTRACT(ISOWEEK FROM x)``
* ``REGEXP_REPLACE(a, b, c, 'g')`` -> ``REGEXP_REPLACE(a, b, c)``
* ``CAST(x AS VARCHAR)`` -> ``CAST(x AS STRING)``
* ``CAST(x AS DOUBLE)`` -> ``CAST(x AS FLOAT64)``

BigQuery already supports ``||`` as a string-concatenation operator, so we
do *not* rewrite it to ``CONCAT``.  Marts additionally receive
``partition_by: snapshot_week`` + a valid ``cluster_by`` list derived from
the output columns for cost pruning and clustering.
"""

from __future__ import annotations

import re
from pathlib import Path

PIPELINE = Path(__file__).resolve().parent.parent / "pipeline"
DUCKDB_ROOT = PIPELINE / "assets"
BQ_ROOT = PIPELINE / "assets_bq"
LAYERS = ("staging", "intermediate", "marts")


def translate(sql: str) -> str:
    def date_diff_sub(m: re.Match[str]) -> str:
        unit = m.group("unit").strip("'\"").upper()
        start = m.group("start").strip()
        end = m.group("end").strip()
        return f"DATE_DIFF({end}, {start}, {unit})"

    sql = re.sub(
        r"DATE_DIFF\(\s*'(?P<unit>[a-zA-Z]+)'\s*,\s*(?P<start>[^,]+?)\s*,\s*(?P<end>[^)]+?)\)",
        date_diff_sub,
        sql,
    )

    def date_trunc_sub(m: re.Match[str]) -> str:
        unit = m.group("unit").strip("'\"").lower()
        expr = m.group("expr").strip()
        bq_unit = {"week": "ISOWEEK"}.get(unit, unit.upper())
        return f"DATE_TRUNC({expr}, {bq_unit})"

    sql = re.sub(
        r"DATE_TRUNC\(\s*'(?P<unit>[a-zA-Z]+)'\s*,\s*(?P<expr>[^)]+?)\)",
        date_trunc_sub,
        sql,
    )

    sql = re.sub(
        r"EXTRACT\(\s*WEEK\s+FROM",
        "EXTRACT(ISOWEEK FROM",
        sql,
        flags=re.IGNORECASE,
    )

    def regexp_sub(m: re.Match[str]) -> str:
        return f"REGEXP_REPLACE({m.group(1)}, {m.group(2)}, {m.group(3)})"

    sql = re.sub(
        r"REGEXP_REPLACE\(\s*([^,]+?)\s*,\s*('[^']*'|\"[^\"]*\")\s*,\s*('[^']*'|\"[^\"]*\")\s*,\s*'g'\s*\)",
        regexp_sub,
        sql,
    )

    sql = re.sub(r"CAST\((.+?)\s+AS\s+VARCHAR\)", lambda m: f"CAST({m.group(1)} AS STRING)", sql)
    sql = re.sub(r"CAST\((.+?)\s+AS\s+DOUBLE\)", lambda m: f"CAST({m.group(1)} AS FLOAT64)", sql)
    sql = re.sub(r"\bSTRING_SPLIT\(", "SPLIT(", sql)

    return sql


def _frontmatter_columns(text: str) -> list[str]:
    return [
        match.group(1)
        for match in re.finditer(r"^\s*-\s+name:\s*([A-Za-z0-9_]+)\s*$", text, flags=re.MULTILINE)
    ]


def _cluster_columns(text: str) -> list[str]:
    output_cols = _frontmatter_columns(text)
    preferred = [
        ["ecosystem", "package_name"],
        ["ecosystem", "methodology_version"],
        ["ecosystem", "rank"],
        ["source_name", "source_category"],
        ["example_id"],
        ["row_order", "category"],
    ]
    for candidate in preferred:
        if all(col in output_cols for col in candidate):
            return candidate
    fallback = [col for col in output_cols if col != "snapshot_week"][:2]
    return fallback or ["snapshot_week"]


def ensure_dialect_tag(text: str, dialect: str) -> str:
    tag = f"dialect:{dialect}"
    if tag in text:
        return text
    if re.search(r"^tags:\s*$", text, flags=re.MULTILINE):
        return re.sub(
            r"^tags:\s*$",
            f"tags:\n  - {tag}",
            text,
            count=1,
            flags=re.MULTILINE,
        )
    return text.replace(
        "@bruin */",
        f"\ntags:\n  - {tag}\n\n@bruin */",
        1,
    )


def process_file(duckdb_path: Path) -> Path | None:
    text = duckdb_path.read_text()
    if "type: duckdb.sql" not in text:
        return None

    duckdb_text = ensure_dialect_tag(text, "duckdb")
    if duckdb_text != text:
        duckdb_path.write_text(duckdb_text)
        text = duckdb_text

    rel = duckdb_path.relative_to(DUCKDB_ROOT)
    bq_path = (BQ_ROOT / rel).with_suffix(".bq.sql")
    bq_path.parent.mkdir(parents=True, exist_ok=True)
    new_text = text.replace("type: duckdb.sql", "type: bq.sql")
    new_text = new_text.replace("dialect:duckdb", "dialect:bigquery")

    if (
        "marts" in str(duckdb_path)
        and "materialization:" in new_text
        and "partition_by" not in new_text
    ):
        cluster_cols = ", ".join(_cluster_columns(new_text))
        new_text = re.sub(
            r"materialization:\s*\n\s*type:\s*table",
            (
                "materialization:\n  type: table\n"
                "  partition_by: snapshot_week\n"
                f"  cluster_by: [{cluster_cols}]"
            ),
            new_text,
            count=1,
        )

    marker = "@bruin */"
    head, sep, body = new_text.partition(marker)
    if not sep:
        return None
    body = translate(body)
    bq_path.write_text(head + sep + body)
    return bq_path


def main() -> None:
    count = 0
    for layer in LAYERS:
        folder = DUCKDB_ROOT / layer
        for sql_file in sorted(folder.glob("*.sql")):
            if sql_file.name.endswith(".bq.sql"):
                continue
            result = process_file(sql_file)
            if result:
                count += 1
                print(f"wrote {result.relative_to(PIPELINE.parent)}")
    print(f"generated {count} BigQuery siblings")


if __name__ == "__main__":
    main()
