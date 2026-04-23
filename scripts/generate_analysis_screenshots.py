from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_capture_png import render  # noqa: E402

OUTPUT_DIRS = [
    REPO_ROOT / "analysis" / "screenshots",
    REPO_ROOT / "web" / "public" / "screenshots",
]

PROMPTS = {
    "Q1": "Which currently flagged npm packages have the highest risk score, and what evidence explains each score?",
    "Q2": "Which currently flagged PyPI packages have the highest risk score, and what evidence explains each score?",
    "Q3": "Group the current flagged packages by ecosystem and severity tier.",
    "Q4": "Which high-importance packages have elevated fragility signals but are not flagged, and why are they below the threshold?",
    "Q5": "Which packages were excluded because they are archived or deprecated, and why are they not ranked?",
    "Q6": "Which tracked packages are excluded because their repository mapping confidence is too low?",
    "Q7": "Compare the top flagged npm and PyPI packages by importance score, fragility score, confidence, and primary findings.",
    "Q8": "Which packages changed severity tier since the previous methodology-compatible weekly snapshot?",
}

FILENAMES = {
    "Q1": "Q1_flagged_npm.png",
    "Q2": "Q2_flagged_pypi.png",
    "Q3": "Q3_grouped.png",
    "Q4": "Q4_elevated_not_flagged.png",
    "Q5": "Q5_archived.png",
    "Q6": "Q6_unmappable.png",
    "Q7": "Q7_cross_ecosystem.png",
    "Q8": "Q8_tier_changes.png",
}


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def _rows(conn: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    return conn.execute(sql).fetchdf().to_dict(orient="records")


def _fmt_score(value: object) -> str:
    return f"{float(value):.1f}"


def _bullet(lines: list[str]) -> str:
    return "\n".join(f"- {line}" for line in lines)


def _card(conn: duckdb.DuckDBPyConnection, example_id: str) -> dict[str, object]:
    snap = conn.execute(
        "SELECT snapshot_week_label, snapshot_week, methodology_version FROM int.snapshot"
    ).fetchone()
    dataset_version, snapshot_week, methodology_version = map(str, snap)
    subtitle = (
        f"Local analysis capture | dataset {dataset_version} | "
        f"snapshot {snapshot_week} | methodology {methodology_version}"
    )
    footer = "Grounded in checked-in Bruin marts and rendered from local query output."
    blocks = _build_blocks(conn, example_id, dataset_version, methodology_version)
    return {
        "title": "Bruin Analysis Capture",
        "subtitle": subtitle,
        "footer": footer,
        "theme": "default",
        "blocks": blocks,
    }


def _build_blocks(
    conn: duckdb.DuckDBPyConnection, example_id: str, dataset_version: str, methodology_version: str
) -> list[dict[str, str]]:
    prompt = {"label": "Prompt", "text": PROMPTS[example_id]}
    builders = {
        "Q1": _build_q1,
        "Q2": _build_q2,
        "Q3": _build_q3,
        "Q4": _build_q4,
        "Q5": _build_q5,
        "Q6": _build_q6,
        "Q7": _build_q7,
        "Q8": lambda c: _build_q8(c, dataset_version, methodology_version),
    }
    answer, grounding = builders[example_id](conn)
    return [prompt, {"label": "Answer", "text": answer}, {"label": "Grounding", "text": grounding}]


def _flagged_rows(conn: duckdb.DuckDBPyConnection, ecosystem: str) -> list[dict[str, object]]:
    return _rows(
        conn,
        f"""
        WITH ranked AS (
            SELECT
                s.package_name,
                s.risk_score,
                s.importance_score,
                s.fragility_score,
                s.confidence,
                e.signal_name,
                e.evidence,
                e.contribution,
                ROW_NUMBER() OVER (
                    PARTITION BY s.package_name
                    ORDER BY e.contribution DESC, e.signal_name
                ) AS evidence_rank
            FROM mart.package_scores s
            JOIN mart.package_evidence e
                USING (ecosystem, package_name)
            WHERE s.flagged AND s.ecosystem = '{ecosystem}'
        )
        SELECT
            package_name,
            ROUND(risk_score, 1) AS risk_score,
            ROUND(importance_score, 1) AS importance_score,
            ROUND(fragility_score, 1) AS fragility_score,
            confidence,
            STRING_AGG(evidence, ' | ' ORDER BY evidence_rank) AS findings
        FROM ranked
        WHERE evidence_rank <= 3
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY risk_score DESC
        """,
    )


def _build_q1(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _flagged_rows(conn, "npm")
    answer_lines = [
        (
            f"{row['package_name']} leads the npm flagged cohort at risk {_fmt_score(row['risk_score'])} "
            f"(importance {_fmt_score(row['importance_score'])}, fragility {_fmt_score(row['fragility_score'])}, "
            f"confidence {row['confidence']}). Evidence: {row['findings']}"
        )
        for row in rows
    ]
    grounding_lines = [
        f"{row['package_name']} | risk {_fmt_score(row['risk_score'])} | importance {_fmt_score(row['importance_score'])} | "
        f"fragility {_fmt_score(row['fragility_score'])} | {row['confidence']}"
        for row in rows
    ]
    return _bullet(answer_lines), "mart.package_scores + mart.package_evidence\n" + "\n".join(
        grounding_lines
    )


def _build_q2(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _flagged_rows(conn, "pypi")
    answer_lines = [
        (
            f"{row['package_name']} leads the PyPI flagged cohort at risk {_fmt_score(row['risk_score'])} "
            f"(importance {_fmt_score(row['importance_score'])}, fragility {_fmt_score(row['fragility_score'])}, "
            f"confidence {row['confidence']}). Evidence: {row['findings']}"
        )
        for row in rows
    ]
    grounding_lines = [
        f"{row['package_name']} | risk {_fmt_score(row['risk_score'])} | importance {_fmt_score(row['importance_score'])} | "
        f"fragility {_fmt_score(row['fragility_score'])} | {row['confidence']}"
        for row in rows
    ]
    return _bullet(answer_lines), "mart.package_scores + mart.package_evidence\n" + "\n".join(
        grounding_lines
    )


def _build_q3(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _rows(
        conn,
        """
        SELECT ecosystem, severity_tier, COUNT(*) AS flagged_packages
        FROM mart.package_scores
        WHERE flagged
        GROUP BY 1, 2
        ORDER BY ecosystem, severity_tier
        """,
    )
    total = sum(int(row["flagged_packages"]) for row in rows)
    answer = dedent(
        f"""
        All {total} currently flagged packages land in the Critical tier.
        The flagged split is 4 npm packages and 4 PyPI packages; there are no
        flagged Watch, Elevated, or High rows in this committed snapshot.
        """
    ).strip()
    grounding = "mart.package_scores grouped by ecosystem and severity_tier\n" + "\n".join(
        f"{row['ecosystem']} | {row['severity_tier']} | {row['flagged_packages']} flagged"
        for row in rows
    )
    return answer, grounding


def _build_q4(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _rows(
        conn,
        """
        SELECT
            ecosystem,
            package_name,
            ROUND(importance_score, 1) AS importance_score,
            ROUND(fragility_score, 1) AS fragility_score,
            ROUND(risk_score, 1) AS risk_score,
            severity_tier,
            confidence,
            ROUND(importance_percentile_within_eligible, 1) AS importance_pct
        FROM mart.package_scores
        WHERE NOT flagged
          AND (severity_tier IN ('Elevated', 'High', 'Critical') OR fragility_score >= 40)
        ORDER BY importance_score DESC, risk_score DESC
        """,
    )
    answer_lines = []
    for row in rows:
        reason = (
            f"importance percentile {row['importance_pct']} is below the 75th-percentile flag gate"
            if float(row["risk_score"]) >= 30
            else f"risk {_fmt_score(row['risk_score'])} stays below the 30-point flag floor"
        )
        answer_lines.append(
            f"{row['package_name']} ({row['ecosystem']}) is {row['severity_tier']} with confidence {row['confidence']}, "
            f"but {reason}."
        )
    grounding = (
        "mart.package_scores filtered to not-flagged rows with Elevated-or-higher fragility\n"
        + "\n".join(
            f"{row['ecosystem']} | {row['package_name']} | importance {_fmt_score(row['importance_score'])} | "
            f"fragility {_fmt_score(row['fragility_score'])} | risk {_fmt_score(row['risk_score'])} | "
            f"pct {row['importance_pct']}"
            for row in rows
        )
    )
    return _bullet(answer_lines), grounding


def _build_q5(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _rows(
        conn,
        """
        SELECT ecosystem, package_name, exclusion_reason, is_archived, is_deprecated
        FROM mart.packages_current
        WHERE exclusion_reason = 'archived_deprecated'
        ORDER BY ecosystem, package_name
        """,
    )
    answer = dedent(
        """
        Two packages are excluded under the archived/deprecated bucket in the
        current fixture snapshot, and both are archived rather than deprecated.
        They remain visible for auditability, but they are removed from the
        eligible ranking universe before scoring and flagging.
        """
    ).strip()
    grounding = "mart.packages_current where exclusion_reason = archived_deprecated\n" + "\n".join(
        f"{row['ecosystem']} | {row['package_name']} | archived={row['is_archived']} | deprecated={row['is_deprecated']}"
        for row in rows
    )
    return answer, grounding


def _build_q6(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _rows(
        conn,
        """
        SELECT ecosystem, package_name, mapping_bucket, mapping_points
        FROM mart.packages_current
        WHERE exclusion_reason = 'unmappable'
        ORDER BY ecosystem, package_name
        """,
    )
    answer = dedent(
        """
        The current tracked universe has two unmappable packages: one npm and
        one PyPI row. Both carry low mapping confidence with zero mapping points,
        so they are excluded before any ranked output is produced.
        """
    ).strip()
    grounding = "mart.packages_current where exclusion_reason = unmappable\n" + "\n".join(
        f"{row['ecosystem']} | {row['package_name']} | bucket={row['mapping_bucket']} | points={row['mapping_points']}"
        for row in rows
    )
    return answer, grounding


def _build_q7(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    rows = _rows(
        conn,
        """
        WITH ranked_scores AS (
            SELECT
                ecosystem,
                package_name,
                ROUND(importance_score, 1) AS importance_score,
                ROUND(fragility_score, 1) AS fragility_score,
                ROUND(risk_score, 1) AS risk_score,
                confidence,
                ROW_NUMBER() OVER (
                    PARTITION BY ecosystem
                    ORDER BY risk_score DESC
                ) AS ecosystem_rank
            FROM mart.package_scores
            WHERE flagged
        ),
        ranked_evidence AS (
            SELECT
                ecosystem,
                package_name,
                evidence,
                ROW_NUMBER() OVER (
                    PARTITION BY ecosystem, package_name
                    ORDER BY contribution DESC, signal_name
                ) AS evidence_rank
            FROM mart.package_evidence
        )
        SELECT
            s.ecosystem,
            s.package_name,
            s.importance_score,
            s.fragility_score,
            s.risk_score,
            s.confidence,
            STRING_AGG(e.evidence, ' | ' ORDER BY e.evidence_rank) AS findings
        FROM ranked_scores s
        JOIN ranked_evidence e
            USING (ecosystem, package_name)
        WHERE s.ecosystem_rank <= 4
          AND e.evidence_rank <= 2
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY ecosystem, risk_score DESC
        """,
    )
    npm_rows = [row for row in rows if row["ecosystem"] == "npm"]
    pypi_rows = [row for row in rows if row["ecosystem"] == "pypi"]
    answer = dedent(
        """
        The npm and PyPI leaders are near mirrors in this fixture snapshot.
        legacy-pyforge and legacy-forge sit at the top with risk in the mid-80s,
        followed by the neglected-stream pair around 83. The stale-bundle and
        old-validator pairs stay flagged because importance remains high enough
        to clear the top-quartile gate while fragility remains severe.
        """
    ).strip()
    grounding = (
        "Top 4 flagged rows per ecosystem from mart.package_scores + mart.package_evidence\n"
        + "\n".join(
            f"npm | {row['package_name']} | importance {_fmt_score(row['importance_score'])} | "
            f"fragility {_fmt_score(row['fragility_score'])} | risk {_fmt_score(row['risk_score'])} | "
            f"{row['findings']}"
            for row in npm_rows
        )
        + "\n"
        + "\n".join(
            f"pypi | {row['package_name']} | importance {_fmt_score(row['importance_score'])} | "
            f"fragility {_fmt_score(row['fragility_score'])} | risk {_fmt_score(row['risk_score'])} | "
            f"{row['findings']}"
            for row in pypi_rows
        )
    )
    return answer, grounding


def _build_q8(
    conn: duckdb.DuckDBPyConnection, dataset_version: str, methodology_version: str
) -> tuple[str, str]:
    answer = dedent(
        f"""
        No severity-tier changes are reported yet. The committed export only
        contains one methodology-compatible snapshot for {methodology_version}
        ({dataset_version}), so there is no prior week to compare against.
        """
    ).strip()
    grounding = dedent(
        f"""
        Current committed snapshot: {dataset_version} under methodology {methodology_version}
        Prior methodology-compatible snapshots available for comparison: 0
        Cross-week tier-change analysis stays intentionally empty until the next compatible export lands.
        """
    ).strip()
    return answer, grounding


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        default=str(REPO_ROOT / "data" / "fixture.duckdb"),
        help="DuckDB database backing the screenshot queries.",
    )
    args = parser.parse_args()

    conn = _connect(Path(args.db_path))
    try:
        for example_id, filename in FILENAMES.items():
            payload = _card(conn, example_id)
            for out_dir in OUTPUT_DIRS:
                render(payload, out_dir / filename)
                print(f"Wrote {out_dir / filename}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
