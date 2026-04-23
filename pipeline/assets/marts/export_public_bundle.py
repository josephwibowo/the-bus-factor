"""@bruin

name: export_public_bundle
type: python

description: |
  Exports the public-data bundle (8 JSON artefacts) and writes JSON Schemas
  to web/public/schemas for build-time Zod validation. Every artefact is
  validated through its Pydantic model before being written so drift is
  caught in the pipeline rather than at the website layer.

  Artefacts:
    public-data/metadata.json
    public-data/leaderboard.json
    public-data/packages.json
    public-data/weekly.json
    public-data/coverage.json
    public-data/sources.json
    public-data/analysis.json
    public-data/positioning.json

depends:
  - mart.packages_current
  - mart.package_scores
  - mart.package_evidence
  - mart.weekly_findings
  - mart.coverage_summary
  - mart.source_health
  - mart.analysis_examples
  - mart.market_positioning

secrets:
  - key: duckdb_default

tags:
  - layer:export
  - surface:public

@bruin"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[3]
PIPELINE_ROOT = REPO_ROOT / "pipeline"

if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from lib.schemas import (  # noqa: E402
    PUBLIC_BUNDLE_SCHEMAS,
    Analysis,
    AnalysisExample,
    Coverage,
    CoverageEcosystemRow,
    FragilitySignal,
    Leaderboard,
    LeaderboardEntry,
    MappingConfidenceBreakdown,
    Metadata,
    PackageDetail,
    Packages,
    Positioning,
    PositioningRow,
    Sources,
    SourceStatus,
    Weekly,
    WeeklyFinding,
    WeeklyHeadline,
)

PUBLIC_DATA_DIR = REPO_ROOT / "public-data"
SCHEMAS_DIR = REPO_ROOT / "web" / "public" / "schemas"


def _connect() -> duckdb.DuckDBPyConnection:
    conn_json = os.environ.get("duckdb_default")
    if conn_json:
        try:
            cfg = json.loads(conn_json)
        except json.JSONDecodeError:
            cfg = {"path": conn_json}
    else:
        cfg = {}
    db_path = cfg.get("path") or str(REPO_ROOT / "data" / "fixture.duckdb")
    resolved = Path(db_path)
    if not resolved.is_absolute():
        resolved = REPO_ROOT / resolved
    return duckdb.connect(str(resolved), read_only=True)


def _is_na(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and value != value:
        return True
    try:
        import pandas as pd

        return bool(pd.isna(value))
    except (TypeError, ValueError, ImportError):
        return False


def _clean(value: Any) -> Any:
    return None if _is_na(value) else value


def _fetch(conn: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    df = conn.execute(sql).fetchdf()
    records = df.to_dict(orient="records")
    return [{str(k): _clean(v) for k, v in row.items()} for row in records]


def _iso_date_opt(value: Any) -> date | None:
    if _is_na(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _iso_date(value: Any) -> date:
    result = _iso_date_opt(value)
    if result is None:
        raise ValueError("expected non-null date value")
    return result


def _build_metadata(conn: duckdb.DuckDBPyConnection) -> Metadata:
    anchor = _fetch(
        conn, "SELECT snapshot_week, snapshot_week_label, methodology_version FROM int.snapshot"
    )[0]
    counts = _fetch(
        conn,
        "SELECT ecosystem, COUNT(*) AS tracked FROM mart.packages_current GROUP BY ecosystem",
    )
    sources = [
        _source_from_row(row)
        for row in _fetch(conn, "SELECT * FROM mart.source_health ORDER BY source_name")
    ]
    package_counts = {row["ecosystem"]: int(row["tracked"]) for row in counts}
    ecosystems = sorted(package_counts.keys())
    from pipeline.lib import live as live_lib

    if live_lib.live_mode():
        notes = [
            "Live ingestion: top-N packages resolved from public registries.",
            ("Scores derive from npm/PyPI registry, deps.dev, GitHub, OSV, and OpenSSF Scorecard."),
        ]
        data_license = (
            "Public data aggregated under each source's terms: npm + PyPI registry, "
            "deps.dev (Apache 2.0), GitHub Public REST/GraphQL, OSV.dev (CC-BY 4.0), "
            "OpenSSF Scorecard (Apache 2.0)."
        )
    else:
        notes = [
            "Fixture data: deterministic snapshot shipped with the repository.",
            "Live ingestion (Cycle 2) replaces raw_* assets with external APIs.",
        ]
        data_license = "Synthetic fixture data for The Bus Factor competition entry."
    return Metadata(
        snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
        snapshot_week_label=str(anchor["snapshot_week_label"]),
        methodology_version=str(anchor["methodology_version"]),
        generated_at=datetime.now(UTC),
        ecosystems_covered=[e for e in ecosystems if e in ("npm", "pypi")],
        package_counts=package_counts,
        sources=sources,
        data_license=data_license,
        notes=notes,
    )


def _source_from_row(row: dict[str, Any]) -> SourceStatus:
    ts = row.get("last_success_at")
    if ts is not None and not isinstance(ts, datetime):
        ts = datetime.fromisoformat(str(ts)) if ts else None
    return SourceStatus(
        source_name=str(row["source_name"]),
        status=str(row["status"]),
        last_success_at=ts,
        stale=bool(row["stale"]),
        failure_count=int(row["failure_count"] or 0),
        note=row.get("note"),
    )


def _build_leaderboard(conn: duckdb.DuckDBPyConnection) -> Leaderboard:
    rows = _fetch(
        conn,
        """
        SELECT
            s.ecosystem,
            s.package_name,
            p.slug,
            s.importance_rank_within_ecosystem,
            s.risk_score,
            s.severity_tier,
            s.flagged,
            s.importance_score,
            s.fragility_score,
            s.confidence,
            s.snapshot_week,
            s.methodology_version,
            COALESCE(ev.evidence, 'No dominant signal identified.') AS primary_finding
        FROM mart.package_scores s
        JOIN mart.packages_current p
            ON s.ecosystem = p.ecosystem AND s.package_name = p.package_name
        LEFT JOIN (
            SELECT ecosystem, package_name, evidence
            FROM (
                SELECT
                    ecosystem,
                    package_name,
                    evidence,
                    ROW_NUMBER() OVER (
                        PARTITION BY ecosystem, package_name
                        ORDER BY contribution DESC
                    ) AS rn
                FROM mart.package_evidence
            ) ranked
            WHERE rn = 1
        ) ev ON ev.ecosystem = s.ecosystem AND ev.package_name = s.package_name
        ORDER BY s.ecosystem, s.importance_rank_within_ecosystem
        """,
    )
    if not rows:
        anchor = _fetch(conn, "SELECT snapshot_week, methodology_version FROM int.snapshot")[0]
        return Leaderboard(
            snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
            methodology_version=str(anchor["methodology_version"]),
            entries=[],
        )
    entries = [
        LeaderboardEntry(
            ecosystem=row["ecosystem"],
            package_name=row["package_name"],
            slug=row["slug"],
            rank_within_ecosystem=int(row["importance_rank_within_ecosystem"]),
            risk_score=float(row["risk_score"]),
            severity_tier=row["severity_tier"],
            flagged=bool(row["flagged"]),
            importance_score=float(row["importance_score"]),
            fragility_score=float(row["fragility_score"]),
            confidence=row["confidence"],
            primary_finding=row["primary_finding"],
        )
        for row in rows
    ]
    return Leaderboard(
        snapshot_week=_iso_date(rows[0]["snapshot_week"]).isoformat(),
        methodology_version=str(rows[0]["methodology_version"]),
        entries=entries,
    )


def _build_packages(conn: duckdb.DuckDBPyConnection) -> Packages:
    anchor = _fetch(conn, "SELECT snapshot_week, methodology_version FROM int.snapshot")[0]

    current_rows = _fetch(
        conn,
        """
        SELECT
            p.ecosystem,
            p.package_name,
            p.slug,
            p.first_release_date,
            p.latest_release_date,
            p.is_deprecated,
            p.is_archived,
            p.repository_url,
            p.mapping_points,
            p.mapping_bucket,
            p.mapping_rationale,
            p.exclusion_reason,
            p.is_eligible,
            s.severity_tier,
            s.flagged,
            s.risk_score,
            s.importance_score,
            s.fragility_score,
            s.confidence
        FROM mart.packages_current p
        LEFT JOIN mart.package_scores s
            ON p.ecosystem = s.ecosystem AND p.package_name = s.package_name
        ORDER BY p.ecosystem, p.package_name
        """,
    )
    evidence_rows = _fetch(
        conn,
        "SELECT ecosystem, package_name, signal_name, contribution, evidence"
        " FROM mart.package_evidence",
    )
    commits = {
        (row["ecosystem"], row["package_name"]): row
        for row in _fetch(
            conn,
            """
            SELECT f.ecosystem, f.package_name, c.last_commit_date
            FROM int.fragility_inputs f
            LEFT JOIN int.repo_mapping m
                ON f.ecosystem = m.ecosystem AND f.package_name = m.package_name
            LEFT JOIN stg.github_commits c ON m.repository_url = c.repo_url
            """,
        )
    }

    by_key: dict[tuple[str, str], list[FragilitySignal]] = {}
    for row in evidence_rows:
        key = (row["ecosystem"], row["package_name"])
        by_key.setdefault(key, []).append(
            FragilitySignal(
                name=row["signal_name"],
                contribution=float(row["contribution"]),
                evidence=str(row["evidence"]),
            )
        )

    entries: list[PackageDetail] = []
    for row in current_rows:
        key = (row["ecosystem"], row["package_name"])
        mapping = MappingConfidenceBreakdown(
            points=int(row["mapping_points"] or 0),
            bucket=row["mapping_bucket"] or "low",
            rationale=[
                chunk.strip()
                for chunk in (row["mapping_rationale"] or "").split(",")
                if chunk.strip()
            ],
        )
        signals = by_key.get(key, [])
        signals_sorted = sorted(signals, key=lambda s: s.contribution, reverse=True)
        entries.append(
            PackageDetail(
                ecosystem=row["ecosystem"],
                package_name=row["package_name"],
                slug=row["slug"],
                snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
                methodology_version=str(anchor["methodology_version"]),
                severity_tier=(row["severity_tier"] or "Stable"),
                flagged=bool(row["flagged"] or False),
                risk_score=float(row["risk_score"] or 0.0),
                importance_score=float(row["importance_score"] or 0.0),
                fragility_score=float(row["fragility_score"] or 0.0),
                confidence=(row["confidence"] or "low"),
                repository_url=row["repository_url"],
                mapping_confidence=mapping,
                fragility_signals=signals_sorted,
                registry_url=_registry_url(row["ecosystem"], row["package_name"]),
                first_release_date=_iso_date(row["first_release_date"]),
                latest_release_date=_iso_date(row["latest_release_date"]),
                last_commit_date=_iso_date_opt((commits.get(key) or {}).get("last_commit_date")),
                is_archived=bool(row["is_archived"]),
                is_deprecated=bool(row["is_deprecated"]),
                exclusion_reason=row["exclusion_reason"],
            )
        )

    return Packages(
        snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
        methodology_version=str(anchor["methodology_version"]),
        entries=entries,
    )


def _registry_url(ecosystem: str, package_name: str) -> str | None:
    if ecosystem == "npm":
        return f"https://www.npmjs.com/package/{package_name}"
    if ecosystem == "pypi":
        return f"https://pypi.org/project/{package_name}/"
    return None


def _build_weekly(conn: duckdb.DuckDBPyConnection, flagged_counts: dict[str, int]) -> Weekly:
    rows = _fetch(
        conn,
        """
        SELECT rank, ecosystem, package_name, slug, severity_tier, risk_score, primary_finding
        FROM mart.weekly_findings ORDER BY rank
        """,
    )
    anchor = _fetch(
        conn, "SELECT snapshot_week, snapshot_week_label, methodology_version FROM int.snapshot"
    )[0]
    total_flagged = sum(flagged_counts.values())
    ecosystem_breakdown = {k: int(v) for k, v in flagged_counts.items()}
    findings = [
        WeeklyFinding(
            rank=int(r["rank"]),
            ecosystem=r["ecosystem"],
            package_name=r["package_name"],
            slug=r["slug"],
            severity_tier=r["severity_tier"],
            risk_score=float(r["risk_score"]),
            primary_finding=r["primary_finding"],
        )
        for r in rows
    ]
    if total_flagged == 0:
        headline_text = "No packages crossed the flag threshold this week."
        summary = (
            "Zero packages met the flagged criteria (risk >= 75 with >= 2 independent "
            "fragility signals and medium+ confidence) for "
            f"{anchor['snapshot_week_label']}. Continuity signals are healthy across the "
            "tracked set."
        )
        fallback = (
            "We do not manufacture alarm: when no package crosses the threshold we "
            "publish the empty state and highlight the methodology instead."
        )
    else:
        headline_text = (
            f"{total_flagged} package"
            f"{'s' if total_flagged != 1 else ''} flagged for continuity risk "
            f"in {anchor['snapshot_week_label']}."
        )
        summary = (
            "Flagged packages combine high downstream reach with at least two "
            "independent fragility signals. Repositories and maintainer identities are "
            "never referenced in the viral surfaces."
        )
        fallback = None
    return Weekly(
        headline=WeeklyHeadline(
            headline=headline_text,
            summary=summary,
            methodology_version=str(anchor["methodology_version"]),
            snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
            ecosystem_breakdown=ecosystem_breakdown,
        ),
        findings=findings,
        zero_flagged_fallback_copy=fallback,
    )


def _build_coverage(conn: duckdb.DuckDBPyConnection) -> Coverage:
    rows = _fetch(conn, "SELECT * FROM mart.coverage_summary ORDER BY ecosystem")
    anchor = _fetch(conn, "SELECT snapshot_week, methodology_version FROM int.snapshot")[0]
    return Coverage(
        snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
        methodology_version=str(anchor["methodology_version"]),
        rows=[
            CoverageEcosystemRow(
                ecosystem=r["ecosystem"],
                tracked=int(r["tracked"]),
                eligible=int(r["eligible"]),
                flagged=int(r["flagged"]),
                excluded_unmappable=int(r["excluded_unmappable"]),
                excluded_archived=int(r["excluded_archived"]),
                excluded_too_new=int(r["excluded_too_new"]),
                excluded_stub_types=int(r["excluded_stub_types"]),
            )
            for r in rows
        ],
    )


def _build_sources(conn: duckdb.DuckDBPyConnection) -> Sources:
    anchor = _fetch(conn, "SELECT snapshot_week FROM int.snapshot")[0]
    rows = _fetch(conn, "SELECT * FROM mart.source_health ORDER BY source_name")
    return Sources(
        snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
        sources=[_source_from_row(r) for r in rows],
    )


def _build_analysis(conn: duckdb.DuckDBPyConnection) -> Analysis:
    anchor = _fetch(conn, "SELECT snapshot_week FROM int.snapshot")[0]
    rows = _fetch(conn, "SELECT * FROM mart.analysis_examples ORDER BY example_id")
    return Analysis(
        snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
        entries=[
            AnalysisExample(
                example_id=str(r["example_id"]),
                prompt=str(r["prompt"]),
                answer_summary=str(r["answer_summary"]),
                screenshot_path=str(r["screenshot_path"]),
                dataset_version=str(r["dataset_version"]),
                methodology_version=str(r["methodology_version"]),
                capture_date=_iso_date(r["capture_date"]),
                capture_source=str(r["capture_source"]),
            )
            for r in rows
        ],
    )


def _build_positioning(conn: duckdb.DuckDBPyConnection) -> Positioning:
    anchor = _fetch(conn, "SELECT snapshot_week FROM int.snapshot")[0]
    rows = _fetch(conn, "SELECT * FROM mart.market_positioning ORDER BY row_order")
    return Positioning(
        snapshot_week=_iso_date(anchor["snapshot_week"]).isoformat(),
        rows=[
            PositioningRow(
                row_order=int(r["row_order"]),
                category=str(r["category"]),
                example_products=str(r["example_products"]),
                primary_question_answered=str(r["primary_question_answered"]),
                relationship_to_bus_factor=str(r["relationship_to_bus_factor"]),
            )
            for r in rows
        ],
    )


def _write_json(target: Path, model: Any) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = model.model_dump(mode="json")
    with target.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        fh.write("\n")


def _write_schemas() -> None:
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    for stem, model in PUBLIC_BUNDLE_SCHEMAS.items():
        schema = model.model_json_schema()
        with (SCHEMAS_DIR / f"{stem}.schema.json").open("w", encoding="utf-8") as fh:
            json.dump(schema, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")


def main() -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _connect()
    try:
        coverage = _build_coverage(conn)
        flagged_counts = {row.ecosystem: row.flagged for row in coverage.rows}
        bundle = {
            "metadata": _build_metadata(conn),
            "leaderboard": _build_leaderboard(conn),
            "packages": _build_packages(conn),
            "weekly": _build_weekly(conn, flagged_counts),
            "coverage": coverage,
            "sources": _build_sources(conn),
            "analysis": _build_analysis(conn),
            "positioning": _build_positioning(conn),
        }
    finally:
        conn.close()

    for stem, model in bundle.items():
        _write_json(PUBLIC_DATA_DIR / f"{stem}.json", model)
    _write_schemas()
    print(f"Wrote {len(bundle)} artefacts to {PUBLIC_DATA_DIR}")
    print(f"Wrote {len(PUBLIC_BUNDLE_SCHEMAS)} JSON Schemas to {SCHEMAS_DIR}")


main()
