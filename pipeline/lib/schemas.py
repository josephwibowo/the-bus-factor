"""Pydantic models for every public-data/*.json artefact.

These are the authoritative schemas for the static-site data contract. The
frontend validates the same shapes via generated JSON Schemas (see
``pipeline/assets/marts/export_public_bundle.py``).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------
# metadata.json
# ---------------------------------------------------------------------------
class SourceStatus(_Base):
    source_name: str
    status: Literal["ok", "degraded", "failed"]
    last_success_at: datetime | None
    stale: bool
    failure_count: NonNegativeInt
    note: str | None = None


class Metadata(_Base):
    snapshot_week: str = Field(..., description="ISO Monday date, e.g. 2026-04-20")
    snapshot_week_label: str = Field(..., description="ISO week label, e.g. 2026-W17")
    methodology_version: str
    generated_at: datetime
    ecosystems_covered: list[Literal["npm", "pypi"]]
    package_counts: dict[str, NonNegativeInt]
    sources: list[SourceStatus]
    data_license: str = "Synthetic fixture data for The Bus Factor competition entry."
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# leaderboard.json
# ---------------------------------------------------------------------------
class LeaderboardEntry(_Base):
    ecosystem: Literal["npm", "pypi"]
    package_name: str
    slug: str
    rank_within_ecosystem: int
    risk_score: float
    severity_tier: Literal["Unrated", "Stable", "Watch", "Elevated", "High", "Critical"]
    flagged: bool
    importance_score: float
    fragility_score: float
    confidence: Literal["low", "medium", "high"]
    primary_finding: str
    signals_above_threshold: int
    importance_percentile: float


class Leaderboard(_Base):
    snapshot_week: str
    methodology_version: str
    entries: list[LeaderboardEntry]


# ---------------------------------------------------------------------------
# packages/{slug}.json-equivalent and packages.json index
# ---------------------------------------------------------------------------
class FragilitySignal(_Base):
    name: Literal[
        "release_recency",
        "commit_recency",
        "release_cadence_decay",
        "issue_responsiveness",
        "all_time_contribution_concentration",
        "recent_commit_concentration_365d",
        "openssf_scorecard",
    ]
    contribution: float
    evidence: str


class MappingConfidenceBreakdown(_Base):
    points: int
    bucket: Literal["high", "medium", "low"]
    rationale: list[str]


class PackageDetail(_Base):
    ecosystem: Literal["npm", "pypi"]
    package_name: str
    slug: str
    snapshot_week: str
    methodology_version: str
    severity_tier: Literal["Unrated", "Stable", "Watch", "Elevated", "High", "Critical"]
    flagged: bool
    risk_score: float
    importance_score: float
    fragility_score: float
    confidence: Literal["low", "medium", "high"]
    repository_url: str | None
    mapping_confidence: MappingConfidenceBreakdown
    fragility_signals: list[FragilitySignal]
    registry_url: str | None
    first_release_date: date
    latest_release_date: date
    last_commit_date: date | None
    is_archived: bool
    is_deprecated: bool
    exclusion_reason: str | None = None


class Packages(_Base):
    snapshot_week: str
    methodology_version: str
    entries: list[PackageDetail]


# ---------------------------------------------------------------------------
# weekly.json
# ---------------------------------------------------------------------------
class WeeklyHeadline(_Base):
    headline: str
    summary: str
    methodology_version: str
    snapshot_week: str
    ecosystem_breakdown: dict[str, NonNegativeInt]


class WeeklyFinding(_Base):
    rank: int
    ecosystem: Literal["npm", "pypi"]
    package_name: str
    slug: str
    severity_tier: Literal["Unrated", "Stable", "Watch", "Elevated", "High", "Critical"]
    risk_score: float
    primary_finding: str


class Weekly(_Base):
    headline: WeeklyHeadline
    findings: list[WeeklyFinding]
    zero_flagged_fallback_copy: str | None = None


# ---------------------------------------------------------------------------
# coverage.json
# ---------------------------------------------------------------------------
class CoverageEcosystemRow(_Base):
    ecosystem: Literal["npm", "pypi"]
    tracked: NonNegativeInt
    eligible: NonNegativeInt
    flagged: NonNegativeInt
    excluded_unmappable: NonNegativeInt
    excluded_archived: NonNegativeInt
    excluded_too_new: NonNegativeInt
    excluded_stub_types: NonNegativeInt


class Coverage(_Base):
    snapshot_week: str
    methodology_version: str
    rows: list[CoverageEcosystemRow]


# ---------------------------------------------------------------------------
# sources.json
# ---------------------------------------------------------------------------
class Sources(_Base):
    snapshot_week: str
    sources: list[SourceStatus]


# ---------------------------------------------------------------------------
# analysis.json
# ---------------------------------------------------------------------------
class AnalysisExample(_Base):
    example_id: str
    prompt: str
    answer_summary: str
    screenshot_path: str
    dataset_version: str
    methodology_version: str
    capture_date: date
    capture_source: str


class Analysis(_Base):
    snapshot_week: str
    entries: list[AnalysisExample]


# ---------------------------------------------------------------------------
# positioning.json
# ---------------------------------------------------------------------------
class PositioningRow(_Base):
    row_order: int
    category: str
    example_products: str
    primary_question_answered: str
    relationship_to_bus_factor: str


class Positioning(_Base):
    snapshot_week: str
    rows: list[PositioningRow]


# ---------------------------------------------------------------------------
# Convenience index
# ---------------------------------------------------------------------------
PUBLIC_BUNDLE_SCHEMAS: dict[str, type[_Base]] = {
    "metadata": Metadata,
    "leaderboard": Leaderboard,
    "packages": Packages,
    "weekly": Weekly,
    "coverage": Coverage,
    "sources": Sources,
    "analysis": Analysis,
    "positioning": Positioning,
}
"""Filename stem (``{stem}.json``) -> Pydantic model mapping."""


def slugify(ecosystem: str, package_name: str) -> str:
    """Deterministic slug used in URLs and exported JSON.

    ``@scope/name`` → ``scope__name`` (two underscores are unique under
    PyPI's normalization rules and safe in filesystem paths).
    """

    cleaned = package_name.lstrip("@").replace("/", "__").replace(" ", "-").lower()
    return f"{ecosystem}-{cleaned}"
