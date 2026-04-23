"""Typed loader for ``pipeline/config/scoring.yml``.

The scoring config is the single source of truth for weights, thresholds,
severity tiers, and eligibility rules. SQL assets consume it via Jinja
macros; Python assets and tests use :func:`load_scoring_config`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

SCORING_YAML_PATH = Path(__file__).resolve().parents[1] / "config" / "scoring.yml"


@dataclass(frozen=True)
class LinearThreshold:
    """Linear ramp from 0 at ``floor_days`` to 100 at ``cap_days``."""

    floor_days: int
    cap_days: int

    def evaluate(self, days: float) -> float:
        if days <= self.floor_days:
            return 0.0
        if days >= self.cap_days:
            return 100.0
        span = self.cap_days - self.floor_days
        return round((days - self.floor_days) / span * 100.0, 4)


@dataclass(frozen=True)
class ShareThreshold:
    """Linear ramp between two share fractions in [0, 1]."""

    share_floor: float
    share_cap: float

    def evaluate(self, share: float) -> float:
        if share <= self.share_floor:
            return 0.0
        if share >= self.share_cap:
            return 100.0
        span = self.share_cap - self.share_floor
        return round((share - self.share_floor) / span * 100.0, 4)


@dataclass(frozen=True)
class SeverityTier:
    name: str
    min_score: int
    max_score: int
    flagged: bool

    def contains(self, score: float) -> bool:
        """Inclusive match. For float scores the tier boundary aligns with
        ``score < next_tier.min_score``; :meth:`ScoringConfig.severity_for`
        is the canonical entry point and handles the ordering."""

        return self.min_score <= score <= self.max_score


@dataclass(frozen=True)
class ScoringConfig:
    methodology_version: str
    importance_weights: dict[str, float]
    fragility_weights: dict[str, float]
    release_recency: LinearThreshold
    commit_recency: LinearThreshold
    issue_responsiveness: LinearThreshold
    contributor_bus_factor: ShareThreshold
    scorecard_scale: int
    release_cadence_min_prior: int
    issue_min_eligible: int
    eligibility_min_age_months: int
    eligibility_reduced_confidence_age_months: int
    exclusion_states: tuple[str, ...]
    flagged_risk_score_min: int
    flagged_allowed_severity_tiers: tuple[str, ...]
    flagged_allowed_confidence: tuple[str, ...]
    flagged_min_signals: int
    flagged_signal_threshold: int
    importance_top_quantile: float
    severity_tiers: tuple[SeverityTier, ...]
    mapping_points: dict[str, int]
    mapping_caps: dict[str, Any]
    mapping_bucket_high_min: int
    mapping_bucket_medium_min: int
    confidence_requirements: dict[str, dict[str, Any]]

    def severity_for(self, risk_score: float) -> SeverityTier:
        """Map a 0-100 risk score to its severity tier, handling fractional scores.

        The YAML bands are inclusive integer ranges (e.g. Stable 0-39,
        Watch 40-59). We treat each band as the half-open interval
        ``[min, next_tier.min)`` so a fractional score such as 39.9 stays
        in Stable and only crosses into Watch at 40.0. The top tier is
        capped at ``[min, max]`` inclusive.
        """

        if risk_score < 0:
            raise ValueError(f"risk_score {risk_score} is negative")
        ordered = sorted(self.severity_tiers, key=lambda t: t.min_score)
        for idx, tier in enumerate(ordered):
            is_last = idx == len(ordered) - 1
            if is_last:
                return tier
            upper_exclusive = ordered[idx + 1].min_score
            if risk_score < upper_exclusive:
                return tier
        return ordered[-1]


@lru_cache(maxsize=1)
def load_scoring_config(path: Path | None = None) -> ScoringConfig:
    """Parse ``scoring.yml`` into a :class:`ScoringConfig`. Cached per path."""

    target = Path(path) if path is not None else SCORING_YAML_PATH
    with target.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    fragility = raw["fragility"]
    thresholds = fragility["thresholds"]

    tiers = tuple(
        SeverityTier(
            name=row["name"],
            min_score=int(row["min"]),
            max_score=int(row["max"]),
            flagged=bool(row["flagged"]),
        )
        for row in raw["severity_tiers"]
    )

    return ScoringConfig(
        methodology_version=str(raw["methodology_version"]),
        importance_weights={k: float(v) for k, v in raw["importance"]["weights"].items()},
        fragility_weights={k: float(v) for k, v in fragility["weights"].items()},
        release_recency=LinearThreshold(
            floor_days=int(thresholds["release_recency"]["floor_days"]),
            cap_days=int(thresholds["release_recency"]["cap_days"]),
        ),
        commit_recency=LinearThreshold(
            floor_days=int(thresholds["commit_recency"]["floor_days"]),
            cap_days=int(thresholds["commit_recency"]["cap_days"]),
        ),
        issue_responsiveness=LinearThreshold(
            floor_days=int(thresholds["issue_responsiveness"]["floor_days"]),
            cap_days=int(thresholds["issue_responsiveness"]["cap_days"]),
        ),
        contributor_bus_factor=ShareThreshold(
            share_floor=float(thresholds["contributor_bus_factor"]["share_floor"]),
            share_cap=float(thresholds["contributor_bus_factor"]["share_cap"]),
        ),
        scorecard_scale=int(thresholds["openssf_scorecard"]["scale"]),
        release_cadence_min_prior=int(
            thresholds["release_cadence_decay"]["min_prior_year_releases"]
        ),
        issue_min_eligible=int(thresholds["issue_responsiveness"]["min_eligible_issues"]),
        eligibility_min_age_months=int(raw["eligibility"]["min_age_months"]),
        eligibility_reduced_confidence_age_months=int(
            raw["eligibility"]["reduced_confidence_age_months"]
        ),
        exclusion_states=tuple(raw["exclusion_states"]),
        flagged_risk_score_min=int(raw["flagged"]["risk_score_min"]),
        flagged_allowed_severity_tiers=tuple(raw["flagged"]["allowed_severity_tiers"]),
        flagged_allowed_confidence=tuple(raw["flagged"]["allowed_confidence"]),
        flagged_min_signals=int(raw["flagged"]["min_independent_fragility_signals"]),
        flagged_signal_threshold=int(raw["flagged"]["signal_contribution_threshold"]),
        importance_top_quantile=float(raw["flagged"]["importance_top_quantile"]),
        severity_tiers=tiers,
        mapping_points={k: int(v) for k, v in raw["mapping_confidence"]["points"].items()},
        mapping_caps=dict(raw["mapping_confidence"]["caps"]),
        mapping_bucket_high_min=int(raw["mapping_confidence"]["buckets"]["high_min"]),
        mapping_bucket_medium_min=int(raw["mapping_confidence"]["buckets"]["medium_min"]),
        confidence_requirements=dict(raw["confidence_requirements"]),
    )
