"""Pure-Python mirror of the SQL scoring logic.

Kept purely for (a) unit tests that hold the SQL asset accountable to the
scoring config, (b) the Pillow-based weekly card which needs to enumerate
top findings without re-reading marts, and (c) any ad-hoc scripts. The
authoritative scoring computation runs inside Bruin marts.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from math import log

from .config import ScoringConfig


@dataclass(frozen=True)
class FragilityInputs:
    """Raw signals per package before per-component transformation."""

    latest_release_date: date | None
    last_commit_date: date | None
    releases_last_365d: int
    releases_prior_365d: int
    issues_opened_last_180d: int
    median_response_days: float | None
    top_contributor_share_365d: float | None
    scorecard_aggregate: float | None


@dataclass(frozen=True)
class FragilityComponent:
    name: str
    contribution: float  # 0 - 100


@dataclass(frozen=True)
class FragilityBreakdown:
    components: tuple[FragilityComponent, ...]
    score: float  # 0 - 100

    def components_at_or_above(self, threshold: float) -> tuple[FragilityComponent, ...]:
        return tuple(c for c in self.components if c.contribution >= threshold)


def fragility_score(
    inputs: FragilityInputs,
    *,
    as_of: date,
    config: ScoringConfig,
) -> FragilityBreakdown:
    """Compute the 0-100 fragility score mirroring the mart SQL."""

    components: list[FragilityComponent] = []
    weights = config.fragility_weights

    release_days = _delta_days(as_of, inputs.latest_release_date)
    components.append(
        FragilityComponent(
            "release_recency",
            _linear_or_zero(release_days, config.release_recency.evaluate),
        )
    )

    commit_days = _delta_days(as_of, inputs.last_commit_date)
    components.append(
        FragilityComponent(
            "commit_recency",
            _linear_or_zero(commit_days, config.commit_recency.evaluate),
        )
    )

    cadence = _cadence_decay(
        last=inputs.releases_last_365d,
        prior=inputs.releases_prior_365d,
        min_prior=config.release_cadence_min_prior,
    )
    components.append(FragilityComponent("release_cadence_decay", cadence))

    if inputs.issues_opened_last_180d >= config.issue_min_eligible and (
        inputs.median_response_days is not None
    ):
        issue_score = config.issue_responsiveness.evaluate(inputs.median_response_days)
    else:
        issue_score = 0.0
    components.append(FragilityComponent("issue_responsiveness", issue_score))

    bus_factor = (
        config.contributor_bus_factor.evaluate(inputs.top_contributor_share_365d)
        if inputs.top_contributor_share_365d is not None
        else 0.0
    )
    components.append(FragilityComponent("contributor_bus_factor", bus_factor))

    scorecard = (
        max(0.0, min(100.0, 100.0 - (inputs.scorecard_aggregate * config.scorecard_scale)))
        if inputs.scorecard_aggregate is not None
        else 0.0
    )
    components.append(FragilityComponent("openssf_scorecard", scorecard))

    total = 0.0
    for component in components:
        total += component.contribution * weights[component.name]

    return FragilityBreakdown(components=tuple(components), score=round(total, 4))


def _delta_days(as_of: date, target: date | None) -> float | None:
    if target is None:
        return None
    return float((as_of - target).days)


def _linear_or_zero(days: float | None, fn: Callable[[float], float]) -> float:
    if days is None or days < 0:
        return 0.0
    return float(fn(days))


def _cadence_decay(last: int, prior: int, min_prior: int) -> float:
    if prior < min_prior:
        return 0.0
    if last >= prior:
        return 0.0
    return round(100.0 * (1.0 - (last / prior)), 4)


@dataclass(frozen=True)
class ImportanceInputs:
    log_dependency_reach_percentile: float | None  # 0 - 100
    log_download_volume_percentile: float | None  # 0 - 100
    log_security_exposure_percentile: float | None  # 0 - 100


def importance_score(inputs: ImportanceInputs, config: ScoringConfig) -> float:
    """Weighted sum of available log-scaled ecosystem percentiles.

    Missing signals are excluded from the denominator so unknown dependency
    reach does not become false zero reach.
    """

    w = config.importance_weights
    pairs = (
        ("dependency_reach", inputs.log_dependency_reach_percentile),
        ("download_volume", inputs.log_download_volume_percentile),
        ("security_exposure", inputs.log_security_exposure_percentile),
    )
    weight_total = sum(w[name] for name, value in pairs if value is not None)
    if weight_total == 0:
        return 0.0
    total = sum(w[name] * value for name, value in pairs if value is not None)
    return round(total / weight_total, 4)


def risk_score(importance: float, fragility: float) -> float:
    return round((importance * fragility) / 100.0, 4)


def log_percentile(values: Sequence[float], value: float) -> float:
    """Return the 0-100 percentile rank of ``log(value)`` among ``values``.

    Uses the inclusive definition (``rank - 1) / (n - 1)`` so the smallest
    element scores 0 and the largest scores 100. Ties share the mean rank,
    matching ``PERCENT_RANK()`` behaviour on the mart side.
    """

    if not values:
        raise ValueError("log_percentile requires at least one value")
    if len(values) == 1:
        return 100.0 if value >= values[0] else 0.0

    log_values = sorted(_log_safe(v) for v in values)
    lv = _log_safe(value)

    # Rank = number of elements strictly less than lv, plus mean tie adjustment.
    strictly_less = sum(1 for v in log_values if v < lv)
    ties = sum(1 for v in log_values if v == lv)
    rank_zero_based = strictly_less + (0 if ties == 0 else (ties - 1) / 2.0)
    return round(rank_zero_based / (len(log_values) - 1) * 100.0, 4)


def _log_safe(value: float) -> float:
    if value <= 0:
        return 0.0
    return log(1.0 + value)


def independent_fragility_signals(
    breakdown: FragilityBreakdown,
    config: ScoringConfig,
    *,
    paired_signal_only_overrides: Iterable[str] = ("release_recency",),
) -> int:
    """Number of fragility components at or above the flagged threshold.

    ``release_recency`` is declared ``paired_signal_only`` in the config,
    matching the spec's requirement that it never flags a package alone.
    When the only signal above threshold is release_recency, the count is
    clamped to 0.
    """

    qualifying = breakdown.components_at_or_above(config.flagged_signal_threshold)
    names = {c.name for c in qualifying}
    paired_only = set(paired_signal_only_overrides)
    if names.issubset(paired_only):
        return 0
    return len(qualifying)


def tier_name(risk: float, config: ScoringConfig) -> str:
    return config.severity_for(risk).name


@dataclass(frozen=True)
class FlaggedDecision:
    flagged: bool
    reason_if_not: str | None


def is_flagged(
    *,
    eligible: bool,
    risk: float,
    tier: str,
    confidence: str,
    fragility: FragilityBreakdown,
    importance_rank_percentile: float,  # 0 - 100, 100 = smallest
    config: ScoringConfig,
) -> FlaggedDecision:
    """Apply the flagged definition short-circuits in order.

    ``importance_rank_percentile`` is oriented so the largest importance is
    100 (top of ecosystem). Packages below ``100 * (1 - quantile)`` fail the
    importance gate.
    """

    if not eligible:
        return FlaggedDecision(False, "not_eligible")
    if risk < config.flagged_risk_score_min:
        return FlaggedDecision(False, "risk_below_min")
    if tier not in config.flagged_allowed_severity_tiers:
        return FlaggedDecision(False, "tier_not_allowed")
    if confidence not in config.flagged_allowed_confidence:
        return FlaggedDecision(False, "confidence_too_low")
    if independent_fragility_signals(fragility, config) < config.flagged_min_signals:
        return FlaggedDecision(False, "insufficient_fragility_signals")
    importance_gate = (1.0 - config.importance_top_quantile) * 100.0
    if importance_rank_percentile < importance_gate:
        return FlaggedDecision(False, "importance_below_gate")
    return FlaggedDecision(True, None)
