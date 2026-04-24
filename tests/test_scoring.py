"""Tests for the pure-Python scoring helpers."""

from __future__ import annotations

from datetime import date

import pytest

from pipeline.lib.config import load_scoring_config
from pipeline.lib.scoring import (
    FragilityBreakdown,
    FragilityComponent,
    FragilityInputs,
    ImportanceInputs,
    fragility_score,
    importance_score,
    independent_fragility_signals,
    is_flagged,
    log_percentile,
    risk_score,
    tier_name,
)

AS_OF = date(2026, 4, 20)


@pytest.fixture(scope="module")
def config():  # type: ignore[no-untyped-def]
    return load_scoring_config()


def _healthy_inputs() -> FragilityInputs:
    """Everything is fresh; fragility score should be ~0."""

    return FragilityInputs(
        latest_release_date=date(2026, 4, 1),  # <30 days old -> 0
        last_commit_date=date(2026, 4, 15),  # <30 days old -> 0
        releases_last_365d=18,
        releases_prior_365d=20,  # 10% decay -> cadence component ~10
        issues_opened_last_180d=40,
        median_response_days=3.0,  # <7 -> 0
        top_contributor_share_all_time=0.20,  # <0.30 -> 0
        top_contributor_share_365d=0.20,  # <0.30 -> 0
        scorecard_aggregate=9.0,  # 100-90=10
    )


def test_healthy_package_low_fragility(config) -> None:  # type: ignore[no-untyped-def]
    breakdown = fragility_score(_healthy_inputs(), as_of=AS_OF, config=config)
    assert 0.0 <= breakdown.score < 15.0


def test_stale_package_high_fragility(config) -> None:  # type: ignore[no-untyped-def]
    stale = FragilityInputs(
        latest_release_date=date(2024, 1, 1),  # >540 days -> 100
        last_commit_date=date(2024, 6, 1),  # >365 days -> 100
        releases_last_365d=0,
        releases_prior_365d=8,  # 100% decay -> 100
        issues_opened_last_180d=20,
        median_response_days=100.0,  # >=90 -> 100
        top_contributor_share_all_time=0.95,  # >=0.90 -> 100
        top_contributor_share_365d=0.95,  # >=0.90 -> 100
        scorecard_aggregate=1.0,  # 100-10 = 90
    )
    breakdown = fragility_score(stale, as_of=AS_OF, config=config)
    assert breakdown.score > 90.0


def test_importance_score_weighted(config) -> None:  # type: ignore[no-untyped-def]
    inputs = ImportanceInputs(
        log_dependency_reach_percentile=100.0,
        log_download_volume_percentile=100.0,
        log_security_exposure_percentile=100.0,
    )
    assert importance_score(inputs, config) == pytest.approx(100.0)
    zero = ImportanceInputs(0.0, 0.0, 0.0)
    assert importance_score(zero, config) == 0.0


def test_importance_score_reweights_missing_dependency_reach(config) -> None:  # type: ignore[no-untyped-def]
    inputs = ImportanceInputs(
        log_dependency_reach_percentile=None,
        log_download_volume_percentile=100.0,
        log_security_exposure_percentile=0.0,
    )
    expected = (
        100.0
        * config.importance_weights["download_volume"]
        / (
            config.importance_weights["download_volume"]
            + config.importance_weights["security_exposure"]
        )
    )
    assert importance_score(inputs, config) == pytest.approx(expected)


def test_risk_score_is_product_over_100() -> None:
    assert risk_score(80.0, 90.0) == pytest.approx(72.0)
    assert risk_score(100.0, 100.0) == pytest.approx(100.0)
    assert risk_score(0.0, 100.0) == 0.0


def test_log_percentile_extremes() -> None:
    values = [1.0, 10.0, 100.0, 1000.0, 10_000.0]
    assert log_percentile(values, 1.0) == 0.0
    assert log_percentile(values, 10_000.0) == 100.0
    # Middle of log-space is 100.
    mid = log_percentile(values, 100.0)
    assert 40.0 < mid < 60.0


def test_independent_fragility_signals_excludes_release_recency_alone(config) -> None:  # type: ignore[no-untyped-def]
    breakdown = FragilityBreakdown(
        components=(
            FragilityComponent("release_recency", 80.0),
            FragilityComponent("commit_recency", 10.0),
            FragilityComponent("release_cadence_decay", 0.0),
            FragilityComponent("issue_responsiveness", 0.0),
            FragilityComponent("all_time_contribution_concentration", 0.0),
            FragilityComponent("recent_commit_concentration_365d", 0.0),
            FragilityComponent("openssf_scorecard", 0.0),
        ),
        score=20.0,
    )
    assert independent_fragility_signals(breakdown, config) == 0


def test_tier_name_matches_config(config) -> None:  # type: ignore[no-untyped-def]
    assert tier_name(35.0, config) == "High"
    assert tier_name(60.0, config) == "Critical"
    assert tier_name(10.0, config) == "Stable"


def test_is_flagged_happy_path(config) -> None:  # type: ignore[no-untyped-def]
    breakdown = FragilityBreakdown(
        components=(
            FragilityComponent("release_recency", 80.0),
            FragilityComponent("commit_recency", 70.0),
            FragilityComponent("release_cadence_decay", 50.0),
            FragilityComponent("issue_responsiveness", 0.0),
            FragilityComponent("all_time_contribution_concentration", 45.0),
            FragilityComponent("recent_commit_concentration_365d", 0.0),
            FragilityComponent("openssf_scorecard", 20.0),
        ),
        score=60.0,
    )
    decision = is_flagged(
        eligible=True,
        risk=35.0,
        tier="High",
        confidence="high",
        fragility=breakdown,
        importance_rank_percentile=95.0,
        config=config,
    )
    assert decision.flagged is True
    assert decision.reason_if_not is None


@pytest.mark.parametrize(
    "override,expected_reason",
    [
        ({"eligible": False}, "not_eligible"),
        ({"risk": 20.0}, "risk_below_min"),
        ({"tier": "Elevated"}, "tier_not_allowed"),
        ({"confidence": "low"}, "confidence_too_low"),
        ({"importance_rank_percentile": 10.0}, "importance_below_gate"),
    ],
)
def test_is_flagged_rejections(config, override, expected_reason) -> None:  # type: ignore[no-untyped-def]
    breakdown = FragilityBreakdown(
        components=(
            FragilityComponent("commit_recency", 70.0),
            FragilityComponent("release_cadence_decay", 60.0),
            FragilityComponent("release_recency", 0.0),
            FragilityComponent("issue_responsiveness", 0.0),
            FragilityComponent("all_time_contribution_concentration", 0.0),
            FragilityComponent("recent_commit_concentration_365d", 0.0),
            FragilityComponent("openssf_scorecard", 0.0),
        ),
        score=55.0,
    )
    defaults = {
        "eligible": True,
        "risk": 35.0,
        "tier": "High",
        "confidence": "high",
        "fragility": breakdown,
        "importance_rank_percentile": 95.0,
        "config": config,
    }
    defaults.update(override)
    decision = is_flagged(**defaults)
    assert decision.flagged is False
    assert decision.reason_if_not == expected_reason


def test_is_flagged_requires_non_release_signal(config) -> None:  # type: ignore[no-untyped-def]
    """Release_recency alone is never sufficient to flag (paired-signal-only)."""

    breakdown = FragilityBreakdown(
        components=(
            FragilityComponent("release_recency", 90.0),  # paired-only
            FragilityComponent("commit_recency", 10.0),
            FragilityComponent("release_cadence_decay", 0.0),
            FragilityComponent("issue_responsiveness", 0.0),
            FragilityComponent("all_time_contribution_concentration", 0.0),
            FragilityComponent("recent_commit_concentration_365d", 0.0),
            FragilityComponent("openssf_scorecard", 0.0),
        ),
        score=25.0,
    )
    decision = is_flagged(
        eligible=True,
        risk=82.0,
        tier="High",
        confidence="high",
        fragility=breakdown,
        importance_rank_percentile=95.0,
        config=config,
    )
    assert decision.flagged is False
    assert decision.reason_if_not == "insufficient_fragility_signals"


def test_is_flagged_release_plus_commit_is_two_signals(config) -> None:  # type: ignore[no-untyped-def]
    """release_recency paired with another independent signal satisfies the gate."""

    breakdown = FragilityBreakdown(
        components=(
            FragilityComponent("commit_recency", 70.0),
            FragilityComponent("release_recency", 90.0),
            FragilityComponent("release_cadence_decay", 0.0),
            FragilityComponent("issue_responsiveness", 0.0),
            FragilityComponent("all_time_contribution_concentration", 0.0),
            FragilityComponent("recent_commit_concentration_365d", 0.0),
            FragilityComponent("openssf_scorecard", 0.0),
        ),
        score=55.0,
    )
    decision = is_flagged(
        eligible=True,
        risk=82.0,
        tier="High",
        confidence="high",
        fragility=breakdown,
        importance_rank_percentile=95.0,
        config=config,
    )
    assert decision.flagged is True
