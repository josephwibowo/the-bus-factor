"""Tests for the scoring config loader."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipeline.lib.config import (
    LinearThreshold,
    ShareThreshold,
    load_scoring_config,
)

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_YAML = ROOT / "pipeline" / "pipeline.yml"


def _pipeline_var_defaults() -> dict[str, object]:
    raw = yaml.safe_load(PIPELINE_YAML.read_text(encoding="utf-8"))
    variables = raw.get("variables", {})
    return {name: meta.get("default") for name, meta in variables.items()}


def test_scoring_config_loads() -> None:
    config = load_scoring_config()
    assert config.methodology_version == "v0.5.0"
    # Weights sum to 1.0 (within fp tolerance) for both pillars.
    assert abs(sum(config.importance_weights.values()) - 1.0) < 1e-9
    assert abs(sum(config.fragility_weights.values()) - 1.0) < 1e-9


def test_linear_threshold_evaluates_monotonic() -> None:
    ramp = LinearThreshold(floor_days=30, cap_days=365)
    assert ramp.evaluate(10) == 0.0
    assert ramp.evaluate(30) == 0.0
    assert ramp.evaluate(365) == 100.0
    assert ramp.evaluate(9999) == 100.0
    mid = ramp.evaluate(197)
    assert 49.0 < mid < 51.0


def test_share_threshold_evaluates() -> None:
    share = ShareThreshold(share_floor=0.30, share_cap=0.90)
    assert share.evaluate(0.20) == 0.0
    assert share.evaluate(0.90) == 100.0
    mid = share.evaluate(0.60)
    assert 49.0 < mid < 51.0


@pytest.mark.parametrize(
    "risk,expected",
    [
        (0.0, "Stable"),
        (14.0, "Stable"),
        (14.9, "Stable"),
        (15.0, "Watch"),
        (24.5, "Watch"),
        (25.0, "Elevated"),
        (29.9, "Elevated"),
        (30.0, "High"),
        (49.9, "High"),
        (50.0, "Critical"),
        (100.0, "Critical"),
    ],
)
def test_severity_for_boundary_cases(risk: float, expected: str) -> None:
    config = load_scoring_config()
    assert config.severity_for(risk).name == expected


def test_severity_for_negative_raises() -> None:
    config = load_scoring_config()
    with pytest.raises(ValueError):
        config.severity_for(-0.1)


def test_pipeline_methodology_var_matches_scoring_config() -> None:
    config = load_scoring_config()
    defaults = _pipeline_var_defaults()
    assert str(defaults["methodology_version"]) == config.methodology_version


def test_pipeline_scoring_var_defaults_match_scoring_config() -> None:
    config = load_scoring_config()
    defaults = _pipeline_var_defaults()
    severity_by_name = {tier.name: tier.max_score for tier in config.severity_tiers}

    expected_float = {
        "importance_weight_dependency_reach": config.importance_weights["dependency_reach"],
        "importance_weight_download_volume": config.importance_weights["download_volume"],
        "importance_weight_security_exposure": config.importance_weights["security_exposure"],
        "fragility_weight_release_recency": config.fragility_weights["release_recency"],
        "fragility_weight_commit_recency": config.fragility_weights["commit_recency"],
        "fragility_weight_release_cadence_decay": config.fragility_weights["release_cadence_decay"],
        "fragility_weight_issue_responsiveness": config.fragility_weights["issue_responsiveness"],
        "fragility_weight_all_time_contribution_concentration": config.fragility_weights[
            "all_time_contribution_concentration"
        ],
        "fragility_weight_recent_commit_concentration_365d": config.fragility_weights[
            "recent_commit_concentration_365d"
        ],
        "fragility_weight_openssf_scorecard": config.fragility_weights["openssf_scorecard"],
        "threshold_all_time_contributor_share_floor": config.all_time_contribution_concentration.share_floor,
        "threshold_all_time_contributor_share_cap": config.all_time_contribution_concentration.share_cap,
        "threshold_recent_commit_contributor_share_floor": config.recent_commit_concentration_365d.share_floor,
        "threshold_recent_commit_contributor_share_cap": config.recent_commit_concentration_365d.share_cap,
        "threshold_scorecard_scale": float(config.scorecard_scale),
        "flagged_importance_top_quantile": config.importance_top_quantile,
    }
    expected_int = {
        "threshold_release_recency_floor_days": config.release_recency.floor_days,
        "threshold_release_recency_cap_days": config.release_recency.cap_days,
        "threshold_commit_recency_floor_days": config.commit_recency.floor_days,
        "threshold_commit_recency_cap_days": config.commit_recency.cap_days,
        "threshold_release_cadence_min_prior_releases": config.release_cadence_min_prior,
        "threshold_issue_min_eligible_issues": config.issue_min_eligible,
        "threshold_issue_floor_days": config.issue_responsiveness.floor_days,
        "threshold_issue_cap_days": config.issue_responsiveness.cap_days,
        "flagged_risk_score_min": config.flagged_risk_score_min,
        "flagged_signal_contribution_threshold": config.flagged_signal_threshold,
        "flagged_min_independent_fragility_signals": config.flagged_min_signals,
        "severity_stable_max": severity_by_name["Stable"],
        "severity_watch_max": severity_by_name["Watch"],
        "severity_elevated_max": severity_by_name["Elevated"],
        "severity_high_max": severity_by_name["High"],
    }

    for key, expected in expected_float.items():
        assert float(defaults[key]) == pytest.approx(expected)
    for key, expected in expected_int.items():
        assert int(defaults[key]) == expected
