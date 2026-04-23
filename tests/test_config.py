"""Tests for the scoring config loader."""

from __future__ import annotations

import pytest

from pipeline.lib.config import (
    LinearThreshold,
    ShareThreshold,
    load_scoring_config,
)


def test_scoring_config_loads() -> None:
    config = load_scoring_config()
    assert config.methodology_version == "v0.1.0"
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
        (39.0, "Stable"),
        (39.9, "Stable"),
        (40.0, "Watch"),
        (59.5, "Watch"),
        (60.0, "Elevated"),
        (74.9, "Elevated"),
        (75.0, "High"),
        (89.9, "High"),
        (90.0, "Critical"),
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
