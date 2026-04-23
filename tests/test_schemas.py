"""Tests for the Pydantic public-bundle schemas."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.lib.schemas import (
    PUBLIC_BUNDLE_SCHEMAS,
    Analysis,
    Coverage,
    Leaderboard,
    Metadata,
    Packages,
    Positioning,
    Sources,
    Weekly,
    slugify,
)

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DATA = ROOT / "public-data"


@pytest.mark.parametrize(
    "ecosystem,name,expected",
    [
        ("npm", "left-pad", "npm-left-pad"),
        ("npm", "@types/node", "npm-types__node"),
        ("pypi", "Requests Oauth", "pypi-requests-oauth"),
        ("npm", "@scope/Pkg", "npm-scope__pkg"),
    ],
)
def test_slugify_normalises(ecosystem: str, name: str, expected: str) -> None:
    assert slugify(ecosystem, name) == expected


def test_schema_index_contains_eight_stems() -> None:
    expected = {
        "metadata",
        "leaderboard",
        "packages",
        "weekly",
        "coverage",
        "sources",
        "analysis",
        "positioning",
    }
    assert set(PUBLIC_BUNDLE_SCHEMAS.keys()) == expected


@pytest.mark.parametrize(
    "stem,model",
    [
        ("metadata", Metadata),
        ("leaderboard", Leaderboard),
        ("packages", Packages),
        ("weekly", Weekly),
        ("coverage", Coverage),
        ("sources", Sources),
        ("analysis", Analysis),
        ("positioning", Positioning),
    ],
)
def test_exported_bundle_validates(stem: str, model) -> None:  # type: ignore[no-untyped-def]
    path = PUBLIC_DATA / f"{stem}.json"
    if not path.exists():
        pytest.skip(
            f"{path.name} not exported yet - run `bruin run -e fixture pipeline/pipeline.yml`"
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    model.model_validate(data)


def test_schemas_round_trip() -> None:
    sample = Metadata(
        snapshot_week="2026-04-20",
        snapshot_week_label="2026-W17",
        methodology_version="v0.1.0",
        generated_at="2026-04-20T12:00:00Z",  # type: ignore[arg-type]
        ecosystems_covered=["npm", "pypi"],
        package_counts={"npm": 24, "pypi": 24},
        sources=[],
    )
    round_tripped = Metadata.model_validate_json(sample.model_dump_json())
    assert round_tripped.snapshot_week == sample.snapshot_week
