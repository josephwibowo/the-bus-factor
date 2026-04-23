"""Baseline smoke test so the test runner has something to execute in the scaffold."""

from __future__ import annotations

from pathlib import Path


def test_repo_root_files_exist() -> None:
    """The minimum competition-critical artifacts are present in the scaffold."""
    root = Path(__file__).resolve().parent.parent
    required = [
        "README.md",
        "AGENTS.md",
        "pyproject.toml",
        ".python-version",
        ".nvmrc",
        ".bruin.yml.example",
        "pipeline/pipeline.yml",
        "pipeline/config/scoring.yml",
        "docs/init.md",
        "launch/submission-checklist.md",
    ]
    missing = [p for p in required if not (root / p).exists()]
    assert not missing, f"Missing scaffold files: {missing}"
