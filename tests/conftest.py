"""Shared pytest setup: tier marker from the directory, and a fixed hypothesis profile."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings

settings.register_profile(
    "ci",
    max_examples=60,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("ci")

REPO = Path(__file__).resolve().parents[1]
TIERS = ("unit", "property", "integration", "e2e", "fault")


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Directory = tier: every test gets exactly the marker of its tier directory."""
    for item in items:
        rel = Path(str(item.fspath)).resolve().relative_to(REPO / "tests")
        tier = rel.parts[0]
        if tier in TIERS:
            item.add_marker(getattr(pytest.mark, tier))


@pytest.fixture
def repo() -> Path:
    return REPO
