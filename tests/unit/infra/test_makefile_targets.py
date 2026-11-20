"""Makefile targets of spec 010, read as text (nothing is run).

AC-010-03: `register` posts the deployment with `force: $(FORCE)`, and only after that starts
the ingress profile.
AC-010-06: `down` and `reset` include the ingress profile and remove every container labelled
`ems.role=ingress`.
AC-010-07: `dev` serves `ems.actors.app` on 127.0.0.1:9080.
AC-010-18, AC-010-23: `measure` and `spike` run their jobs.
AC-010-27: every tier target writes its coverage file under `logs/`; `report-coverage` combines
them into `logs/coverage.xml`.
The targets the spec adds are declared `.PHONY`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

TARGET = re.compile(r"^([A-Za-z0-9_-]+):(?!:?=)")
LOGS_DIR_LINE = re.compile(r"^LOGS_DIR\s*:=\s*logs\s*$")
NEW_TARGETS = {"register", "dev", "test-e2e", "test-fault", "report-coverage", "spike", "measure"}
TIER_COVERAGE = [
    ("test-unit", "unit"),
    ("test-integration", "integration"),
    ("test-e2e", "e2e"),
    ("test-fault", "fault"),
]


def _lines(repo: Path) -> list[str]:
    """The Makefile's logical lines: a line ending in a backslash continues on the next one."""
    lines: list[str] = []
    pending = ""
    for raw in (repo / "Makefile").read_text().splitlines():
        if raw.endswith("\\"):
            pending += raw[:-1] + " "
            continue
        lines.append(pending + raw)
        pending = ""
    if pending:
        lines.append(pending)
    return lines


def _parse(repo: Path) -> tuple[dict[str, list[str]], set[str]]:
    """Recipes by target and the `.PHONY` names.

    A target line is `name:` at the start of a line (not `.PHONY`, not a `:=` assignment); its
    recipe is the lines after it that start with a TAB. Comment-only and blank lines are skipped,
    as make skips them between recipe lines, so the commented-out planned targets never count.
    """
    recipes: dict[str, list[str]] = {}
    phony: set[str] = set()
    recipe: list[str] | None = None
    for line in _lines(repo):
        if line.startswith("#") or not line.strip():
            continue
        if line.startswith("\t"):
            if recipe is not None:
                recipe.append(line.strip())
            continue
        recipe = None
        if line.startswith(".PHONY:"):
            phony.update(line.removeprefix(".PHONY:").split())
        elif match := TARGET.match(line):
            recipe = recipes[match.group(1)] = []
    return recipes, phony


def _recipe(repo: Path, target: str) -> list[str]:
    recipes, _ = _parse(repo)
    assert target in recipes, f"no target {target!r} in the Makefile"
    return recipes[target]


@pytest.mark.parametrize("target", ["down", "reset"])
def test_ac_010_06_stop_targets_remove_ingress_containers(repo: Path, target: str) -> None:
    recipe = "\n".join(_recipe(repo, target))
    assert "--profile ingress" in recipe
    assert "label=ems.role=ingress" in recipe


def test_ac_010_03_register_posts_then_starts_ingress(repo: Path) -> None:
    lines = _recipe(repo, "register")
    posts = [
        i for i, line in enumerate(lines) if "/deployments" in line and '"force": $(FORCE)' in line
    ]
    assert posts, 'no recipe line posts to /deployments with "force": $(FORCE)'
    assert any("--profile ingress up -d --wait" in line for line in lines[posts[0] + 1 :])


def test_ac_010_07_dev_serves_app_on_loopback_9080(repo: Path) -> None:
    recipe = "\n".join(_recipe(repo, "dev"))
    assert "hypercorn ems.actors.app:app --bind 127.0.0.1:9080" in recipe


def test_ac_010_27_logs_dir_is_logs(repo: Path) -> None:
    lines = _lines(repo)
    assert any(LOGS_DIR_LINE.match(line) for line in lines), "no line `LOGS_DIR := logs`"


@pytest.mark.parametrize(("target", "tier"), TIER_COVERAGE)
def test_ac_010_27_tier_writes_coverage_under_logs(repo: Path, target: str, tier: str) -> None:
    recipe = "\n".join(_recipe(repo, target))
    assert f"COVERAGE_FILE=$(LOGS_DIR)/.coverage.{tier}" in recipe
    assert "--cov=ems" in recipe


def test_ac_010_27_report_coverage_combines_into_logs(repo: Path) -> None:
    recipe = "\n".join(_recipe(repo, "report-coverage"))
    assert "coverage combine" in recipe
    assert "coverage xml" in recipe
    assert "$(LOGS_DIR)/coverage.xml" in recipe


def test_ac_010_23_spike_target_runs_spike_job(repo: Path) -> None:
    assert "jobs/spike_repro.py" in "\n".join(_recipe(repo, "spike"))


def test_ac_010_18_measure_target_runs_measure_job(repo: Path) -> None:
    assert "jobs/ingress_measure.py" in "\n".join(_recipe(repo, "measure"))


def test_spec_010_new_targets_are_phony(repo: Path) -> None:
    _, phony = _parse(repo)
    assert sorted(NEW_TARGETS - phony) == []
