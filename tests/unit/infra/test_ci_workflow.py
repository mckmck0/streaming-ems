"""The GitHub Actions workflow of spec 010 (`.github/workflows/ci.yml`), read as YAML.

AC-010-28: `make ci` on every push to `main` and every pull request, on a fixed runner image,
every action pinned by commit SHA, read-only default permissions, uv pinned, no secret.
AC-010-29: a `badge` job, after `ci` and only for a push to `main`, holds `contents: write` and
pushes `coverage.json` to the orphan branch `badges` with plain git.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

WORKFLOW = Path(".github") / "workflows" / "ci.yml"
RUNNER = "ubuntu-26.04-arm"
PINNED_ACTION = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$")


@pytest.fixture
def workflow(repo: Path) -> dict[str, Any]:
    return YAML(typ="safe").load((repo / WORKFLOW).read_text())


def _job(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    jobs = workflow.get("jobs") or {}
    assert name in jobs, f"no job {name!r} in {WORKFLOW}"
    return jobs[name]


def _steps(*jobs: dict[str, Any]) -> list[dict[str, Any]]:
    return [step for job in jobs for step in job.get("steps") or []]


def test_ac_010_28_runs_on_push_to_main_and_pull_requests(workflow: dict[str, Any]) -> None:
    triggers = workflow["on"]
    assert triggers["push"]["branches"] == ["main"]
    assert "pull_request" in triggers


def test_ac_010_28_default_permissions_read_only(workflow: dict[str, Any]) -> None:
    assert workflow.get("permissions") == {"contents": "read"}


def test_ac_010_28_jobs_on_fixed_runner(workflow: dict[str, Any]) -> None:
    _job(workflow, "ci")
    _job(workflow, "badge")
    for name, job in workflow["jobs"].items():
        assert job.get("runs-on") == RUNNER, name


def test_ac_010_28_actions_pinned_by_commit_sha(workflow: dict[str, Any]) -> None:
    for step in _steps(*workflow["jobs"].values()):
        if "uses" in step:
            assert PINNED_ACTION.match(str(step["uses"])), step["uses"]


def test_ac_010_28_ci_job_runs_make_ci(workflow: dict[str, Any]) -> None:
    assert any("make ci" in str(step.get("run", "")) for step in _steps(_job(workflow, "ci")))


def test_ac_010_28_ci_job_cannot_write(workflow: dict[str, Any]) -> None:
    permissions = _job(workflow, "ci").get("permissions") or {}
    values = permissions.values() if isinstance(permissions, dict) else [permissions]
    assert not any("write" in str(value) for value in values)


def test_ac_010_28_setup_uv_pins_uv(workflow: dict[str, Any]) -> None:
    setup_uv = [
        step
        for step in _steps(*workflow["jobs"].values())
        if str(step.get("uses", "")).startswith("astral-sh/setup-uv@")
    ]
    assert setup_uv, "no step uses astral-sh/setup-uv"
    for step in setup_uv:
        assert (step.get("with") or {}).get("version") == "0.13.8"


def test_ac_010_28_no_secrets(repo: Path) -> None:
    assert "secrets." not in (repo / WORKFLOW).read_text()


def test_ac_010_29_badge_job_after_ci_on_push_to_main(workflow: dict[str, Any]) -> None:
    badge = _job(workflow, "badge")
    needs = badge.get("needs") or []
    assert "ci" in ([needs] if isinstance(needs, str) else needs)
    condition = str(badge.get("if", ""))
    assert "push" in condition
    assert "refs/heads/main" in condition


def test_ac_010_29_badge_job_holds_contents_write(workflow: dict[str, Any]) -> None:
    assert _job(workflow, "badge").get("permissions") == {"contents": "write"}


def test_ac_010_29_badge_job_force_pushes_orphan_branch(workflow: dict[str, Any]) -> None:
    script = "\n".join(str(step.get("run", "")) for step in _steps(_job(workflow, "badge")))
    assert "--orphan" in script
    assert "coverage.json" in script
    assert "push --force" in script
