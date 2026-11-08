"""The Stop hook that asks for a PR once every task of a spec branch is checked (.claude/hooks)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / ".claude" / "hooks" / "spec-pr-check.sh"
BRANCH = "042-demo"

pytestmark = pytest.mark.skipif(
    shutil.which("jq") is None or shutil.which("git") is None, reason="the hook needs jq and git"
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q", "-b", BRANCH)
    _git(
        tmp_path,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.com",
        "commit",
        "-q",
        "--allow-empty",
        "-m",
        "init",
    )
    (tmp_path / "specs" / BRANCH).mkdir(parents=True)
    _tasks(tmp_path, "- [x] T01 tests\n- [x] T02 code\n")
    return tmp_path


def _tasks(repo: Path, text: str) -> None:
    (repo / "specs" / BRANCH / "tasks.md").write_text(f"# Tasks\n\n{text}")


def _run(repo: Path, payload: dict[str, object] | None = None) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    result = subprocess.run(
        [str(HOOK)],
        input=json.dumps(payload or {"stop_hook_active": False}),
        capture_output=True,
        text=True,
        cwd=repo,
        env={**env, "CLAUDE_PROJECT_DIR": str(repo)},
        check=True,
    )
    return result.stdout.strip()


def test_done_spec_without_pr_blocks_the_stop(repo: Path) -> None:
    out = json.loads(_run(repo))
    assert out["decision"] == "block"
    assert BRANCH in out["reason"] and "ems-pr" in out["reason"]


def test_a_continued_stop_is_let_through(repo: Path) -> None:
    assert _run(repo, {"stop_hook_active": True}) == ""


@pytest.mark.parametrize("tasks", ["- [x] T01 tests\n- [ ] T02 code\n", "no tasks yet\n"])
def test_unfinished_spec_is_quiet(repo: Path, tasks: str) -> None:
    _tasks(repo, tasks)
    assert _run(repo) == ""


@pytest.mark.parametrize("outcome", ["https://github.com/o/r/pull/1", "manual", "skip"])
def test_recorded_outcome_is_quiet(repo: Path, outcome: str) -> None:
    (repo / ".git" / "ems-pr").mkdir()
    (repo / ".git" / "ems-pr" / BRANCH).write_text(outcome)
    assert _run(repo) == ""


def test_other_branches_are_quiet(repo: Path) -> None:
    _git(repo, "switch", "-q", "-c", "main")
    assert _run(repo) == ""
