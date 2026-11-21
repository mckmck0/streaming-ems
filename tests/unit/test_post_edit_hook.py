"""AC-011-06 (spec 011, T09): the PostToolUse hook ``.claude/hooks/post-edit.sh`` flags a Restate
import in the pure code, ``src/ems/assembler/`` as well as ``src/ems/domain/``: it exits 2 and its
message names the file and the rule (principle 2). A clean file, and a Restate import where it
belongs (``src/ems/actors/``), exit 0.

The hook runs against a temporary project (``CLAUDE_PROJECT_DIR``), so it never touches the repo.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / ".claude" / "hooks" / "post-edit.sh"
RESTATE_IMPORTS = (
    "import restate\n",
    "from restate import Context\n",
    "from restate.serde import Serde\n",
)
CLEAN = "from __future__ import annotations\n\nimport math\n\nX = math.pi\n"

pytestmark = pytest.mark.skipif(shutil.which("jq") is None, reason="the hook needs jq")


def _edit(project: Path, rel: str, text: str) -> subprocess.CompletedProcess[str]:
    """Write ``text`` to ``project/rel`` and run the hook as Claude Code does after an edit."""
    path = project / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    payload = {"tool_name": "Write", "tool_input": {"file_path": str(path)}}
    return subprocess.run(
        [str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=project,
        env={**os.environ, "CLAUDE_PROJECT_DIR": str(project)},
        check=False,
    )


@pytest.mark.parametrize("line", RESTATE_IMPORTS)
@pytest.mark.parametrize("folder", ["src/ems/assembler", "src/ems/domain"])
def test_ac_011_06_a_restate_import_in_pure_code_is_flagged(
    tmp_path: Path, folder: str, line: str
) -> None:
    result = _edit(tmp_path, f"{folder}/join.py", f"from __future__ import annotations\n\n{line}")
    assert result.returncode == 2, result.stderr
    assert "Restate import" in result.stderr
    assert f"{folder}/join.py" in result.stderr
    assert "principle 2" in result.stderr


@pytest.mark.parametrize("folder", ["src/ems/assembler", "src/ems/domain"])
def test_ac_011_06_a_clean_file_passes(tmp_path: Path, folder: str) -> None:
    result = _edit(tmp_path, f"{folder}/join.py", CLEAN)
    assert result.returncode == 0, result.stderr
    assert "Restate import" not in result.stderr


def test_a_restate_import_in_the_actors_passes(tmp_path: Path) -> None:
    result = _edit(tmp_path, "src/ems/actors/household.py", RESTATE_IMPORTS[0])
    assert result.returncode == 0, result.stderr
