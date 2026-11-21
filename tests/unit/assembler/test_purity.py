"""AC-011-06: the assembler is pure. Every file of ``src/ems/assembler/`` imports only the standard
library modules below, ``ems.contracts`` and ``ems.domain`` (and its own package), and calls no wall
clock, no I/O builtin and no ``hash()`` (string hashes change with the process's hash seed). The
post-edit hook's half of the criterion is in ``tests/unit/test_post_edit_hook.py``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ASSEMBLER = Path(__file__).resolve().parents[3] / "src" / "ems" / "assembler"
STDLIB = {
    "__future__",
    "dataclasses",
    "math",
    "datetime",
    "zoneinfo",
    "typing",
    "collections.abc",
    "functools",
    "itertools",
}
PACKAGES = ("ems.contracts", "ems.domain", "ems.assembler")
MODULES = {
    "__init__.py",
    "params.py",
    "mapping.py",
    "meter_stream.py",
    "sparse_streams.py",
    "join.py",
    "offline.py",
}
FILES = sorted(ASSEMBLER.rglob("*.py"))
CLOCK_ATTRS = {"now", "today", "utcnow", "time", "monotonic", "perf_counter", "time_ns"}
FORBIDDEN_CALLS = {"open", "input", "print", "hash", "id", "__import__", "exec", "eval"}


def _imports(tree: ast.AST) -> list[str]:
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            out.append("." * node.level + (node.module or ""))
    return out


def _allowed(name: str) -> bool:
    if name in STDLIB or name.startswith("."):
        return True
    return any(name == p or name.startswith(p + ".") for p in PACKAGES)


def test_ac_011_06_the_package_has_its_modules() -> None:
    assert {p.name for p in FILES} >= MODULES


@pytest.mark.parametrize("path", FILES, ids=lambda p: str(p.relative_to(ASSEMBLER)))
def test_ac_011_06_imports_are_on_the_allow_list(path: Path) -> None:
    for name in _imports(ast.parse(path.read_text())):
        assert _allowed(name), f"{path.name} imports {name}"


@pytest.mark.parametrize("path", FILES, ids=lambda p: str(p.relative_to(ASSEMBLER)))
def test_ac_011_06_no_clock_no_io_no_hash(path: Path) -> None:
    for node in ast.walk(ast.parse(path.read_text())):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            assert node.func.attr not in CLOCK_ATTRS, f"{path.name}: .{node.func.attr}()"
        elif isinstance(node.func, ast.Name):
            assert node.func.id not in FORBIDDEN_CALLS, f"{path.name}: {node.func.id}()"


def test_the_allow_list_check_refuses_runtime_and_io_modules() -> None:
    for name in ("restate", "restate.serde", "pydantic", "time", "random", "os", "pathlib"):
        assert not _allowed(name), name
    for name in ("ems.contracts.envelope", "ems.domain.stepper", "ems.assembler.join", ".join"):
        assert _allowed(name), name
    assert not _allowed("ems.actors.shell") and not _allowed("ems.ingest.generator")
