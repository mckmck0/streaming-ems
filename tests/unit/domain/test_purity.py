"""AC-009-23: the domain core is pure, and PLN/MWh becomes PLN/kWh in exactly one function."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ems.domain import units

DOMAIN = Path(__file__).resolve().parents[3] / "src" / "ems" / "domain"
ALLOWED = {
    "__future__",
    "dataclasses",
    "enum",
    "math",
    "datetime",
    "zoneinfo",
    "typing",
    "collections.abc",
    "functools",
    "itertools",
    "numpy",
    "numpy.typing",
    "ems.contracts.time",
}
FILES = sorted(DOMAIN.glob("*.py"))


def _imports(tree: ast.AST) -> list[str]:
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            out.append("." * node.level + (node.module or ""))
    return out


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_imports_are_on_the_allow_list(path: Path) -> None:
    for name in _imports(ast.parse(path.read_text())):
        ok = name in ALLOWED or name.startswith(("ems.domain", "."))
        assert ok, f"{path.name} imports {name}"


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_no_wall_clock(path: Path) -> None:
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "today", "utcnow", "time"}, path.name


def _scales_by_1000(node: ast.AST) -> bool:
    if not isinstance(node, ast.BinOp) or not isinstance(node.right, ast.Constant):
        return False
    value = node.right.value
    if isinstance(node.op, ast.Div):
        return value in (1000, 1000.0, 1e3)
    if isinstance(node.op, ast.Mult):
        return value in (1e-3, 0.001)
    return False


def test_the_only_division_by_1000_is_in_units() -> None:
    found = [
        p.name
        for p in FILES
        for node in ast.walk(ast.parse(p.read_text()))
        if _scales_by_1000(node)
    ]
    assert found == ["units.py"]


def test_units() -> None:
    assert units.DT_H == 0.25
    assert units.QUARTERS_PER_HOUR == 4
    assert units.pln_per_mwh_to_pln_per_kwh(439.78) == 439.78 / 1000.0
    assert units.pln_per_mwh_to_pln_per_kwh(-12.5) == -0.0125
