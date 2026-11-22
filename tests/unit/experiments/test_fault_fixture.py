"""Spec 011, plan decision "Fault fixture" (serves AC-011-40, -41): the e2e fault runs use
`tests/fixtures/generator.faults.yaml`, which is `config/generator.yaml` with three raised rates and
nothing else changed, so the fault runs differ from a normal backfill only in how often faults hit.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from ems.ingest.faults import FaultConfig
from ems.ingest.generator import load_generator_config

RAISED_RATES = {
    "faults.late_rate": (0.005, 0.15),
    "faults.missing_rate": (0.002, 0.03),
    "faults.duplicate_rate": (0.001, 0.03),
}


def _flat(node: Any, prefix: str = "") -> dict[str, Any]:
    """Every leaf of a YAML document by its dotted path."""
    if isinstance(node, Mapping):
        out: dict[str, Any] = {}
        for key, value in node.items():
            out |= _flat(value, f"{prefix}{key}.")
        return out
    return {prefix.rstrip("."): node}


def _load(path: Path) -> dict[str, Any]:
    return _flat(YAML(typ="safe").load(path.read_text()))


def test_fault_fixture_differs_from_the_generator_config_only_in_three_rates(repo: Path) -> None:
    config = _load(repo / "config" / "generator.yaml")
    fixture = _load(repo / "tests" / "fixtures" / "generator.faults.yaml")
    assert set(fixture) == set(config)
    differences = {k: (config[k], fixture[k]) for k in config if config[k] != fixture[k]}
    assert differences == RAISED_RATES


def test_fault_fixture_loads_as_a_generator_config_with_the_raised_rates(repo: Path) -> None:
    cfg = load_generator_config(repo / "tests" / "fixtures" / "generator.faults.yaml")
    faults = FaultConfig.from_dict(cfg.faults)
    assert (faults.late_rate, faults.missing_rate, faults.duplicate_rate) == (0.15, 0.03, 0.03)
    assert faults.enabled is False  # the runs switch it on with `ems-gen run --faults`
