"""AC-003-08: the benchmark job writes timings with config, versions, hardware and git commit."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def test_ac_003_08_benchmark_writes_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = importlib.util.spec_from_file_location("benchmark", REPO / "jobs" / "benchmark.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "CONFIG", REPO / "config")
    module.main(sites=[5], hours=1, produce_sites=0, bootstrap="unused:1", out=tmp_path)
    [results] = list(tmp_path.glob("benchmark-*/results.json"))
    doc = json.loads(results.read_text())
    for key in ("generation", "config", "versions", "hardware", "git_commit", "window"):
        assert key in doc
    gen = doc["generation"][0]
    assert gen["sites"] == 5 and gen["events"] > 0 and gen["events_per_s"] > 0
