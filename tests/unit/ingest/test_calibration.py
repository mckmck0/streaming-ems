"""AC-002-13: the calibration job fits candidates (with data windows) from recorded history."""

from __future__ import annotations

import json
from pathlib import Path

from ruamel.yaml import YAML

from ems.ingest.calibration import fit_prices, fit_temperature, write_candidate

REPO = Path(__file__).resolve().parents[3]
FIX = REPO / "tests" / "fixtures"


def test_ac_002_13_prices_use_only_quarter_hour_history(tmp_path: Path) -> None:
    files = sorted((FIX / "pse").glob("rce-pln_20*.json"))
    files = [f for f in files if "page" not in f.name]
    result = fit_prices([json.loads(f.read_text()) for f in files], REPO / "mappings")
    # 2025-03-30 is before the quarter-hour switch (2025-10-01) and is skipped
    assert result["data_window"] == {"start": "2025-10-26", "end": "2026-10-21", "days": 3}
    assert result["samples"] == 100 + 92 + 96 and 0 <= result["negative_share"] < 1
    path = write_candidate(result, tmp_path, "fixtures")
    doc = YAML(typ="safe").load(path.read_text())
    assert doc["kind"] == "prices" and doc["data_window"]["days"] == 3


def test_ac_002_13_temperature_fit_is_plausible(tmp_path: Path) -> None:
    result = fit_temperature(
        json.loads((FIX / "openmeteo" / "archive_era5_2024_temperature.json").read_text())
    )
    assert 6 < result["annual_mean_c"] < 13 and 7 < result["annual_amplitude_c"] < 13
    assert result["coldest_day_of_year"] < 45 or result["coldest_day_of_year"] > 340
    assert result["data_window"]["start"] == "2024-01-01"
    assert write_candidate(result, tmp_path, "era5").exists()
