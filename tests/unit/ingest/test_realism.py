"""AC-001-22/-23/-24: one simulated year for the test site is plausible for Poland."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import cache
from pathlib import Path

import numpy as np

from ems.contracts.time import local_day_bounds, quarter_index
from ems.ingest.generator import Chunk, SyntheticModel, load_generator_config, load_sites
from ems.ingest.synthetic.prices import price_curve

REPO = Path(__file__).resolve().parents[3]
YEAR_START = datetime(2025, 12, 31, 23, tzinfo=UTC)  # 2026-01-01 00:00 local


@cache
def year() -> tuple[SyntheticModel, Chunk]:
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    model = SyntheticModel(load_sites(REPO / "config", cfg), cfg.seed, REPO / "config")
    t0 = quarter_index(YEAR_START)
    n = quarter_index(local_day_bounds(date(2026, 12, 31))[1]) - t0
    return model, model.chunk(t0, n, [])


def test_ac_001_22_pv_specific_yield() -> None:
    model, chunk = year()
    site = model.sites[0]
    assert site.pv is not None
    yield_kwh_per_kwp = float(chunk.pv_kwh[0].sum()) / site.pv.kwp
    assert 850 <= yield_kwh_per_kwp <= 1150, yield_kwh_per_kwp


def test_ac_001_23_base_load_matches_configured_annual_energy() -> None:
    model, chunk = year()
    total = float(chunk.load_kwh[0].sum())
    configured = model.sites[0].annual_base_load_kwh
    assert abs(total - configured) / configured < 0.05, total


def test_heat_pump_adds_a_few_megawatt_hours() -> None:
    _model, chunk = year()
    hp = float(chunk.hp_kwh[0].sum())
    assert 2500 <= hp <= 7000, hp


def test_ac_001_24_negative_price_share_and_seasonality() -> None:
    model, _ = year()
    t0 = quarter_index(YEAR_START)
    n = quarter_index(local_day_bounds(date(2026, 12, 31))[1]) - t0
    prices = price_curve(t0, n, model.seed, model.price_params, model.weather_params)
    share = float((prices < 0).mean())
    assert 0.01 <= share <= 0.08, share
    months = np.array([(YEAR_START + timedelta(seconds=900 * i)).month for i in range(n)])
    summer = float((prices[(months >= 4) & (months <= 8)] < 0).mean())
    winter = float((prices[(months >= 11) | (months <= 1)] < 0).mean())
    assert summer > winter
