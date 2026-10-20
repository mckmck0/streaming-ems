"""AC-001-20/-21/-26: weather and PV bounds, forecast skill, batch independence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from hypothesis import given
from hypothesis import strategies as st

from ems.contracts.time import quarter_index
from ems.ingest import rng
from ems.ingest.synthetic.pv import PvArrays, pv_energy_kwh
from ems.ingest.synthetic.weather import (
    SiteArrays,
    TimeAxis,
    forecast_run,
    generate_truth,
    load_params,
)

REPO = Path(__file__).resolve().parents[2]
PARAMS = load_params(REPO / "config" / "calibration" / "weather.yaml")


def sites(ids: list[int]) -> SiteArrays:
    n = len(ids)
    lat = np.array([49.5 + (i * 0.37) % 5 for i in ids])
    lon = np.array([14.5 + (i * 0.71) % 9.5 for i in ids])
    return SiteArrays(
        keys=np.array([rng.location_key(f"PL-LOC-{i:06d}") for i in ids], dtype=np.uint64),
        lat=lat,
        lon=lon,
        elevation_m=np.full(n, 120.0),
        shear_alpha=np.full(n, 0.2),
        wind_scale_ms=np.full(n, 3.4),
    )


def pv(n: int) -> PvArrays:
    return PvArrays(
        kwp=np.full(n, 10.0),
        tilt_deg=np.full(n, 35.0),
        azimuth_deg=np.full(n, 180.0),
        ac_kw=np.full(n, 9.0),
        gamma_pdc=np.full(n, -0.0037),
        losses=np.full(n, 0.1),
    )


@given(day=st.integers(0, 364), seed=st.integers(0, 2**31))
def test_ac_001_20_irradiance_bounds_and_pv_zero_at_night(day: int, seed: int) -> None:
    t0 = quarter_index(datetime(2026, 1, 1, tzinfo=UTC)) + 96 * day
    s = sites([1, 2, 3])
    truth = generate_truth(s, TimeAxis.from_range(t0, 96), seed, PARAMS)
    cs = truth.geometry.cs_ghi
    assert (truth.ghi >= 0).all() and (truth.ghi <= 1.1 * cs + 1e-9).all()
    assert (
        (truth.dni >= 0).all() and (truth.dhi >= 0).all() and (truth.dhi <= truth.ghi + 1e-9).all()
    )
    energy = pv_energy_kwh(truth, pv(3))
    assert (energy >= 0).all()
    night = truth.geometry.apparent_zenith >= 90
    assert (energy[night] == 0).all()
    assert (energy <= 9.0 * 0.25 + 1e-9).all()  # inverter clipping at the AC rating


def test_ac_001_21_forecast_error_grows_with_lead_time() -> None:
    s = sites([1])
    t0 = quarter_index(datetime(2026, 4, 1, tzinfo=UTC))
    truth = generate_truth(s, TimeAxis.from_range(t0, 96 * 60), 11, PARAMS)
    errors = np.stack(
        [
            forecast_run(truth, t0 + 12 * k, 192, 11, s.keys, PARAMS).kt_error[0]
            for k in range(0, 400)
        ]
    )
    early = errors[:, :8].std()
    late = errors[:, -8:].std()
    assert late > 1.5 * early


def test_ac_001_26_truth_for_a_site_is_independent_of_the_batch() -> None:
    t0 = quarter_index(datetime(2026, 6, 1, tzinfo=UTC))
    axis = TimeAxis.from_range(t0, 96 * 3)
    alone = generate_truth(sites([2]), axis, 5, PARAMS)
    batch = generate_truth(sites([1, 2, 3, 4]), axis, 5, PARAMS)
    for name in ("ghi", "temp_c", "wind100", "kt"):
        assert np.array_equal(getattr(alone, name)[0], getattr(batch, name)[1]), name
