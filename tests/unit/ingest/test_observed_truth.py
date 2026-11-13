"""AC-008-13 … -16: truth weather observed by the satellite and ERA5 (spec 008, plan §6).

Hand-written arrays only (no fixture file). Times are UTC on 2026-05-15 at Poznań (sunrise about
02:40, sunset about 18:55 UTC).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray

from ems.contracts.time import quarter_index
from ems.ingest import rng
from ems.ingest.generator import SyntheticModel, load_generator_config, load_sites
from ems.ingest.history.observed import (
    ObservedSeries,
    ObservedTruth,
    check_split,
    era5_fill_ghi,
    instants_at_end,
    quarter_means,
)
from ems.ingest.localtime import local_hour
from ems.ingest.synthetic.heat_pump import heat_pump_kwh
from ems.ingest.synthetic.pv import pv_energy_kwh
from ems.ingest.synthetic.solar import decompose
from ems.ingest.synthetic.weather import SiteArrays, TimeAxis, Truth, generate_truth, load_params

F64 = NDArray[np.float64]
REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config"
PARAMS = load_params(CONFIG / "calibration" / "weather.yaml")
SEED = 4242
DAY = datetime(2026, 5, 15, tzinfo=UTC)
D0 = int(DAY.timestamp())
T0 = quarter_index(DAY)
N = 96  # one UTC day
HOUR_S = 3600
SLOT_S = 600  # satellite: 10-minute means labelled at their end

REAL = "PL-LOC-000001"  # has a series
OTHER = "PL-LOC-000002"  # has none: the fallback's values, unchanged
COORDS = {REAL: (52.41, 16.93, 85.0), OTHER: (50.06, 19.94, 219.0)}
TRUTH_ARRAYS = (
    "kt",
    "ghi",
    "dni",
    "dhi",
    "temp_c",
    "wind10",
    "wind100",
    "wind120",
    "wind_dir",
    "cloud_pct",
    "national_kt",
    "national_wind100",
)
UNTOUCHED = ("kt", "cloud_pct", "wind100", "wind120", "wind_dir", "national_kt", "national_wind100")
MASKS = (
    "radiation_erbs",
    "radiation_era5",
    "radiation_synthetic",
    "temperature_synthetic",
    "wind_synthetic",
)


def at(hh: int, mm: int = 0) -> int:
    """Unix seconds of hh:mm UTC on DAY (minutes may exceed 59)."""
    return D0 + hh * HOUR_S + mm * 60


def qi(hh: int, mm: int = 0) -> int:
    """Quarter index of the quarter starting at hh:mm UTC on DAY."""
    return at(hh, mm) // 900


def quarters(hh0: int, hh1: int) -> NDArray[np.int64]:
    """Quarter indices of the hours ``[hh0, hh1)``."""
    return np.arange(qi(hh0), qi(hh1), dtype=np.int64)


def i64(values: list[int]) -> NDArray[np.int64]:
    return np.asarray(values, dtype=np.int64)


def f64(values: Sequence[Any]) -> F64:
    return np.asarray(values, dtype=np.float64)


# ── AC-008-13: quarter-hour means of 10-minute backward means ────────────────────────────────────


def test_ac_008_13_label_is_the_end_of_the_mean_and_weights_are_2_3_and_1_3() -> None:
    labels = i64([at(10, 10 * k) for k in range(1, 13)])  # 10:10 … 12:00
    values = f64([11.0, 23.0, 37.0, 41.0, 53.0, 67.0, 71.0, 83.0, 97.0, 101.0, 113.0, 127.0])
    cs10 = np.full(labels.size, 500.0)
    t = quarters(10, 12)
    v = dict(zip(labels.tolist(), values.tolist(), strict=True))

    def hour(hh: int) -> list[float]:
        def x(mm: int) -> float:
            return v[at(hh, mm)]

        return [
            2 / 3 * x(10) + 1 / 3 * x(20),  # [hh:00, hh:15)
            1 / 3 * x(20) + 2 / 3 * x(30),  # [hh:15, hh:30)
            2 / 3 * x(40) + 1 / 3 * x(50),  # [hh:30, hh:45)
            1 / 3 * x(50) + 2 / 3 * x(60),  # [hh:45, hh+1:00)
        ]

    got = quarter_means(labels, values, cs10, t)
    np.testing.assert_allclose(got, hour(10) + hour(11), rtol=1e-12)

    # The value labelled 10:20 is the mean over [10:10, 10:20): it touches the quarters starting
    # at 10:00 and 10:15 for 5 minutes each (a start label would put 2/3 into 10:15 and none
    # into 10:00).
    impulse = np.where(labels == at(10, 20), 900.0, 0.0)
    np.testing.assert_allclose(
        quarter_means(labels, impulse, cs10, t), [300, 300, 0, 0, 0, 0, 0, 0], atol=1e-9
    )


def _dawn() -> tuple[NDArray[np.int64], F64, F64]:
    """02:10 … 04:00: null while the slot's clear-sky GHI is 0, values from 03:20."""
    labels = i64([at(2, 10 * k) for k in range(1, 13)])
    values = f64([np.nan] * 7 + [30.0, 45.0, 60.0, 75.0, 90.0])
    cs10 = f64([0.0] * 7 + [5.0, 12.0, 20.0, 30.0, 40.0])
    return labels, values, cs10


def test_ac_008_13_null_with_zero_clear_sky_is_zero_also_in_a_sunrise_quarter() -> None:
    labels, values, cs10 = _dawn()
    got = quarter_means(labels, values, cs10, quarters(2, 4))
    # 03:00: 2/3 x 0 (03:10 null, clear-sky 0) + 1/3 x 30 -> 10, not a gap
    expected = [0.0, 0.0, 0.0, 0.0, 10.0, 40.0, 2 / 3 * 60 + 1 / 3 * 75, 1 / 3 * 75 + 2 / 3 * 90]
    np.testing.assert_allclose(got, expected, rtol=1e-12, atol=1e-12)


def test_ac_008_13_null_with_sun_or_an_absent_slot_is_a_gap() -> None:
    labels, values, cs10 = _dawn()
    values[labels == at(3, 40)] = np.nan  # clear-sky 20 W/m2: a real gap
    got = quarter_means(labels, values, cs10, quarters(3, 4))
    assert np.isnan(got[2])  # 03:30 touches 03:40
    np.testing.assert_allclose(got[[0, 1, 3]], [10.0, 40.0, 1 / 3 * 75 + 2 / 3 * 90], rtol=1e-12)

    labels, values, cs10 = _dawn()
    keep = labels != at(3, 50)  # the slot is absent from the response
    got = quarter_means(labels[keep], values[keep], cs10[keep], quarters(3, 5))
    assert np.isnan(got[2]) and np.isnan(got[3])  # 03:30 and 03:45 touch 03:50
    np.testing.assert_allclose(got[:2], [10.0, 40.0], rtol=1e-12)
    assert np.isnan(got[4:]).all()  # past the last label (04:00)


# ── AC-008-14: ERA5 instants at the interval end ─────────────────────────────────────────────────


def test_ac_008_14_instants_are_interpolated_to_the_interval_end_including_the_last() -> None:
    hours = i64([at(6), at(7), at(8), at(9)])
    values = f64([10.0, 14.0, 6.0, 8.0])
    got = instants_at_end(hours, values, quarters(6, 9))
    expected = [11.0, 12.0, 13.0, 14.0, 12.0, 10.0, 8.0, 6.0, 6.5, 7.0, 7.5, 8.0]
    np.testing.assert_allclose(got, expected, rtol=1e-12)
    assert got[-1] == pytest.approx(8.0)  # the last quarter ends at 09:00: the value at 09:00

    short = instants_at_end(hours[:3], values[:3], quarters(6, 9))  # no 09:00 instant
    np.testing.assert_allclose(short[:8], expected[:8], rtol=1e-12)
    assert np.isnan(short[8:]).all()


def test_ac_008_14_a_missing_neighbour_gives_nan() -> None:
    hours = i64([at(6), at(7), at(8), at(9)])
    got = instants_at_end(hours, f64([10.0, np.nan, 6.0, 8.0]), quarters(6, 9))
    assert np.isnan(got[:7]).all()  # ends 06:15 … 07:45 need 07:00
    np.testing.assert_allclose(got[8:], [6.5, 7.0, 7.5, 8.0], rtol=1e-12)

    absent = i64([at(6), at(8), at(9)])  # 07:00 absent: never interpolate across the hole
    got = instants_at_end(absent, f64([10.0, 6.0, 8.0]), quarters(6, 9))
    assert np.isnan(got[:7]).all()
    np.testing.assert_allclose(got[8:], [6.5, 7.0, 7.5, 8.0], rtol=1e-12)


# ── AC-008-16: ERA5 GHI fill ─────────────────────────────────────────────────────────────────────

FILL_T = quarters(2, 6)  # 02:00 … 06:00, sunrise in the 03:00 hour
FILL_CS = f64(
    [
        *[0.0, 0.0, 0.0, 0.8],  # mean 0.2 < 1 W/m2: index 0
        *[4.0, 12.0, 24.0, 40.0],  # mean 20
        *[60.0, 85.0, 110.0, 145.0],  # mean 100
        *[170.0, 200.0, 230.0, 260.0],  # mean 215
    ]
)
# ERA5 GHI labelled H is the mean over [H - 1 h, H).
FILL_GHI_H = {
    at(1): 0.0,
    at(2): 0.0,
    at(3): 0.5,
    at(4): 16.0,
    at(5): 75.0,
    at(6): 300.0,
    at(7): 500.0,
}


def _fill_inputs(drop: int | None = None) -> tuple[NDArray[np.int64], F64]:
    items = [(h, g) for h, g in FILL_GHI_H.items() if h != drop]
    return i64([h for h, _ in items]), f64([g for _, g in items])


def _fill_with_label_shift(shift_h: int) -> F64:
    """The fill if the quarter [s, s + 15) used the label floor_hour(s) + (1 + shift_h) h."""
    out = []
    for k in range(4):
        cs = FILL_CS[4 * k : 4 * k + 4]
        ghi = FILL_GHI_H[at(2 + k + 1 + shift_h)]
        index = 0.0 if cs.mean() < 1.0 else min(max(ghi / cs.mean(), 0.0), 1.1)
        out.extend(index * cs)
    return f64(out)


def test_ac_008_16_era5_fill_uses_the_label_one_hour_after_the_quarters_hour() -> None:
    hours, ghi_h = _fill_inputs()
    got = era5_fill_ghi(hours, ghi_h, FILL_CS, FILL_T)
    expected = [
        *[0.0, 0.0, 0.0, 0.0],  # 03:00 label: 0.5 / 0.2 would be 2.5, but the mean is < 1 W/m2
        *[3.2, 9.6, 19.2, 32.0],  # sunrise hour: 04:00 label, index 16 / 20 = 0.8
        *[45.0, 63.75, 82.5, 108.75],  # 05:00 label, index 75 / 100 = 0.75
        *[187.0, 220.0, 253.0, 286.0],  # 06:00 label, 300 / 215 = 1.395 clipped to 1.1
    ]
    np.testing.assert_allclose(got, expected, rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(got[4:8] / FILL_CS[4:8], 0.8, rtol=1e-12)
    np.testing.assert_allclose(got[12:] / FILL_CS[12:], 1.1, rtol=1e-12)
    for shift_h in (-1, 1):  # the case is built so that a one-hour label shift is visible
        assert not np.allclose(_fill_with_label_shift(shift_h), expected), shift_h


def test_ac_008_16_era5_fill_is_nan_where_the_hour_is_missing() -> None:
    hours, ghi_h = _fill_inputs()
    ghi_h[hours == at(5)] = np.nan  # null 05:00 -> the 04:00 hour has no fill
    got = era5_fill_ghi(hours, ghi_h, FILL_CS, FILL_T)
    assert np.isnan(got[8:12]).all() and not np.isnan(got[4:8]).any()
    hours, ghi_h = _fill_inputs(drop=at(4))  # absent 04:00 -> the 03:00 hour has no fill
    got = era5_fill_ghi(hours, ghi_h, FILL_CS, FILL_T)
    assert np.isnan(got[4:8]).all() and not np.isnan(got[8:]).any()


def test_ac_008_16_era5_fill_needs_an_hour_aligned_axis() -> None:
    hours, ghi_h = _fill_inputs()
    with pytest.raises(ValueError):
        era5_fill_ghi(hours, ghi_h, FILL_CS[1:], FILL_T[1:])  # starts at 02:15
    with pytest.raises(ValueError):
        era5_fill_ghi(hours, ghi_h, FILL_CS[:-1], FILL_T[:-1])  # ends at 05:45


# ── AC-008-16: closure check of the satellite split ──────────────────────────────────────────────


def test_ac_008_16_check_split() -> None:
    cases = [  # name, GHI, DNI, DHI, zenith (deg), DNI extra, kept
        ("plausible", 500.0, 500.0, 150.0, 40.0, 1360.0, True),
        ("dni_cos_z_at_ghi_plus_20", 100.0, 120.0, 0.0, 0.0, 1360.0, True),
        ("dni_cos_z_above_ghi_plus_20", 100.0, 120.5, 0.0, 0.0, 1360.0, False),
        ("low_sun_dni_too_high", 100.0, 500.0, 50.0, 60.0, 1360.0, False),
        ("dhi_equals_ghi", 100.0, 0.0, 100.0, 30.0, 1360.0, True),
        ("dhi_above_ghi", 100.0, 0.0, 120.0, 30.0, 1360.0, False),
        ("dni_equals_extra", 1000.0, 1360.0, 0.0, 89.9, 1360.0, True),
        ("dni_above_extra", 1000.0, 1400.0, 0.0, 89.9, 1360.0, False),
        ("night", 0.0, 0.0, 0.0, 100.0, 1360.0, True),
    ]
    cols = list(zip(*cases, strict=True))
    got = check_split(f64(cols[1]), f64(cols[2]), f64(cols[3]), f64(cols[4]), f64(cols[5]))
    assert dict(zip(cols[0], got.tolist(), strict=True)) == dict(zip(cols[0], cols[6], strict=True))


# ── ObservedTruth ─────────────────────────────────────────────────────────────────────────────────


def site_arrays(ids: list[str]) -> SiteArrays:
    return SiteArrays(
        keys=np.array([rng.location_key(i) for i in ids], dtype=np.uint64),
        lat=f64([COORDS[i][0] for i in ids]),
        lon=f64([COORDS[i][1] for i in ids]),
        elevation_m=f64([COORDS[i][2] for i in ids]),
        shear_alpha=np.full(len(ids), 0.22),
        wind_scale_ms=np.full(len(ids), 3.4),
    )


def by_key(ids: list[str]) -> dict[int, str]:
    return {int(rng.location_key(i)): i for i in ids}


@dataclass
class Fallback:
    """Stub fallback ``TruthSource``: the synthetic truth of a fixed seed, with a call log."""

    calls: list[tuple[int, int, int]] = field(default_factory=list)

    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth:
        self.calls.append((int(sites.keys.size), int(axis.t[0]), axis.n))
        return generate_truth(sites, axis, SEED, PARAMS)


def sat_labels() -> NDArray[np.int64]:
    return D0 + SLOT_S * np.arange(1, 145, dtype=np.int64)  # 00:10 … 24:00


def era5_hours() -> NDArray[np.int64]:
    return D0 + HOUR_S * np.arange(25, dtype=np.int64)  # 00:00 … 24:00


def constant_series(location_id: str) -> ObservedSeries:
    n_sat, n_era5 = sat_labels().size, era5_hours().size
    return ObservedSeries(
        location_id=location_id,
        sat_labels=sat_labels(),
        ghi10=np.full(n_sat, 400.0),
        dni10=np.full(n_sat, 300.0),
        dhi10=np.full(n_sat, 150.0),
        era5_hours=era5_hours(),
        temp_h=np.full(n_era5, 5.0),
        wind10_h=np.full(n_era5, 3.0),
        ghi_h=np.full(n_era5, 400.0),
    )


def base_series(location_id: str) -> ObservedSeries:
    """Satellite 400/300/150 W/m2 for labels in (03:30, 18:30], reported 0 outside (so it reads 0
    at low sun); ERA5 linear in time: temperature 8 + 0.5 h, wind 2 + 0.1 h, GHI 300 + 10 h.
    """
    labels = sat_labels()
    end_h = (labels - D0) / HOUR_S
    sun = (end_h > 3.5) & (end_h <= 18.5)
    h = np.arange(25, dtype=np.float64)
    return ObservedSeries(
        location_id=location_id,
        sat_labels=labels,
        ghi10=np.where(sun, 400.0, 0.0),
        dni10=np.where(sun, 300.0, 0.0),
        dhi10=np.where(sun, 150.0, 0.0),
        era5_hours=era5_hours(),
        temp_h=8.0 + 0.5 * h,
        wind10_h=2.0 + 0.1 * h,
        ghi_h=300.0 + 10.0 * h,
    )


def gappy_series(location_id: str, era5_ghi_11: float = 410.0) -> ObservedSeries:
    """``base_series`` with one case per hour:

    - 00:00-01:30 satellite null at night (clear-sky 0): 0, not a gap;
    - 10:00 satellite null in daylight, ERA5 GHI at 11:00 present: ERA5 fill + Erbs;
    - 13:00 DNI null, 14:00 DHI null: Erbs split of the satellite GHI;
    - 15:00 DNI 2000 W/m2 (above the extraterrestrial DNI), 17:00 DHI 500 > GHI: Erbs;
    - 16:00 satellite null and ERA5 GHI at 17:00 null: the fallback's radiation;
    - ERA5 temperature null at 12:00, 10 m wind null at 07:00: the fallback's values.
    """
    s = base_series(location_id)
    lab = s.sat_labels
    ghi10, dni10, dhi10 = s.ghi10.copy(), s.dni10.copy(), s.dhi10.copy()

    def hour_slots(hh: int) -> NDArray[np.bool_]:
        return (lab > at(hh)) & (lab <= at(hh + 1))  # the six means inside [hh:00, hh+1:00)

    night = lab <= at(1, 30)
    for arr in (ghi10, dni10, dhi10):
        arr[night] = np.nan
        arr[hour_slots(10)] = np.nan
        arr[hour_slots(16)] = np.nan
    dni10[hour_slots(13)] = np.nan
    dhi10[hour_slots(14)] = np.nan
    dni10[hour_slots(15)] = 2000.0
    dhi10[hour_slots(17)] = 500.0
    temp_h, wind10_h, ghi_h = s.temp_h.copy(), s.wind10_h.copy(), s.ghi_h.copy()
    temp_h[12] = np.nan
    wind10_h[7] = np.nan
    ghi_h[11] = era5_ghi_11
    ghi_h[17] = np.nan
    return dataclasses.replace(
        s,
        ghi10=ghi10,
        dni10=dni10,
        dhi10=dhi10,
        temp_h=temp_h,
        wind10_h=wind10_h,
        ghi_h=ghi_h,
    )


def observed(series: dict[str, ObservedSeries], fallback: Any = None) -> ObservedTruth:
    return ObservedTruth(
        location_by_key=by_key([REAL, OTHER]),
        series=series,
        fallback=fallback or Fallback(),
    )


def hour_idx(*hours: int) -> NDArray[np.int64]:
    """Axis positions of the quarters of the given hours."""
    return np.concatenate([np.arange(4 * h, 4 * h + 4) for h in hours]).astype(np.int64)


def mask_of(idx: NDArray[np.int64]) -> NDArray[np.bool_]:
    out = np.zeros(N, dtype=bool)
    out[idx] = True
    return out


@dataclass(frozen=True)
class Scenario:
    sites: SiteArrays
    axis: TimeAxis
    source: ObservedTruth
    truth: Truth
    ref: Truth  # the fallback's truth for the same sites and axis
    masks: dict[str, dict[str, NDArray[np.bool_]]]
    doy: NDArray[np.int64]


@pytest.fixture(scope="module")
def gappy() -> Scenario:
    sites = site_arrays([REAL, OTHER])
    axis = TimeAxis.from_range(T0, N)
    source = observed({REAL: gappy_series(REAL)})
    return Scenario(
        sites=sites,
        axis=axis,
        source=source,
        truth=source(sites, axis),
        ref=Fallback()(sites, axis),
        masks=source.masks(sites, axis),
        doy=axis.calendar(axis.unix_mid)[0],
    )


def erbs(sc: Scenario, ghi_row: F64) -> tuple[F64, F64]:
    dni, dhi = decompose(ghi_row[None, :], sc.ref.geometry.zenith[:1], sc.doy)
    return dni[0], dhi[0]


# ── AC-008-15 ────────────────────────────────────────────────────────────────────────────────────


def test_ac_008_15_pv_and_heat_pump_are_the_unchanged_models_on_the_observed_arrays() -> None:
    """A constant series at both real-data sites: the model's PV and heat pump equal
    ``pv_energy_kwh`` and ``heat_pump_kwh`` on the source's arrays, which come from the series.
    """
    cfg = load_generator_config(CONFIG / "generator.history.yaml")
    sites = [s for s in load_sites(CONFIG, cfg) if s.location_id in cfg.live_locations]
    ids = [s.location_id for s in sites]
    assert ids == list(cfg.live_locations)
    source = ObservedTruth(
        location_by_key=by_key(ids),
        series={i: constant_series(i) for i in ids},
        fallback=Fallback(),
    )
    model = SyntheticModel(sites, cfg.seed, CONFIG, truth_source=source)
    chunk = model.chunk(T0, N, [])
    axis = chunk.truth_axis
    truth = source(model.arrays, axis)
    pv = pv_energy_kwh(truth, model.pv)[:, :N]
    hp = heat_pump_kwh(
        truth.temp_c[:, :N],
        local_hour(axis.unix_mid[:N]),
        model.hp_loss,
        model.hp_dhw,
        model.hp_base,
        model.hp_eta,
    )
    assert np.array_equal(chunk.pv_kwh, pv) and np.array_equal(chunk.hp_kwh, hp)
    assert chunk.pv_kwh.max() > 0 and chunk.hp_kwh.max() > 0

    day = truth.geometry.cs_ghi > 0
    np.testing.assert_allclose(truth.ghi[day], 400.0, rtol=1e-12)
    np.testing.assert_allclose(truth.dni[day], 300.0, rtol=1e-12)
    np.testing.assert_allclose(truth.dhi[day], 150.0, rtol=1e-12)
    np.testing.assert_allclose(truth.temp_c, 5.0, rtol=1e-12)
    np.testing.assert_allclose(truth.wind10, 3.0, rtol=1e-12)

    default = SyntheticModel(sites, cfg.seed, CONFIG).chunk(T0, N, [])
    assert not np.array_equal(default.pv_kwh, chunk.pv_kwh)  # the observed source was used
    assert np.array_equal(default.load_kwh, chunk.load_kwh)  # base load reads no weather
    sliced = SyntheticModel(sites, cfg.seed, CONFIG, site_chunk=1, truth_source=source)
    one_by_one = sliced.chunk(T0, N, [])
    assert np.array_equal(one_by_one.pv_kwh, chunk.pv_kwh)
    assert np.array_equal(one_by_one.hp_kwh, chunk.hp_kwh)


# ── AC-008-16: overlay, fallbacks, masks ─────────────────────────────────────────────────────────


def test_ac_008_16_satellite_split_is_kept_where_it_passes(gappy: Scenario) -> None:
    kept = hour_idx(5, 6, 7, 8, 9, 11, 12)
    np.testing.assert_allclose(gappy.truth.ghi[0, kept], 400.0, rtol=1e-12)
    np.testing.assert_allclose(gappy.truth.dni[0, kept], 300.0, rtol=1e-12)
    np.testing.assert_allclose(gappy.truth.dhi[0, kept], 150.0, rtol=1e-12)
    for name in MASKS[:3]:
        assert not gappy.masks[REAL][name][kept].any(), name


def test_ac_008_16_night_nulls_are_zero_not_gaps(gappy: Scenario) -> None:
    night = np.arange(6)  # 00:00 … 01:15: every touched slot is null with clear-sky 0
    for name in ("ghi", "dni", "dhi"):
        assert (getattr(gappy.truth, name)[0, night] == 0.0).all(), name
    for name in MASKS[:3]:
        assert not gappy.masks[REAL][name][night].any(), name


def test_ac_008_16_daylight_gap_is_filled_from_era5_with_an_erbs_split(gappy: Scenario) -> None:
    gap = hour_idx(10)
    cs = gappy.ref.geometry.cs_ghi[0, gap]
    index = min(410.0 / cs.mean(), 1.1)  # ERA5 label 11:00 = the mean over [10:00, 11:00)
    np.testing.assert_allclose(gappy.truth.ghi[0, gap], index * cs, rtol=1e-12)
    dni, dhi = erbs(gappy, gappy.truth.ghi[0])
    np.testing.assert_allclose(gappy.truth.dni[0, gap], dni[gap], rtol=1e-9, atol=1e-9)
    np.testing.assert_allclose(gappy.truth.dhi[0, gap], dhi[gap], rtol=1e-9, atol=1e-9)
    assert np.array_equal(gappy.masks[REAL]["radiation_era5"], mask_of(gap))


def test_ac_008_16_missing_or_failing_dni_dhi_gives_an_erbs_split(gappy: Scenario) -> None:
    resplit = hour_idx(13, 14, 15, 17)  # DNI null, DHI null, DNI > extraterrestrial, DHI > GHI
    np.testing.assert_allclose(gappy.truth.ghi[0, resplit], 400.0, rtol=1e-12)
    dni, dhi = erbs(gappy, gappy.truth.ghi[0])
    np.testing.assert_allclose(gappy.truth.dni[0, resplit], dni[resplit], rtol=1e-9, atol=1e-9)
    np.testing.assert_allclose(gappy.truth.dhi[0, resplit], dhi[resplit], rtol=1e-9, atol=1e-9)
    assert np.array_equal(gappy.masks[REAL]["radiation_erbs"], mask_of(resplit))


def test_ac_008_16_without_satellite_and_era5_the_fallback_radiation_is_used(
    gappy: Scenario,
) -> None:
    both = hour_idx(16)
    for name in ("ghi", "dni", "dhi"):
        got, ref = getattr(gappy.truth, name), getattr(gappy.ref, name)
        assert np.array_equal(got[0, both], ref[0, both]), name
    assert np.array_equal(gappy.masks[REAL]["radiation_synthetic"], mask_of(both))


def _instant_case(sc: Scenario, field_name: str, mask: str, nan_hour: int, expected: F64) -> None:
    """ERA5 null at ``nan_hour``: quarters ending strictly between the hours around it take the
    fallback's value; quarters ending at least one quarter away from that span take ERA5.
    """
    end_h = (sc.axis.unix_end - D0) / HOUR_S
    hit = (end_h > nan_hour - 1) & (end_h < nan_hour + 1)
    far = (end_h < nan_hour - 1) | (end_h > nan_hour + 1)
    got, ref = getattr(sc.truth, field_name)[0], getattr(sc.ref, field_name)[0]
    assert hit.sum() == 7
    assert np.array_equal(got[hit], ref[hit])
    np.testing.assert_allclose(got[far], expected[far], rtol=1e-12)
    m = sc.masks[REAL][mask]
    assert m[hit].all() and not m[far].any()


def test_ac_008_16_missing_era5_temperature_and_wind_come_from_the_fallback(
    gappy: Scenario,
) -> None:
    end_h = (gappy.axis.unix_end - D0) / HOUR_S  # ERA5 here is linear in time
    _instant_case(gappy, "temp_c", "temperature_synthetic", 12, 8.0 + 0.5 * end_h)
    _instant_case(gappy, "wind10", "wind_synthetic", 7, 2.0 + 0.1 * end_h)


def test_ac_008_16_other_fields_and_rows_without_a_series_are_the_fallbacks(
    gappy: Scenario,
) -> None:
    for name in UNTOUCHED:
        assert np.array_equal(getattr(gappy.truth, name), getattr(gappy.ref, name)), name
    for name in TRUTH_ARRAYS:
        assert np.array_equal(getattr(gappy.truth, name)[1], getattr(gappy.ref, name)[1]), name
    assert np.array_equal(gappy.truth.geometry.cs_ghi, gappy.ref.geometry.cs_ghi)
    assert np.array_equal(gappy.truth.axis.t, gappy.axis.t)
    other = gappy.masks.get(OTHER)
    assert other is None or not any(m.any() for m in other.values())


def test_ac_008_16_masks_per_site(gappy: Scenario) -> None:
    masks = gappy.masks[REAL]
    assert set(masks) >= set(MASKS)
    for name in MASKS:
        assert masks[name].dtype == np.bool_ and masks[name].shape == (N,), name
    radiation = np.stack([masks[name] for name in MASKS[:3]])
    assert (radiation.sum(axis=0) <= 1).all()  # one radiation source per quarter


def test_ac_008_16_no_nan_in_any_output(gappy: Scenario) -> None:
    for name in TRUTH_ARRAYS:
        assert not np.isnan(getattr(gappy.truth, name)).any(), name


def test_ac_008_16_axis_must_be_hour_aligned() -> None:
    sites = site_arrays([REAL, OTHER])
    source = observed({REAL: base_series(REAL)})
    with pytest.raises(ValueError):
        source(sites, TimeAxis.from_range(T0 + 1, N - 4))  # starts at 00:15
    with pytest.raises(ValueError):
        source(sites, TimeAxis.from_range(T0, N - 2))  # ends at 23:30


def test_ac_008_16_diagnostics_per_site(gappy: Scenario) -> None:
    diag = gappy.source.diagnostics(gappy.sites, gappy.axis)
    assert set(diag[REAL]) >= {"era5_clip_share", "low_sun_zero_share"}
    assert diag[REAL]["era5_clip_share"] == 0.0  # 410 / ~840 W/m2: no clip
    # the satellite reads 0 between sunrise (~02:40) and 03:30, and after 18:30, while the sun is up
    assert 0.0 < diag[REAL]["low_sun_zero_share"] < 0.5

    clipped = observed({REAL: gappy_series(REAL, era5_ghi_11=5000.0)})
    truth = clipped(gappy.sites, gappy.axis)
    gap = hour_idx(10)
    np.testing.assert_allclose(
        truth.ghi[0, gap], 1.1 * gappy.ref.geometry.cs_ghi[0, gap], rtol=1e-12
    )
    assert clipped.diagnostics(gappy.sites, gappy.axis)[REAL]["era5_clip_share"] == 1.0

    constant = observed({REAL: constant_series(REAL)})
    assert constant.diagnostics(gappy.sites, gappy.axis)[REAL]["low_sun_zero_share"] == 0.0
