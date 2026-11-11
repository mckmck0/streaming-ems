"""AC-007-02 … -06: synthetic forecast runs agree for a valid time, carry a bounded timing error and
stay pure functions of (seed, site, variable, valid time or run and step) (spec 007, plan §2).

The statistics (AC-007-02 … -05) use one site, 60 days of runs issued every 3 h, and the total kt
error (amplitude plus phase, ``ForecastRun.kt_error``) on daylight intervals (clear-sky GHI > 0).
Leads follow the plan's convention ``L_k = (k + 1) * 0.25 h`` (interval end); a lead bin
``(lo, hi]`` holds the steps whose lead is above ``lo`` and at most ``hi`` hours.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from numpy.typing import NDArray

from ems.contracts.time import quarter_index
from ems.ingest import rng
from ems.ingest.catalog.build import build_catalog
from ems.ingest.generator import FORECAST_STEPS, SyntheticModel
from ems.ingest.synthetic.weather import (
    ForecastRun,
    SiteArrays,
    TimeAxis,
    forecast_run,
    generate_truth,
    load_params,
    truth_margin_q,
)

F64 = NDArray[np.float64]
REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "config"
PARAMS = load_params(CONFIG / "calibration" / "weather.yaml")

SEED = 11
DAYS = 60
CADENCE_Q = 12  # runs every 3 h
RUNS = DAYS * 96 // CADENCE_Q
STEPS = FORECAST_STEPS  # 192 quarter-hours, 48 h
LEAD_H = (np.arange(STEPS) + 1) * 0.25  # plan §2: L_k = (k + 1) * 0.25 h
T0 = quarter_index(datetime(2026, 4, 1, tzinfo=UTC))
T_YEAR = quarter_index(datetime(2026, 1, 1, tzinfo=UTC))

# Every array a ForecastRun carries, including the new phase diagnostic.
FIELDS = (
    "ghi",
    "dni",
    "dhi",
    "temp_c",
    "cloud_pct",
    "wind10",
    "wind100",
    "wind120",
    "wind_dir",
    "kt_error",
    "phase_h",
)

# Rebuilding the warp k + 4*delta from delta in hours costs a few ulps; 1e-9 quarter-hours is far
# below any real step backwards.
WARP_EPS_Q = 1e-9

# Risk G1: an AR(1) value depends on where its window starts only through a weight below exp(-6) on
# the difference of two unit-variance states (|difference| < 6 in practice), so a unit AR path moves
# by at most G1_UNIT between window splits. The tolerances scale that by the processes behind each
# field:
# - kt: truth kt (local sd up to 0.17 + regional 0.08), times 4 for the logit slope of forecast kt;
# - GHI: clear-sky GHI (<= ~1000 W/m2) times that kt bound;
# - temperature: national 3.2 C + local 0.8 C;
# - 10 m wind: the Weibull copula (<= ~2.5 m/s per unit z), times a forecast factor of up to ~3.
# Observed on the implementation (60 seed/day/split cases): kt 1.4e-3, GHI 0.6 W/m2, 0.026 C,
# 0.018 m/s, all below these bounds.
G1_UNIT = 6.0 * math.exp(-6.0)  # 0.0149
G1_TOL = {
    "kt_error": 4.0 * (0.17 + 0.08) * G1_UNIT,  # 0.015 (kt)
    "ghi": 1000.0 * 4.0 * (0.17 + 0.08) * G1_UNIT,  # 15 W/m2
    "temp_c": (3.2 + 0.8) * G1_UNIT,  # 0.06 C
    "wind10": 3.0 * 2.5 * G1_UNIT,  # 0.11 m/s
}


def sites(ids: list[int]) -> SiteArrays:
    """Same site columns as tests/property/test_weather_pv.py (a site's values depend on its id)."""
    n = len(ids)
    return SiteArrays(
        keys=np.array([rng.location_key(f"PL-LOC-{i:06d}") for i in ids], dtype=np.uint64),
        lat=np.array([49.5 + (i * 0.37) % 5 for i in ids]),
        lon=np.array([14.5 + (i * 0.71) % 9.5 for i in ids]),
        elevation_m=np.full(n, 120.0),
        shear_alpha=np.full(n, 0.2),
        wind_scale_ms=np.full(n, 3.4),
    )


def lead_bin(lo_h: float, hi_h: float) -> NDArray[np.bool_]:
    return (lo_h < LEAD_H) & (hi_h >= LEAD_H)


@dataclass(frozen=True)
class Series:
    """60 days of runs for one site, as [run, step] arrays aligned on each run's own steps."""

    kt_err: F64
    temp_err: F64
    wind_err: F64  # log(forecast / truth) at 10 m
    day: NDArray[np.bool_]  # truth clear-sky GHI > 0 at the step's valid time
    wind_ok: NDArray[np.bool_]  # truth 10 m wind > 0
    phase: F64
    ghi: F64
    cs: F64


def make_series() -> Series:
    site = sites([1])
    n_axis = (RUNS - 1) * CADENCE_Q + STEPS + truth_margin_q(PARAMS)
    truth = generate_truth(site, TimeAxis.from_range(T0, n_axis), SEED, PARAMS)
    cols: dict[str, list[F64]] = {k: [] for k in ("kt", "temp", "wind", "phase", "ghi", "cs", "tw")}
    for r in range(RUNS):
        i0 = r * CADENCE_Q
        sl = slice(i0, i0 + STEPS)
        run: ForecastRun = forecast_run(truth, T0 + i0, STEPS, SEED, site.keys, PARAMS)
        cols["kt"].append(run.kt_error[0])
        cols["temp"].append(run.temp_c[0] - truth.temp_c[0, sl])
        tw = truth.wind10[0, sl]
        cols["tw"].append(tw)
        with np.errstate(divide="ignore", invalid="ignore"):
            cols["wind"].append(np.log(run.wind10[0] / tw))
        cols["phase"].append(run.phase_h[0])
        cols["ghi"].append(run.ghi[0])
        cols["cs"].append(truth.geometry.cs_ghi[0, sl])
    cs = np.stack(cols["cs"])
    return Series(
        kt_err=np.stack(cols["kt"]),
        temp_err=np.stack(cols["temp"]),
        wind_err=np.stack(cols["wind"]),
        day=cs > 0,
        wind_ok=np.stack(cols["tw"]) > 0,
        phase=np.stack(cols["phase"]),
        ghi=np.stack(cols["ghi"]),
        cs=cs,
    )


@pytest.fixture(scope="module")
def series() -> Series:
    """Computed once per module: every statistic below reads the same 480 runs."""
    return make_series()


def binned_sd(err: F64, ok: NDArray[np.bool_], lo_h: float, hi_h: float) -> float:
    m = ok & lead_bin(lo_h, hi_h)[None, :]
    return float(np.std(err[m]))


def consecutive_corr(err: F64, ok: NDArray[np.bool_], lo_h: float, hi_h: float) -> float:
    """Pearson correlation of runs r and r+1 at one valid time, binned by the newer run's lead.

    The newer run (issued 3 h later) has the valid time at step k, the older one at step k + 12.
    """
    k = np.flatnonzero(lead_bin(lo_h, hi_h))
    assert k.size and k.max() + CADENCE_Q < STEPS
    newer, older = err[1:, k], err[:-1, k + CADENCE_Q]
    m = ok[1:, k] & ok[:-1, k + CADENCE_Q]
    return float(np.corrcoef(older[m], newer[m])[0, 1])


BINS_3H = [(lo, lo + 3) for lo in range(3, 45, 3)]  # newer run's lead, 3 h … 45 h
BINS_6H = [(lo, lo + 6) for lo in range(0, 48, 6)]


def test_ac_007_02_consecutive_runs_agree_on_the_kt_error(series: Series) -> None:
    """AC-007-02: correlation >= 0.7 in every 3 h bin of the newer run's lead, 3-45 h (daylight)."""
    corr = {b: round(consecutive_corr(series.kt_err, series.day, *b), 3) for b in BINS_3H}
    assert min(corr.values()) >= 0.7, corr


def test_ac_007_03_kt_error_sd_grows_weakly_within_the_published_band(series: Series) -> None:
    """AC-007-03: sd at 42-48 h >= 1.1 x sd at 0-6 h; every 6 h bin within 0.17-0.30 (kt units)."""
    sd = {b: round(binned_sd(series.kt_err, series.day, *b), 4) for b in BINS_6H}
    assert sd[(42, 48)] >= 1.1 * sd[(0, 6)], sd
    assert all(0.17 <= v <= 0.30 for v in sd.values()), sd


@pytest.mark.parametrize("var", ["temp", "wind"])
def test_ac_007_04_temperature_and_wind_share_the_run_structure(series: Series, var: str) -> None:
    """AC-007-04 (plan: temperature and wind get the shared-plus-per-run structure): AC-007-02 holds
    for the error (temperature: forecast - truth in C; wind: log(forecast / truth) at 10 m) and its
    sd at 42-48 h is >= 1.3 x the sd at 0-6 h. All intervals count, day and night.
    """
    err = series.temp_err if var == "temp" else series.wind_err
    ok = np.ones_like(series.day) if var == "temp" else series.wind_ok
    corr = {b: round(consecutive_corr(err, ok, *b), 3) for b in BINS_3H}
    assert min(corr.values()) >= 0.7, corr
    early, late = binned_sd(err, ok, 0, 6), binned_sd(err, ok, 42, 48)
    assert late >= 1.3 * early, (var, early, late)


def test_ac_007_05_phase_error_is_bounded_and_grows_with_lead(series: Series) -> None:
    """AC-007-05: |delta| <= delta_max at every step, delta(issue) = 0, sd at 42-48 h >= 2 x sd at
    0-6 h, the warp k + 4*delta_k is non-decreasing and >= 0 in every run (never samples the truth
    before the issue time), and forecast GHI is 0 wherever the clear-sky GHI is 0.
    """
    dmax = float(PARAMS["forecast"]["phase_max_h"])
    phase = series.phase
    assert np.abs(phase).max() <= dmax
    assert np.allclose(phase[:, 0], 0.0, rtol=0.0, atol=1e-12)
    early = float(phase[:, lead_bin(0, 6)].std())
    late = float(phase[:, lead_bin(42, 48)].std())
    assert late >= 2.0 * early, (early, late)
    warp = np.arange(STEPS)[None, :] + 4.0 * phase  # pos_k - i0, in quarter-hours
    assert (np.diff(warp, axis=1) >= -WARP_EPS_Q).all()
    assert (warp >= -WARP_EPS_Q).all()
    night = series.cs == 0
    assert night.any() and (series.ghi[night] == 0).all()


# ── AC-007-06: purity (hypothesis, fixed CI profile from tests/conftest.py) ───────────────────────


def _run(ids: list[int], issue: int, pre_q: int, seed: int) -> ForecastRun:
    s = sites(ids)
    axis = TimeAxis.from_range(issue - pre_q, pre_q + STEPS + truth_margin_q(PARAMS))
    return forecast_run(generate_truth(s, axis, seed, PARAMS), issue, STEPS, seed, s.keys, PARAMS)


@given(
    seed=st.integers(0, 2**31),
    day=st.integers(0, 364),
    slot=st.integers(0, 7),
    pre_q=st.integers(0, 96),
)
def test_ac_007_06_runs_are_bit_identical_alone_and_in_a_batch(
    seed: int, day: int, slot: int, pre_q: int
) -> None:
    """AC-007-06: a site's run alone equals its row in a batch of 4, bit for bit, in every field."""
    issue = T_YEAR + 96 * day + CADENCE_Q * slot
    alone = _run([2], issue, pre_q, seed)
    batch = _run([1, 2, 3, 4], issue, pre_q, seed)
    for name in FIELDS:
        assert np.array_equal(getattr(alone, name)[0], getattr(batch, name)[1]), name


@given(seed=st.integers(0, 2**31), day=st.integers(0, 364), slot=st.integers(0, 7))
def test_ac_007_06_same_seed_same_values(seed: int, day: int, slot: int) -> None:
    """AC-007-06: the same seed reproduces every value; another seed changes the errors."""
    issue = T_YEAR + 96 * day + CADENCE_Q * slot
    one, two = _run([1, 5], issue, 0, seed), _run([1, 5], issue, 0, seed)
    for name in FIELDS:
        assert np.array_equal(getattr(one, name), getattr(two, name)), name
    other = _run([1, 5], issue, 0, seed + 1)
    assert not np.array_equal(one.phase_h, other.phase_h)
    assert not np.array_equal(one.kt_error, other.kt_error)


@given(seed=st.integers(0, 2**31), day=st.integers(0, 364), pre_q=st.integers(1, 192))
def test_ac_007_06_errors_do_not_depend_on_the_truth_window(
    seed: int, day: int, pre_q: int
) -> None:
    """AC-007-06: the phase and the temperature and wind errors of a run are keyed by (run, step)
    and valid time, never by the position of the run in the truth axis.
    """
    issue = T_YEAR + 96 * day + CADENCE_Q * (day % 8)
    s = sites([3])
    margin = truth_margin_q(PARAMS)
    short = generate_truth(s, TimeAxis.from_range(issue, STEPS + margin), seed, PARAMS)
    long = generate_truth(
        s, TimeAxis.from_range(issue - pre_q, pre_q + STEPS + margin), seed, PARAMS
    )
    a = forecast_run(short, issue, STEPS, seed, s.keys, PARAMS)
    b = forecast_run(long, issue, STEPS, seed, s.keys, PARAMS)
    assert np.array_equal(a.phase_h, b.phase_h)
    sl = slice(pre_q, pre_q + STEPS)
    np.testing.assert_allclose(
        a.temp_c - short.temp_c[:, :STEPS], b.temp_c - long.temp_c[:, sl], rtol=0, atol=1e-9
    )
    np.testing.assert_allclose(
        np.log(a.wind10 / short.wind10[:, :STEPS]),
        np.log(b.wind10 / long.wind10[:, sl]),
        rtol=0,
        atol=1e-9,
    )


@settings(max_examples=10)
@given(seed=st.integers(0, 2**31), day=st.integers(0, 364))
def test_ac_007_06_runs_are_bit_identical_under_any_site_chunk(seed: int, day: int) -> None:
    """AC-007-06: ``scale.site_chunk`` 1 and 2000 and a site alone give identical runs, bitwise."""
    fleet = build_catalog(4, catalog_seed=7, global_seed=4242)
    t0 = T_YEAR + 96 * day
    issues = [t0, t0 + CADENCE_Q, t0 + 7 * CADENCE_Q]
    one = SyntheticModel(fleet, seed, CONFIG, site_chunk=1).chunk(t0, 96, issues).forecasts
    big = SyntheticModel(fleet, seed, CONFIG, site_chunk=2000).chunk(t0, 96, issues).forecasts
    alone = SyntheticModel([fleet[2]], seed, CONFIG).chunk(t0, 96, issues).forecasts
    for issue in issues:
        for name in FIELDS:
            assert np.array_equal(getattr(one[issue], name), getattr(big[issue], name)), name
            assert np.array_equal(getattr(alone[issue], name)[0], getattr(big[issue], name)[2]), (
                name
            )


@settings(max_examples=10)
@given(seed=st.integers(0, 2**31), day=st.integers(0, 364), split=st.integers(1, 7))
def test_ac_007_06_window_split_matches_within_the_g1_residual(
    seed: int, day: int, split: int
) -> None:
    """AC-007-06: runs issued in [a, c) equal those from [a, b) followed by [b, c): the phase
    exactly (knots are keyed by index), the rest within the G1 burn-in residual of the truth
    (G1_TOL).
    """
    fleet = build_catalog(2, catalog_seed=7, global_seed=4242)
    a = T_YEAR + 96 * day
    b, c = a + CADENCE_Q * split, a + 96

    def runs(lo: int, hi: int) -> dict[int, ForecastRun]:
        issues = list(range(lo, hi, CADENCE_Q))
        return SyntheticModel(fleet, seed, CONFIG).chunk(lo, hi - lo, issues).forecasts

    whole = runs(a, c)
    parts = {**runs(a, b), **runs(b, c)}
    assert sorted(parts) == sorted(whole) == list(range(a, c, CADENCE_Q))
    for issue, run in whole.items():
        other = parts[issue]
        assert np.array_equal(run.phase_h, other.phase_h), issue
        for name, tol in G1_TOL.items():
            diff = float(np.abs(getattr(run, name) - getattr(other, name)).max())
            assert diff <= tol, (name, issue, diff, tol)
