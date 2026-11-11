"""AC-007-11 … -16: base load with two-timescale log noise, impulses and a local-day factor.

Spec 007, tasks T04 and T16 (AC-007-12 revised during implementation, 2026-11-10). Defaults come
from ``config/calibration/load.yaml``; custom and guard cases go through ``parse_load_params``. One
local year is 2026 (365 days, 35,040 quarter-hours); the full-load check uses 2019, the WPuQ year.
"""

from __future__ import annotations

import copy
import itertools
import math
from datetime import UTC, date, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray
from ruamel.yaml import YAML

from ems.contracts.time import QUARTER_S, local_day_bounds, quarter_index
from ems.ingest import rng
from ems.ingest.calibration import ACF_LAGS_Q, holiday_day_indices, household_stats
from ems.ingest.catalog.build import build_catalog, generate_plant, load_overrides
from ems.ingest.catalog.sites import Site, SiteType
from ems.ingest.generator import SyntheticModel
from ems.ingest.localtime import local_hour
from ems.ingest.synthetic.load import (
    ImpulseDraws,
    LoadParams,
    base_load_kwh,
    daily_factor,
    impulses,
    load_load_params,
    local_day_index,
    log_noise,
    noise_acf,
    parse_load_params,
    shares,
)

F64 = NDArray[np.float64]
I64 = NDArray[np.int64]

CONFIG = Path(__file__).resolve().parents[3] / "config"
SEED = 4242
N_HOUSEHOLDS = 50
DAYS_2026 = 365

# Risk G1: the AR(1) burn-in leaves a residual weight below exp(-6) on the window start. The two
# starting values it weighs are unit-variance draws, so their difference stays below 8 (about 5.7
# sd of a difference) and X moves by less than 8·exp(-6) ≈ 0.020 across a window split.
# Two AR(1) components move by at most (sqrt(1 - w) + sqrt(w)) <= sqrt(2) times one component's
# burn-in residual across a window split (risk G1).
G1_TOL_X = math.sqrt(2.0) * 8.0 * math.exp(-6.0)

# The documented load.yaml layout (spec 007 API contract), for custom and guard cases.
RAW: dict[str, Any] = {
    "reference_annual_kwh": 2300,
    "impulses": {
        "kettle": {"p_kw": 2.0, "dur_min": 3, "rate_per_day": 4.0},
        "oven": {"p_kw": 2.1, "dur_min": 27, "rate_per_day": 0.45},
        "washing_machine": {"p_kw": 2.0, "dur_min": 20, "rate_per_day": 0.41},
    },
    "profiles": {
        "H25": {"tau_h": 1.5, "interval_sd": 0.30, "daily_sd": 0.15, "impulses": True},
        "G25": {"tau_h": 1.5, "interval_sd": 0.20, "daily_sd": 0.10, "impulses": False},
    },
}


def _local_days(first: date, last: date) -> I64:
    """Interval starts of the Europe/Warsaw days ``first`` … ``last`` (inclusive)."""
    t0 = quarter_index(local_day_bounds(first)[0])
    t1 = quarter_index(local_day_bounds(last)[1])
    return np.arange(t0, t1, dtype=np.int64)


YEAR = _local_days(date(2026, 1, 1), date(2026, 12, 31))


@cache
def default_params() -> LoadParams:
    return load_load_params(CONFIG / "calibration" / "load.yaml")


def _keys(first: int, n: int) -> NDArray[np.uint64]:
    return np.array(
        [rng.location_key(f"PL-LOC-{i:06d}") for i in range(first, first + n)], dtype=np.uint64
    )


def _households(n: int = N_HOUSEHOLDS) -> tuple[NDArray[np.uint64], list[str], F64]:
    """n H25 households with annual energies spread over 1,500 … 4,500 kWh."""
    return _keys(1, n), ["H25"] * n, np.linspace(1500.0, 4500.0, n)


def _with_g25() -> tuple[NDArray[np.uint64], list[str], F64]:
    """The 50 households plus one G25 site (last row)."""
    keys, profiles, annual = _households()
    return (
        np.concatenate([keys, _keys(9001, 1)]),
        [*profiles, "G25"],
        np.concatenate([annual, [3000.0]]),
    )


@cache
def year_load() -> F64:
    keys, profiles, annual = _households()
    return base_load_kwh(SEED, keys, profiles, annual, YEAR, default_params())


@cache
def year_impulses() -> dict[str, ImpulseDraws]:
    keys, profiles, annual = _with_g25()
    return impulses(SEED, keys, profiles, annual, YEAR, default_params())


# ── defaults ──────────────────────────────────────────────────────────────────────────────────────


def test_ac_007_13_default_catalogue_has_the_three_kinds() -> None:
    p = default_params()
    kinds = {k.name: k for k in p.kinds}
    assert {"kettle", "oven", "washing_machine"} <= set(kinds)
    assert all(k.p_kw > 0 and k.dur_min > 0 and k.rate_per_day > 0 for k in p.kinds)
    assert p.profiles["H25"].impulses and not p.profiles["G25"].impulses
    expected_s = (
        sum(k.rate_per_day * 365.25 * k.p_kw * k.dur_min / 60 for k in p.kinds)
        / p.reference_annual_kwh
    )
    assert p.impulse_share == pytest.approx(expected_s, rel=1e-12)
    assert 0 < p.impulse_share <= 0.4


# ── AC-007-11: annual energy ──────────────────────────────────────────────────────────────────────


def test_ac_007_11_test_site_keeps_its_annual_energy() -> None:
    site = load_overrides(CONFIG / "sites.yaml", SEED)["PL-LOC-000001"]
    configured = float(site.annual_base_load_kwh)
    load = base_load_kwh(
        SEED,
        np.array([rng.location_key(site.location_id)], dtype=np.uint64),
        [site.profile],
        np.array([configured]),
        YEAR,
        default_params(),
    )
    assert load.shape == (1, YEAR.size) and (load > 0).all()
    ratio = float(load.sum()) / configured
    assert abs(ratio - 1) <= 0.05, ratio


def test_ac_007_11_fleet_mean_ratio_within_two_percent() -> None:
    _, _, annual = _households()
    ratios = year_load().sum(axis=1) / annual
    assert (np.abs(ratios - 1) <= 0.05).all(), ratios
    assert abs(float(ratios.mean()) - 1) <= 0.02, ratios.mean()


# ── AC-007-12: two-timescale log noise ────────────────────────────────────────────────────────────

NOISE_SITES = 10
NOISE_SD = 0.3
NOISE_LAGS_Q = (1, 4, 8)  # 15 min, 1 h, 2 h


def _unit_noise(sd: float, tau_h: float, tau_slow_h: float, slow_share: float) -> F64:
    """X = (log noise + sd^2/2) / sd for NOISE_SITES sites over YEAR."""
    x = log_noise(SEED, _keys(1, NOISE_SITES), YEAR, sd, tau_h, tau_slow_h, slow_share)
    assert x.shape == (NOISE_SITES, YEAR.size)
    return (x + sd**2 / 2) / sd


def _pooled_acf(x: F64, lags_q: tuple[int, ...]) -> F64:
    """Autocorrelation at ``lags_q`` with lagged pairs pooled over the sites (rows), not across."""
    xc = x - x.mean()
    den = float((xc * xc).sum())
    return np.array([float((xc[:, :-lag] * xc[:, lag:]).sum()) / den for lag in lags_q])


def _check_noise(sd: float, tau_h: float, tau_slow_h: float, slow_share: float) -> None:
    x = _unit_noise(sd, tau_h, tau_slow_h, slow_share)
    # Unit variance and zero mean in X, so exp(sd*X - sd^2/2) has mean one.
    assert abs(float(x.var()) - 1.0) <= 0.05, x.var()
    assert abs(float(x.mean())) <= 0.05, x.mean()
    got = _pooled_acf(x, NOISE_LAGS_Q)
    expected = noise_acf(np.array(NOISE_LAGS_Q, dtype=np.float64), tau_h, tau_slow_h, slow_share)
    gap = np.abs(got - expected)
    assert (gap <= 0.03).all(), (got.tolist(), expected.tolist())


@pytest.mark.parametrize(
    ("tau_h", "tau_slow_h", "slow_share"),
    [(1.5, 0.0, 0.0), (1.0, 0.0, 0.0), (0.66, 3.0, 0.44), (0.3, 3.0, 0.6)],
    ids=["ar1-1.5h", "ar1-1.0h", "h25-default", "fast-0.3h-w0.6"],
)
def test_ac_007_12_log_noise_acf_matches_noise_acf(
    tau_h: float, tau_slow_h: float, slow_share: float
) -> None:
    _check_noise(NOISE_SD, tau_h, tau_slow_h, slow_share)


def test_ac_007_12_noise_acf_is_the_two_component_mixture() -> None:
    lags = np.array(NOISE_LAGS_Q, dtype=np.float64)
    phi_fast, phi_slow = math.exp(-0.25 / 0.66), math.exp(-0.25 / 3.0)
    np.testing.assert_allclose(
        noise_acf(lags, 0.66, 3.0, 0.44),
        0.56 * phi_fast**lags + 0.44 * phi_slow**lags,
        rtol=1e-12,
        atol=0,
    )
    # slow_share 0 is the single AR(1), whatever tau_slow_h says.
    np.testing.assert_allclose(noise_acf(lags, 1.5, 3.0, 0.0), math.exp(-0.25 / 1.5) ** lags)


@pytest.mark.parametrize("profile", ["H25", "G25"])
def test_ac_007_12_configured_noise_acf_matches(profile: str) -> None:
    noise = default_params().profiles[profile]
    _check_noise(noise.interval_sd, noise.tau_h, noise.tau_slow_h, noise.slow_share)


def test_ac_007_12_h25_default_is_two_timescale_with_fixed_slow_tau() -> None:
    h25 = default_params().profiles["H25"]
    assert h25.tau_slow_h == 3.0 and 0 < h25.slow_share < 1 and h25.tau_h < h25.tau_slow_h
    assert default_params().profiles["G25"].slow_share == 0.0


# Full default load against the WPuQ statistics recorded in load.yaml (calibration_targets).
YEAR_2019 = _local_days(date(2019, 1, 1), date(2019, 12, 31))
FULL_LOAD_HOUSEHOLDS = 30


def _calibration_targets() -> dict[str, Any]:
    raw = YAML(typ="safe").load((CONFIG / "calibration" / "load.yaml").read_text())
    return dict(raw["calibration_targets"])


def test_ac_007_12_full_default_load_matches_wpuq_statistics() -> None:
    targets = _calibration_targets()
    assert tuple(targets["acf_lags_q"]) == ACF_LAGS_Q == (1, 2, 4, 8)
    assert YEAR_2019[0] == quarter_index(datetime(2018, 12, 31, 23, tzinfo=UTC))
    assert YEAR_2019[-1] + 1 == quarter_index(datetime(2019, 12, 31, 23, tzinfo=UTC))
    assert YEAR_2019.size == 35_040
    lo_kwh, hi_kwh = targets["annual_kwh_p10_p90"]
    n = FULL_LOAD_HOUSEHOLDS
    annual_kwh = np.linspace(float(lo_kwh), float(hi_kwh), n)
    load = base_load_kwh(SEED, _keys(1, n), ["H25"] * n, annual_kwh, YEAR_2019, default_params())
    holiday_days = holiday_day_indices([2019])
    stats = [household_stats(YEAR_2019, row, holiday_days) for row in load]
    assert all(s is not None for s in stats)
    measured = [s for s in stats if s is not None]

    def median(key: str) -> float:
        return float(np.median([s[key] for s in measured]))

    acf = np.array([median(f"acf_{lag}") for lag in ACF_LAGS_Q])
    target_acf = np.array(targets["acf"], dtype=np.float64)
    assert (np.abs(acf - target_acf) <= 0.05).all(), (acf.tolist(), target_acf.tolist())
    interval_sd, day_sd = median("interval_sd"), median("day_sd")
    assert abs(interval_sd - float(targets["interval_sd"])) <= 0.03, interval_sd
    assert abs(day_sd - float(targets["day_sd"])) <= 0.03, day_sd


# ── AC-007-13: appliance impulses ─────────────────────────────────────────────────────────────────


def test_ac_007_13_counts_per_kind_match_expected() -> None:
    p = default_params()
    _, _, annual = _households()
    draws = year_impulses()
    assert {k.name for k in p.kinds} == set(draws)
    for kind in p.kinds:
        count = int(draws[kind.name].starts[:N_HOUSEHOLDS].sum())
        expected = float((kind.rate_per_day * annual / p.reference_annual_kwh * DAYS_2026).sum())
        assert abs(count / expected - 1) <= 0.10, (kind.name, count, expected)


def test_ac_007_13_energy_per_impulse_and_quarter_bounded() -> None:
    p = default_params()
    draws = year_impulses()
    n_loc, n = N_HOUSEHOLDS + 1, YEAR.size
    for kind in p.kinds:
        d = draws[kind.name]
        e = d.energy_by_lag
        assert d.starts.shape == (n_loc, n) and e.shape[1:] == (n_loc, n), kind.name
        assert e.min() >= 0.0, kind.name
        assert e.max() <= kind.p_kw * 0.25 + 1e-12, (kind.name, e.max())
        # An impulse only touches the quarter-hours it overlaps.
        for lag in range(math.ceil(kind.dur_min / 15) + 1, e.shape[0]):
            assert not e[lag].any(), (kind.name, lag)
        # Every impulse that starts (and ends) inside the window lands exactly p·dur in total.
        last = e.shape[0] - 1
        per_start = sum(e[lag][:, lag : n - last + lag] for lag in range(e.shape[0]))
        expected = kind.p_kw * kind.dur_min / 60 * d.starts[:, : n - last]
        np.testing.assert_allclose(per_start, expected, rtol=0, atol=1e-12, err_msg=kind.name)
        np.testing.assert_allclose(d.energy, e.sum(axis=0), rtol=0, atol=1e-12)


def test_ac_007_13_g25_sites_get_no_impulses() -> None:
    draws = year_impulses()
    for name, d in draws.items():
        assert not d.starts[-1].any(), name
        assert not d.energy[-1].any(), name
        assert d.starts[:N_HOUSEHOLDS].any(), name


# ── AC-007-14: profile shape ──────────────────────────────────────────────────────────────────────


def test_ac_007_14_hourly_means_follow_h25() -> None:
    _, _, annual = _households()
    fleet = year_load().sum(axis=0)
    expected = float(annual.sum()) * shares("H25", YEAR)
    hour = np.floor(local_hour((YEAR * QUARTER_S).astype(np.float64))).astype(np.int64)
    for h in range(24):
        at = hour == h
        ratio = float(fleet[at].mean() / expected[at].mean())
        assert abs(ratio - 1) <= 0.10, (h, ratio)


# ── AC-007-15: daily factor on the Europe/Warsaw delivery day ─────────────────────────────────────

DST_DAYS = [(date(2026, 3, 28), [96, 92, 96]), (date(2026, 10, 24), [96, 100, 96])]


@pytest.mark.parametrize(("first", "slots"), DST_DAYS, ids=["spring", "autumn"])
def test_ac_007_15_local_days_have_92_96_100_slots(first: date, slots: list[int]) -> None:
    t = _local_days(first, first + timedelta(days=2))
    d = local_day_index(t)
    assert d.dtype == np.int64 and d.shape == t.shape
    uniq, counts = np.unique(d, return_counts=True)
    assert counts.tolist() == slots
    assert (np.diff(uniq) == 1).all() and (np.diff(d) >= 0).all()


@pytest.mark.parametrize(("first", "slots"), DST_DAYS, ids=["spring", "autumn"])
def test_ac_007_15_daily_factor_constant_per_local_day(first: date, slots: list[int]) -> None:
    t = _local_days(first, first + timedelta(days=2))
    f = daily_factor(SEED, _keys(1, 3), t, 0.15)
    assert f.shape == (3, t.size) and (f > 0).all()
    bounds = np.cumsum([0, *slots])
    for lo, hi in itertools.pairwise(bounds):
        assert (f[:, lo:hi] == f[:, lo : lo + 1]).all(), (lo, hi)
    # It changes only at local midnight, and there for every site.
    changes = f[:, 1:] != f[:, :-1]
    assert np.flatnonzero(changes.any(axis=0)).tolist() == [b - 1 for b in bounds[1:-1]]
    assert changes[:, bounds[1:-1] - 1].all()


def test_ac_007_15_daily_factor_changes_at_local_midnight_not_utc() -> None:
    t = _local_days(date(2026, 3, 28), date(2026, 10, 26))
    f = daily_factor(SEED, _keys(1, 3), t, 0.15)

    def at(hour: int, minute: int, day: date) -> F64:
        i = quarter_index(datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)) - int(
            t[0]
        )
        return f[:, i]

    # 2026-03-28T23:00Z is local midnight of 2026-03-29 (CET, UTC+1).
    assert (at(22, 45, date(2026, 3, 28)) != at(23, 0, date(2026, 3, 28))).all()
    # 00:45Z and 01:00Z on 2026-10-25 are 02:45 CEST and 02:00 CET: the same local day.
    assert (at(0, 45, date(2026, 10, 25)) == at(1, 0, date(2026, 10, 25))).all()


# ── AC-007-16: batch, chunk and window independence ───────────────────────────────────────────────


def _mixed() -> tuple[NDArray[np.uint64], list[str], F64]:
    """Six sites with both profiles in a mixed order."""
    profiles = ["H25", "G25", "H25", "H25", "G25", "H25"]
    return _keys(101, 6), profiles, np.array([1500.0, 4200.0, 2300.0, 7000.0, 900.0, 3100.0])


WEEK = _local_days(date(2026, 3, 26), date(2026, 4, 1))  # includes the 92-slot day


def test_ac_007_16_site_alone_equals_site_in_any_batch() -> None:
    keys, profiles, annual = _mixed()
    batch = base_load_kwh(SEED, keys, profiles, annual, WEEK, default_params())
    assert batch.shape == (6, WEEK.size)
    for k in range(6):
        alone = base_load_kwh(
            SEED, keys[k : k + 1], [profiles[k]], annual[k : k + 1], WEEK, default_params()
        )
        assert np.array_equal(alone[0], batch[k]), k
    reverse = base_load_kwh(SEED, keys[::-1], profiles[::-1], annual[::-1], WEEK, default_params())
    assert np.array_equal(reverse[::-1], batch)


def _sites() -> list[Site]:
    households = build_catalog(
        5,
        catalog_seed=7,
        global_seed=SEED,
        overrides=load_overrides(CONFIG / "sites.yaml", SEED),
    )
    commercial = generate_plant(1, SiteType.COMMERCIAL, catalog_seed=7, global_seed=SEED)
    return [*households, commercial]


def test_ac_007_16_site_chunk_keeps_base_load_bit_identical() -> None:
    sites = _sites()
    assert {s.profile for s in sites} == {"H25", "G25"}
    t0 = quarter_index(local_day_bounds(date(2026, 3, 29))[0])
    whole_model = SyntheticModel(sites, SEED, CONFIG)
    assert whole_model.load_params == default_params()
    whole = whole_model.chunk(t0, 92, [])
    sliced = SyntheticModel(sites, SEED, CONFIG, site_chunk=1).chunk(t0, 92, [])
    assert np.array_equal(whole.load_kwh, sliced.load_kwh)
    direct = base_load_kwh(
        SEED,
        np.array([rng.location_key(s.location_id) for s in sites], dtype=np.uint64),
        [s.profile for s in sites],
        np.array([s.annual_base_load_kwh for s in sites]),
        np.arange(t0, t0 + 92, dtype=np.int64),
        whole_model.load_params,
    )
    assert np.array_equal(whole.load_kwh, direct)


def _spill(draws: dict[str, ImpulseDraws]) -> tuple[str, int, int]:
    """(kind, site row, b): an impulse that starts in quarter b-1 and spills into quarter b."""
    for name, d in draws.items():
        rows, cols = np.nonzero(d.energy_by_lag[1] > 0)
        for r, c in zip(rows, cols, strict=True):
            if c >= 1 and d.starts[r, c - 1]:
                return name, int(r), int(c)
    raise AssertionError("no spilling impulse in the window")


def test_ac_007_16_impulses_bit_identical_across_window_split() -> None:
    keys, profiles, annual = _households(10)
    t = WEEK
    full = impulses(SEED, keys, profiles, annual, t, default_params())
    kind, row, b = _spill(full)
    left = impulses(SEED, keys, profiles, annual, t[:b], default_params())
    right = impulses(SEED, keys, profiles, annual, t[b:], default_params())
    for name, d in full.items():
        joined = np.concatenate([left[name].energy, right[name].energy], axis=1)
        assert np.array_equal(joined, d.energy), name
        assert np.array_equal(
            np.concatenate([left[name].starts, right[name].starts], axis=1), d.starts
        ), name
        assert np.array_equal(
            np.concatenate([left[name].energy_by_lag, right[name].energy_by_lag], axis=2),
            d.energy_by_lag,
        ), name
    # The impulse that started before [b, c) is still counted in it.
    assert right[kind].energy_by_lag[1, row, 0] > 0


def test_ac_007_16_ar_noise_within_g1_residual() -> None:
    keys, profiles, annual = _mixed()
    t, b = WEEK, 150  # split mid-afternoon of a normal day
    sd, tau_h = 0.3, 1.5
    full = log_noise(SEED, keys, t, sd, tau_h)
    split = np.concatenate(
        [log_noise(SEED, keys, t[:b], sd, tau_h), log_noise(SEED, keys, t[b:], sd, tau_h)], axis=1
    )
    assert float(np.abs(split - full).max()) <= sd * G1_TOL_X
    # The whole base load differs only through that AR residual.
    p = default_params()
    sd_max = max(n.interval_sd for n in p.profiles.values())
    whole = base_load_kwh(SEED, keys, profiles, annual, t, p)
    parts = np.concatenate(
        [
            base_load_kwh(SEED, keys, profiles, annual, t[:b], p),
            base_load_kwh(SEED, keys, profiles, annual, t[b:], p),
        ],
        axis=1,
    )
    np.testing.assert_allclose(parts, whole, rtol=math.expm1(sd_max * G1_TOL_X), atol=0)


# ── guards at parse ───────────────────────────────────────────────────────────────────────────────


def _write(tmp_path: Path, raw: dict[str, Any]) -> Path:
    path = tmp_path / "load.yaml"
    with path.open("w") as f:
        YAML(typ="safe").dump(raw, f)
    return path


def test_guard_documented_layout_parses(tmp_path: Path) -> None:
    p = parse_load_params(copy.deepcopy(RAW))
    assert p.reference_annual_kwh == 2300
    assert [k.name for k in p.kinds] == ["kettle", "oven", "washing_machine"]
    assert p.profiles["G25"].interval_sd == 0.20 and not p.profiles["G25"].impulses
    assert load_load_params(_write(tmp_path, RAW)) == p


def test_guard_impulse_share_above_0_4_fails_at_parse(tmp_path: Path) -> None:
    raw = copy.deepcopy(RAW)
    raw["impulses"]["washing_machine"]["dur_min"] = 150  # s ≈ 0.43; start rates unchanged
    s = (
        sum(
            k["rate_per_day"] * 365.25 * k["p_kw"] * k["dur_min"] / 60
            for k in raw["impulses"].values()
        )
        / raw["reference_annual_kwh"]
    )
    assert s > 0.4
    with pytest.raises(ValueError):
        parse_load_params(raw)
    with pytest.raises(ValueError):
        load_load_params(_write(tmp_path, raw))


def test_guard_start_probability_above_1_at_7000_kwh_fails_at_parse(tmp_path: Path) -> None:
    raw = copy.deepcopy(RAW)
    # 30 one-minute kettle runs a day: s ≈ 0.27, but q ≈ 0.63 at A_ref and ≈ 1.9 at 7,000 kWh
    # (peak H25 share ≈ 5.7e-5 per quarter-hour).
    raw["impulses"]["kettle"] = {"p_kw": 2.0, "dur_min": 1, "rate_per_day": 30.0}
    peak = float(shares("H25", YEAR).max())
    assert 30.0 * DAYS_2026 * peak < 1 < 30.0 * 7000 / 2300 * DAYS_2026 * peak
    with pytest.raises(ValueError):
        parse_load_params(raw)
    with pytest.raises(ValueError):
        load_load_params(_write(tmp_path, raw))
    # The guard is about the largest household: at the reference energy the same rates are fine.
    assert parse_load_params(raw, max_annual_kwh=2300.0).impulse_share <= 0.4


def _with_slow(tau_slow_h: float, slow_share: float) -> dict[str, Any]:
    """RAW with a slow component on H25 (tau_h 1.5)."""
    raw = copy.deepcopy(RAW)
    raw["profiles"]["H25"] |= {"tau_slow_h": tau_slow_h, "slow_share": slow_share}
    return raw


def test_ac_007_12_slow_component_parses() -> None:
    h25 = parse_load_params(_with_slow(3.0, 0.4)).profiles["H25"]
    assert (h25.tau_h, h25.tau_slow_h, h25.slow_share) == (1.5, 3.0, 0.4)
    g25 = parse_load_params(_with_slow(3.0, 0.4)).profiles["G25"]
    assert g25.slow_share == 0.0  # absent keys: the single AR(1)


@pytest.mark.parametrize(
    ("tau_slow_h", "slow_share"),
    [(3.0, -0.1), (3.0, 1.1), (1.5, 0.4), (1.0, 0.4)],
    ids=["share-below-0", "share-above-1", "slow-equals-fast", "slow-below-fast"],
)
def test_ac_007_12_bad_slow_component_fails_at_parse(
    tmp_path: Path, tau_slow_h: float, slow_share: float
) -> None:
    raw = _with_slow(tau_slow_h, slow_share)
    with pytest.raises(ValueError):
        parse_load_params(raw)
    with pytest.raises(ValueError):
        load_load_params(_write(tmp_path, raw))
