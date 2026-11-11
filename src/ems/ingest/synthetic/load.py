"""Base load from BDEW 2025 standard profiles (demandlib), on Polish local time and holidays.

H25 serves G-tariffs (households), G25 C-tariffs. Profiles are built on a tz-aware Europe/Warsaw
15-minute index (92/100-slot DST days), renormalised to sum exactly to one per local year.

Spec 007: ``E(t) = A*(1-s)*shape(t)*D(day)*exp(sd*X(t) - sd^2/2) + I(t)``, with a mean-one daily
factor per Europe/Warsaw delivery day, AR(1) noise in log space (hour-scale persistence) and
appliance impulses whose start probability follows the profile share, so the expected annual energy
stays A.
Every value is a pure function of (seed, site, variable, t); impulses that start before a window and
spill into it are drawn too, so splitting a window never changes a value.
"""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import holidays
import numpy as np
import pandas as pd
from demandlib import bdew
from numpy.typing import NDArray
from ruamel.yaml import YAML

from ems.contracts.time import QUARTER_S
from ems.ingest import rng
from ems.ingest.localtime import local_unix, local_year

F64 = NDArray[np.float64]
I64 = NDArray[np.int64]

PROFILES = {"H25": bdew.H25, "G25": bdew.G25}
MAX_IMPULSE_SHARE = 0.4
DAYS_PER_YEAR = 365.25


@lru_cache(maxsize=32)
def yearly_shape(profile: str, year: int) -> tuple[I64, F64]:
    """(quarter index, energy share) for every slot of a local year; shares sum to 1."""
    idx = pd.date_range(
        f"{year}-01-01", f"{year + 1}-01-01", freq="15min", tz="Europe/Warsaw", inclusive="left"
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        series = PROFILES[profile](idx, holidays=holidays.Poland(years=[year - 1, year, year + 1]))
    values = np.asarray(series, dtype=np.float64)
    share = values / values.sum()
    t = (idx.tz_convert("UTC").as_unit("s").asi8 // QUARTER_S).astype(np.int64)
    return t, share


def shares(profile: str, t: I64) -> F64:
    """Energy share of each quarter ``t`` within its local year."""
    years = local_year((t * QUARTER_S).astype(np.float64))
    out = np.empty(t.size, dtype=np.float64)
    for year in np.unique(years):
        ts, sh = yearly_shape(str(profile), int(year))
        mask = years == year
        pos = np.searchsorted(ts, t[mask])
        out[mask] = sh[pos]
    return out


# ── parameters ────────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ImpulseKind:
    name: str
    p_kw: float
    dur_min: float
    rate_per_day: float  # starts per reference household and day, ownership included

    @property
    def energy_kwh(self) -> float:
        return self.p_kw * self.dur_min / 60.0

    @property
    def lags(self) -> int:
        """Quarter-hours after its start quarter that an impulse can still reach."""
        return math.ceil(self.dur_min / 15.0)


@dataclass(frozen=True)
class NoiseParams:
    tau_h: float  # fast component (the only one when slow_share is 0)
    interval_sd: float
    daily_sd: float
    impulses: bool
    tau_slow_h: float = 0.0  # slow component; used when slow_share > 0
    slow_share: float = 0.0  # share of the noise variance in the slow component


@dataclass(frozen=True)
class LoadParams:
    reference_annual_kwh: float
    kinds: tuple[ImpulseKind, ...]
    profiles: dict[str, NoiseParams]

    @property
    def impulse_share(self) -> float:
        """s: expected annual impulse energy over the reference household's annual energy."""
        energy = sum(k.rate_per_day * DAYS_PER_YEAR * k.energy_kwh for k in self.kinds)
        return energy / self.reference_annual_kwh

    def share_for(self, profile: str) -> float:
        return self.impulse_share if self.profiles[profile].impulses else 0.0


def parse_load_params(raw: dict[str, Any], max_annual_kwh: float = 7000.0) -> LoadParams:
    """Parse ``load.yaml``; fail fast where the mean could break (s > 0.4, or a start probability
    above 1 at the largest household energy).
    """
    ref = float(raw["reference_annual_kwh"])
    kinds = tuple(
        ImpulseKind(str(name), float(v["p_kw"]), float(v["dur_min"]), float(v["rate_per_day"]))
        for name, v in (raw.get("impulses") or {}).items()
    )
    profiles = {
        str(name): NoiseParams(
            float(v["tau_h"]),
            float(v["interval_sd"]),
            float(v["daily_sd"]),
            bool(v["impulses"]),
            float(v.get("tau_slow_h", 0.0)),
            float(v.get("slow_share", 0.0)),
        )
        for name, v in raw["profiles"].items()
    }
    unknown = set(profiles) - set(PROFILES)
    if unknown:
        raise ValueError(
            f"load.yaml: unknown profiles {sorted(unknown)}; allowed {sorted(PROFILES)}"
        )
    if ref <= 0 or any(k.p_kw <= 0 or k.dur_min <= 0 or k.rate_per_day < 0 for k in kinds):
        raise ValueError("load.yaml: reference energy, impulse power and duration must be positive")
    for name, noise in profiles.items():
        if not 0.0 <= noise.slow_share <= 1.0 or noise.tau_h <= 0:
            raise ValueError(f"load.yaml: {name}: tau_h > 0 and slow_share in [0, 1] required")
        if noise.slow_share > 0 and noise.tau_slow_h <= noise.tau_h:
            raise ValueError(f"load.yaml: {name}: tau_slow_h must exceed tau_h")
    params = LoadParams(ref, kinds, profiles)
    if params.impulse_share > MAX_IMPULSE_SHARE:
        raise ValueError(
            f"load.yaml: impulse share {params.impulse_share:.3f} exceeds {MAX_IMPULSE_SHARE}"
        )
    for name, noise in profiles.items():
        if not noise.impulses:
            continue
        peak_share = float(yearly_shape(name, 2026)[1].max())
        for k in kinds:
            q = k.rate_per_day * (max_annual_kwh / ref) * 366 * peak_share
            if q > 1.0:
                raise ValueError(
                    f"load.yaml: {k.name} start probability {q:.2f} > 1 at {max_annual_kwh} kWh/yr"
                )
    return params


@lru_cache(maxsize=4)
def load_load_params(path: Path) -> LoadParams:
    return parse_load_params(YAML(typ="safe").load(path.read_text()))


# ── components ────────────────────────────────────────────────────────────────────────────────────


def local_day_index(t: I64) -> I64:
    """Europe/Warsaw delivery day (days since the local epoch) of each interval start ``t``."""
    local = local_unix((np.asarray(t, dtype=np.int64) * QUARTER_S).astype(np.float64))
    return np.floor(local / 86400.0).astype(np.int64)


def days_in_local_year(t: I64) -> F64:
    years = local_year((np.asarray(t, dtype=np.int64) * QUARTER_S).astype(np.float64))
    leap = ((years % 4 == 0) & (years % 100 != 0)) | (years % 400 == 0)
    return np.where(leap, 366.0, 365.0)


def daily_factor(seed: int, keys: NDArray[np.uint64], t: I64, sd: float) -> F64:
    """Mean-one lognormal factor [n_loc, n], one draw per site and local delivery day."""
    days = local_day_index(t)
    uniq, inv = np.unique(days, return_inverse=True)
    z = rng.normal(seed, keys, "load.daily", uniq)[:, inv]
    return np.asarray(np.exp(sd * z - sd**2 / 2), dtype=np.float64)


def log_noise(
    seed: int,
    keys: NDArray[np.uint64],
    t: I64,
    sd: float,
    tau_h: float,
    tau_slow_h: float = 0.0,
    slow_share: float = 0.0,
) -> F64:
    """Log of the interval noise factor [n_loc, n]: sd*X - sd^2/2.

    X = sqrt(1 - w) * X_fast + sqrt(w) * X_slow with unit AR(1) components (time constants tau_h
    and tau_slow_h, w = slow_share), so X has unit variance and its autocorrelation at lag L is
    (1 - w) * phi_fast^L + w * phi_slow^L (``noise_acf``). Measured household load has both a
    sub-hour and a multi-hour time scale (WPuQ fit, spec 007).
    """
    t = np.asarray(t, dtype=np.int64)
    if t.size > 1 and not np.all(np.diff(t) == 1):
        raise ValueError("base load needs a contiguous time axis")
    x = rng.ar1(seed, keys, "load.ar", int(t[0]), int(t.size), rng.phi_for(tau_h))
    if slow_share > 0:
        slow = rng.ar1(seed, keys, "load.ar.slow", int(t[0]), int(t.size), rng.phi_for(tau_slow_h))
        x = np.sqrt(1.0 - slow_share) * x + np.sqrt(slow_share) * slow
    return np.asarray(sd * x - sd**2 / 2, dtype=np.float64)


def noise_acf(lags_q: F64, tau_h: float, tau_slow_h: float = 0.0, slow_share: float = 0.0) -> F64:
    """Autocorrelation of X at lags in quarter-hours (what the fit and the tests compare to)."""
    lags = np.asarray(lags_q, dtype=np.float64)
    fast = rng.phi_for(tau_h) ** lags
    if slow_share <= 0:
        return np.asarray(fast, dtype=np.float64)
    return np.asarray((1.0 - slow_share) * fast + slow_share * rng.phi_for(tau_slow_h) ** lags)


@dataclass(frozen=True)
class ImpulseDraws:
    starts: NDArray[np.bool_]  # [n_loc, n]: an impulse of this kind starts in quarter t
    energy_by_lag: F64  # [n_lags, n_loc, n]: kWh in quarter t from the impulse started lag earlier

    @property
    def energy(self) -> F64:
        out = self.energy_by_lag[0].copy()
        for lag in range(1, self.energy_by_lag.shape[0]):  # fixed order: bit-identical per quarter
            out += self.energy_by_lag[lag]
        return out


def impulses(
    seed: int,
    keys: NDArray[np.uint64],
    profiles: list[str],
    annual_kwh: F64,
    t: I64,
    params: LoadParams,
) -> dict[str, ImpulseDraws]:
    """Appliance impulses per kind for the window ``t`` (contiguous quarter indices)."""
    t = np.asarray(t, dtype=np.int64)
    n_loc, n = int(keys.size), int(t.size)
    t0 = int(t[0])
    active = np.array([params.profiles[p].impulses for p in profiles], dtype=bool)
    scale = np.asarray(annual_kwh, dtype=np.float64) / params.reference_annual_kwh
    out: dict[str, ImpulseDraws] = {}
    shape_cache: dict[tuple[str, int], F64] = {}
    for kind in params.kinds:
        lags = kind.lags
        ts = np.arange(t0 - lags, t0 + n, dtype=np.int64)
        share = np.zeros((n_loc, ts.size))
        for p in sorted({p for p, a in zip(profiles, active, strict=True) if a}):
            if (p, lags) not in shape_cache:
                shape_cache[(p, lags)] = shares(p, ts) * days_in_local_year(ts)
            rows = np.array([q == p for q in profiles])
            share[rows] = shape_cache[(p, lags)][None, :]
        q = kind.rate_per_day * scale[:, None] * share
        start = rng.uniform(seed, keys, f"load.imp.{kind.name}", ts) < q
        offset = rng.uniform(seed, keys, f"load.imp.{kind.name}.offset", ts) * 15.0
        energy = np.zeros((lags + 1, n_loc, n))
        for lag in range(lags + 1):  # overlap of [offset, offset + dur) with quarter `lag`
            lo, hi = 15.0 * lag, 15.0 * (lag + 1)
            overlap = np.clip(
                np.minimum(offset + kind.dur_min, hi) - np.maximum(offset, lo), 0, None
            )
            e = np.where(start, kind.p_kw * overlap / 60.0, 0.0)
            energy[lag] = e[:, lags - lag : lags - lag + n]
        out[kind.name] = ImpulseDraws(starts=start[:, lags:], energy_by_lag=energy)
    return out


def base_load_kwh(
    seed: int,
    keys: NDArray[np.uint64],
    profiles: list[str],
    annual_kwh: F64,
    t: I64,
    params: LoadParams,
) -> F64:
    """Base-load energy per interval [n_loc, n_steps] in kWh."""
    t = np.asarray(t, dtype=np.int64)
    annual = np.asarray(annual_kwh, dtype=np.float64)
    out = np.zeros((keys.size, t.size), dtype=np.float64)
    for p in sorted(set(profiles)):
        rows = np.array([i for i, q in enumerate(profiles) if q == p])
        noise = params.profiles[p]
        smooth = annual[rows, None] * (1.0 - params.share_for(p)) * shares(p, t)[None, :]
        factor = daily_factor(seed, keys[rows], t, noise.daily_sd) * np.exp(
            log_noise(
                seed,
                keys[rows],
                t,
                noise.interval_sd,
                noise.tau_h,
                noise.tau_slow_h,
                noise.slow_share,
            )
        )
        out[rows] = smooth * factor
    for draws in impulses(seed, keys, profiles, annual, t, params).values():
        out += draws.energy
    return out
