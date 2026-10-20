"""Base load from BDEW 2025 standard profiles (demandlib), on Polish local time and holidays.

H25 serves G-tariffs (households), G25 C-tariffs. Profiles are built on a tz-aware Europe/Warsaw
15-minute index (92/100-slot DST days), renormalised to sum exactly to one per local year, and
scaled to each site's annual consumption with mean-one lognormal noise.
"""

from __future__ import annotations

import warnings
from functools import lru_cache

import holidays
import numpy as np
import pandas as pd
from demandlib import bdew
from numpy.typing import NDArray

from ems.contracts.time import QUARTER_S
from ems.ingest import rng
from ems.ingest.localtime import local_year

F64 = NDArray[np.float64]
I64 = NDArray[np.int64]

PROFILES = {"H25": bdew.H25, "G25": bdew.G25}


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


def base_load_kwh(
    seed: int,
    keys: NDArray[np.uint64],
    profiles: list[str],
    annual_kwh: F64,
    t: I64,
    interval_sd: float = 0.20,
    daily_sd: float = 0.10,
) -> F64:
    """Base-load energy per interval [n_loc, n_steps] in kWh."""
    shape = np.empty((keys.size, t.size), dtype=np.float64)
    for p in sorted(set(profiles)):
        rows = np.array([i for i, q in enumerate(profiles) if q == p])
        shape[rows] = shares(p, t)[None, :]
    days = (t // 96).astype(np.int64)
    z_day = rng.normal(seed, keys, "load.daily", np.unique(days))
    z_day = z_day[:, np.searchsorted(np.unique(days), days)]
    z = rng.normal(seed, keys, "load.interval", t)
    noise = np.exp(daily_sd * z_day - daily_sd**2 / 2) * np.exp(
        interval_sd * z - interval_sd**2 / 2
    )
    return annual_kwh.reshape(-1, 1) * shape * noise
