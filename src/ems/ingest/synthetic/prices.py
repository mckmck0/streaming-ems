"""Synthetic RCE-like day-ahead prices driven by the same national weather process as the sites.

Prices depend on season, day type, a national PV index (midday dip, negative on sunny spring and
summer weekends) and a national wind index; AR noise and rare evening spikes add texture. Everything
is keyed by (seed, step), never by the sites being generated.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import holidays
import numpy as np
from numpy.typing import NDArray

from ems.contracts.time import WARSAW, local_day_bounds, quarter_index
from ems.ingest import rng
from ems.ingest.synthetic.weather import SiteArrays, TimeAxis, generate_truth, load_params

F64 = NDArray[np.float64]
REFERENCE_KEY = rng.location_key("national-reference")


@lru_cache(maxsize=2)
def price_params(path: Path) -> dict[str, Any]:
    return load_params(path)


def _bump(hour: F64, centre: float, width: float) -> F64:
    return np.exp(-(((hour - centre) / width) ** 2))


def price_curve(t0: int, n: int, seed: int, prices: dict[str, Any], weather: dict[str, Any]) -> F64:
    """PLN/MWh for quarter-hours ``t0 .. t0+n-1`` (1-D)."""
    ref = prices["reference_site"]
    site = SiteArrays(
        keys=np.array([REFERENCE_KEY], dtype=np.uint64),
        lat=np.array([float(ref["lat"])]),
        lon=np.array([float(ref["lon"])]),
        elevation_m=np.array([float(ref["elevation_m"])]),
        shear_alpha=np.array([0.2]),
        wind_scale_ms=np.array([3.4]),
    )
    axis = TimeAxis.from_range(t0, n)
    truth = generate_truth(site, axis, seed, weather)
    pv_index = truth.geometry.cs_ghi[0] * truth.national_kt / 1000.0
    wind_index = np.clip((truth.national_wind100 / 11.0) ** 3, 0.0, 1.0)

    hours = np.empty(n)
    months = np.empty(n, dtype=np.int64)
    day_factor = np.empty(n)
    pl_holidays = holidays.Poland(years=range(2000, 2100))
    for i in range(n):  # calendar lookup per step (no per-site work)
        lt = datetime.fromtimestamp(float(axis.unix_mid[i]), tz=WARSAW)
        hours[i] = lt.hour + lt.minute / 60.0
        months[i] = lt.month
        d = lt.date()
        if d in pl_holidays:
            day_factor[i] = float(prices["holiday_factor"])
        elif d.weekday() >= 5:
            day_factor[i] = float(prices["weekend_factor"])
        else:
            day_factor[i] = 1.0
    shp = prices["shape"]
    shape = (
        1.0
        + shp["morning_peak"]["height"]
        * _bump(hours, shp["morning_peak"]["hour"], shp["morning_peak"]["width"])
        + shp["evening_peak"]["height"]
        * _bump(hours, shp["evening_peak"]["hour"], shp["evening_peak"]["width"])
        - shp["night_dip"]["depth"]
        * _bump(hours, shp["night_dip"]["hour"], shp["night_dip"]["width"])
    )
    base = np.asarray(prices["base_by_month"], dtype=np.float64)[months - 1]
    solar = np.asarray(prices["solar_coef_by_month"], dtype=np.float64)[months - 1]
    noise = rng.ar1(
        seed, [rng.NATIONAL_KEY], "price.noise", t0, n, rng.phi_for(float(prices["noise_tau_h"]))
    )[0]
    spike_u = rng.uniform(seed, [rng.NATIONAL_KEY], "price.spike", np.arange(t0, t0 + n))[0]
    spike_size = rng.uniform(seed, [rng.NATIONAL_KEY], "price.spike.size", np.arange(t0, t0 + n))[0]
    lo, hi = prices["spike_range"]
    evening = (hours >= 16) & (hours <= 21)
    spikes = np.where(
        (spike_u < float(prices["spike_probability"])) & evening, lo + (hi - lo) * spike_size, 0.0
    )
    price = (
        base * shape * day_factor
        - solar * pv_index
        - float(prices["wind_coef"]) * wind_index
        + float(prices["noise_sd"]) * noise
        + spikes
    )
    return np.round(np.clip(price, float(prices["floor"]), float(prices["cap"])), 2)


def day_prices(day: date, seed: int, prices: dict[str, Any], weather: dict[str, Any]) -> F64:
    """Quarter-hour prices for a local delivery day (92/96/100 values)."""
    start, end = local_day_bounds(day)
    t0 = quarter_index(start)
    return price_curve(t0, quarter_index(end) - t0, seed, prices, weather)


def publication_time(day: date, seed: int, prices: dict[str, Any]) -> datetime:
    """Deterministic D-1 publication instant (UTC), drawn from the observed local-time bands."""
    bands = prices["publication"]["bands"]
    u = float(
        rng.uniform(seed, [rng.NATIONAL_KEY], "price.publication", np.array([day.toordinal()]))[
            0, 0
        ]
    )
    v = float(
        rng.uniform(
            seed, [rng.NATIONAL_KEY], "price.publication.minute", np.array([day.toordinal()])
        )[0, 0]
    )
    cum = 0.0
    lo, hi = bands[-1][0], bands[-1][1]
    for b_lo, b_hi, p in bands:
        cum += p
        if u <= cum:
            lo, hi = b_lo, b_hi
            break
    hour = lo + (hi - lo) * v
    prev = day - timedelta(days=1)
    local = datetime(prev.year, prev.month, prev.day, tzinfo=WARSAW) + timedelta(hours=hour)
    return datetime.fromtimestamp(int(local.timestamp()), tz=UTC)
