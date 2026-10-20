"""Thermal and hydro plants: heat-led CHP, near-baseload biogas, price-following biomass
with ramp limits,
run-of-river hydro with a seasonal flow curve. Vectorised over sites; recursions loop over time
only.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from ems.ingest import rng

F64 = NDArray[np.float64]
DT_H = 0.25


def chp_kwh(
    temp_c: F64,
    weekday: NDArray[np.bool_],
    rated_kw: F64,
    power_to_heat: F64,
    min_load: F64,
    heat_kw_per_k: F64,
    heat_base_kw: F64,
    availability: F64,
) -> tuple[F64, F64]:
    """Electric and heat energy per interval: heat-led, off below minimum load, capped at rating."""
    heat_demand = heat_kw_per_k.reshape(-1, 1) * np.clip(
        15.0 - temp_c, 0.0, None
    ) + heat_base_kw.reshape(-1, 1)
    heat_demand = heat_demand * np.where(weekday, 1.0, 0.85)[None, :]
    electric = heat_demand * power_to_heat.reshape(-1, 1)
    rated = rated_kw.reshape(-1, 1)
    running = electric >= min_load.reshape(-1, 1) * rated
    electric = np.where(running, np.minimum(electric, rated), 0.0) * availability
    heat = electric / power_to_heat.reshape(-1, 1)
    return electric * DT_H, heat * DT_H


def biogas_kwh(
    seed: int, keys: NDArray[np.uint64], t0: int, n: int, rated_kw: F64, availability: F64
) -> F64:
    noise = rng.ar1(seed, keys, "biogas", t0, n, rng.phi_for(12.0))
    level = np.clip(0.93 + 0.03 * noise, 0.0, 1.0)
    return rated_kw.reshape(-1, 1) * level * availability * DT_H


def biomass_kwh(
    price: F64,
    rated_kw: F64,
    min_load: F64,
    ramp_kw: F64,
    availability: F64,
    level0_kw: F64 | None = None,
    price_band: tuple[float, float] = (100.0, 700.0),
) -> tuple[F64, F64]:
    """Follow the price (minimum load at or below the band's low end, rated power at its high end,
    PLN/MWh), never changing faster than the ramp. Returns energy per interval and the last output
    level (kW) to carry into the next chunk; an outage zeroes the output but not the boiler level.
    Depends on each interval's price only, so chunking never changes a value.
    """
    lo_p, hi_p = price_band
    frac = np.clip((price - lo_p) / (hi_p - lo_p), 0.0, 1.0)
    rated = rated_kw.reshape(-1, 1)
    lo = min_load.reshape(-1, 1)
    target = rated * (lo + (1 - lo) * frac[None, :])
    out = np.zeros_like(target)
    level = target[:, 0].copy() if level0_kw is None else level0_kw.copy()
    for i in range(target.shape[1]):  # time only
        level = level + np.clip(target[:, i] - level, -ramp_kw, ramp_kw)
        out[:, i] = level
    return out * availability * DT_H, level


def hydro_flow_factor(day_of_year: F64) -> F64:
    """Seasonal run-of-river flow (fraction of rated): spring snowmelt peak, late-summer low."""
    return (
        0.5
        + 0.4 * np.exp(-(((day_of_year - 105.0) / 30.0) ** 2))
        - 0.25 * np.exp(-(((day_of_year - 235.0) / 35.0) ** 2))
    )


def hydro_kwh(
    seed: int,
    keys: NDArray[np.uint64],
    t0: int,
    n: int,
    day_of_year: F64,
    rated_kw: F64,
    availability: F64,
) -> F64:
    noise = rng.ar1(seed, keys, "hydro", t0, n, rng.phi_for(72.0))
    flow = hydro_flow_factor(day_of_year)[None, :] * np.exp(0.15 * noise)
    return rated_kw.reshape(-1, 1) * np.clip(flow, 0.0, 1.0) * availability * DT_H
