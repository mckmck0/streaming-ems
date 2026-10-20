"""Heat-pump electricity: degree-hour space heating plus hot water, over a Carnot-fraction COP."""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

F64 = NDArray[np.float64]
DT_H = 0.25


def dhw_shape(hour_local: F64) -> F64:
    """Relative hot-water draw over the day (morning and evening peaks), mean 1."""
    shape = (
        0.3
        + np.exp(-(((hour_local - 7.0) / 1.5) ** 2))
        + 1.2 * np.exp(-(((hour_local - 20.0) / 2.0) ** 2))
    )
    mean = (0.3 * 24 + np.sqrt(np.pi) * (1.5 + 1.2 * 2.0)) / 24  # analytic daily mean
    return shape / mean


def cop(t_out_c: F64, t_supply_c: F64, carnot_eta: F64) -> F64:
    t_sup_k = t_supply_c + 273.15
    lift = np.maximum(t_supply_c - t_out_c, 5.0)
    return np.clip(carnot_eta * t_sup_k / lift, 1.5, 6.0)


def heat_pump_kwh(
    temp_c: F64,
    hour_local: F64,
    heat_loss_kw_per_k: F64,
    dhw_kwh_per_year: F64,
    base_temp_c: F64,
    carnot_eta: F64,
) -> F64:
    """Electricity per interval [n_loc, n_steps] in kWh; rows without a heat pump have zero loss."""
    ua = heat_loss_kw_per_k.reshape(-1, 1)
    base = base_temp_c.reshape(-1, 1)
    eta = carnot_eta.reshape(-1, 1)
    space_kw = ua * np.clip(base - temp_c, 0.0, None)
    supply_space = 30.0 + 0.6 * np.clip(base - temp_c, 0.0, None)  # heating curve (floor heating)
    dhw_kw = (dhw_kwh_per_year / 8760.0).reshape(-1, 1) * dhw_shape(hour_local)[None, :]
    elec_kw = space_kw / cop(temp_c, supply_space, eta) + dhw_kw / cop(
        temp_c, np.full_like(temp_c, 55.0), eta
    )
    return np.where(ua > 0, elec_kw * DT_H, 0.0)
