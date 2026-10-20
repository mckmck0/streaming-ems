"""Rule-based dispatch of site-controlled batteries (background sites only; ADR 0014).

Charge from PV surplus, grid-charge in the cheapest night slots, discharge to cover the net load in
the evening peak and in the most expensive slots. The physics is the domain battery model
(``ems.domain.battery.step``), applied to all sites at once; the loop runs over time only.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from ems.domain.battery import step

F64 = NDArray[np.float64]
DT_H = 0.25


@dataclass(frozen=True)
class BessArrays:
    present: NDArray[np.bool_]
    capacity_kwh: F64
    p_max_kw: F64
    eta: F64
    soc_min_kwh: F64
    soc_max_kwh: F64

    def initial_soc(self) -> F64:
        return np.where(self.present, 0.5 * (self.soc_min_kwh + self.soc_max_kwh), 0.0)


def slot_flags(
    price: F64, hour_local: F64, day_index: NDArray[np.int64]
) -> tuple[NDArray[np.bool_], NDArray[np.bool_]]:
    """Cheap = the 4 cheapest slots between 00:00 and 06:00 local of each day; expensive
    = evening peak
    (17-21 local) or the day's top 10 % prices.
    """
    cheap = np.zeros(price.size, dtype=bool)
    expensive = (hour_local >= 17) & (hour_local < 21)
    for d in np.unique(day_index):
        idx = np.flatnonzero(day_index == d)
        night = idx[hour_local[idx] < 6]
        if night.size:
            cheap[night[np.argsort(price[night], kind="stable")[:4]]] = True
        expensive[idx] |= price[idx] >= np.quantile(price[idx], 0.9)
    return cheap, expensive


def dispatch(
    bess: BessArrays,
    net_load_kwh: F64,
    price: F64,
    hour_local: F64,
    day_index: NDArray[np.int64],
    soc0: F64,
) -> tuple[F64, F64, F64]:
    """Charge kWh, discharge kWh and end-of-interval SoC, each [n_loc, n_steps]."""
    n_loc, n = net_load_kwh.shape
    cheap, expensive = slot_flags(price, hour_local, day_index)
    charge = np.zeros((n_loc, n))
    discharge = np.zeros((n_loc, n))
    soc_out = np.zeros((n_loc, n))
    soc = soc0.copy()
    for i in range(n):  # time only
        net_kw = net_load_kwh[:, i] / DT_H
        cmd = np.where(net_kw < 0, -net_kw, 0.0)  # store PV surplus
        if cheap[i]:
            cmd = np.maximum(cmd, bess.p_max_kw)
        if expensive[i]:
            cmd = np.where(net_kw > 0, -net_kw, cmd)  # cover the net load
        cmd = np.where(bess.present, cmd, 0.0)
        new, p = step(soc, cmd, DT_H, bess.p_max_kw, bess.eta, bess.soc_min_kwh, bess.soc_max_kwh)
        soc = np.where(bess.present, new, 0.0)
        p = np.where(bess.present, p, 0.0)
        charge[:, i] = np.maximum(p, 0.0) * DT_H
        discharge[:, i] = np.maximum(-p, 0.0) * DT_H
        soc_out[:, i] = soc
    return charge, discharge, soc_out
