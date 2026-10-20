"""Battery physics (brief §4): one-way efficiency on charge and on discharge, power and SoC limits.

Charge (p >= 0, AC side): ``soc += p * eta * dt``. Discharge (p < 0): ``soc += p / eta * dt``. The
command is clamped by the power limit and by the SoC bounds, so infeasible commands are never
executed (principle 8). Works element-wise on floats and numpy arrays alike; identical inputs give
bit-identical outputs.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

F64 = NDArray[np.float64]
ArrayLike = float | F64


@dataclass(frozen=True)
class BatteryParams:
    capacity_kwh: float
    p_max_kw: float
    eta: float
    soc_min_kwh: float
    soc_max_kwh: float

    def __post_init__(self) -> None:
        if not 0 < self.eta <= 1:
            raise ValueError("eta must be in (0, 1]")
        if not 0 <= self.soc_min_kwh < self.soc_max_kwh <= self.capacity_kwh:
            raise ValueError("need 0 <= soc_min < soc_max <= capacity")
        if self.p_max_kw <= 0:
            raise ValueError("p_max must be positive")


def step(
    soc_kwh: ArrayLike,
    p_cmd_kw: ArrayLike,
    dt_h: float,
    p_max_kw: ArrayLike,
    eta: ArrayLike,
    soc_min_kwh: ArrayLike,
    soc_max_kwh: ArrayLike,
) -> tuple[F64, F64]:
    """Apply a power command for ``dt_h``; returns (new SoC, actual AC power). p > 0 charges."""
    soc = np.asarray(soc_kwh, dtype=np.float64)
    cmd = np.asarray(p_cmd_kw, dtype=np.float64)
    p_max = np.asarray(p_max_kw, dtype=np.float64)
    e = np.asarray(eta, dtype=np.float64)
    lo = np.asarray(soc_min_kwh, dtype=np.float64)
    hi = np.asarray(soc_max_kwh, dtype=np.float64)
    charge = np.minimum(np.minimum(cmd, p_max), np.maximum(hi - soc, 0.0) / (e * dt_h))
    discharge = -np.minimum(np.minimum(-cmd, p_max), np.maximum(soc - lo, 0.0) * e / dt_h)
    p = np.where(cmd >= 0, np.maximum(charge, 0.0), np.minimum(discharge, 0.0))
    new_soc = np.where(p >= 0, soc + p * e * dt_h, soc + p / e * dt_h)
    return np.clip(new_soc, lo, hi), p  # clip guards rounding at the bounds


def step_params(
    soc_kwh: float, p_cmd_kw: float, dt_h: float, b: BatteryParams
) -> tuple[float, float]:
    soc, p = step(soc_kwh, p_cmd_kw, dt_h, b.p_max_kw, b.eta, b.soc_min_kwh, b.soc_max_kwh)
    return float(soc), float(p)
