"""Wind power from vendored power curves (``config/turbines/power_curves.csv``, ODbL; no
windpowerlib).

Hub-height wind follows the site's power-law shear; air density from the hub pressure and
temperature corrects the wind speed (IEC: v_eq = v (rho / 1.225)^(1/3)) before a linear
interpolation of the curve, which is zero below cut-in and above cut-out.
"""

from __future__ import annotations

import csv
from functools import cache
from pathlib import Path

import numpy as np
from numpy.typing import NDArray
from pvlib.atmosphere import alt2pres

F64 = NDArray[np.float64]
DT_H = 0.25
R_DRY = 287.058
RHO_REF = 1.225


@cache
def power_curves(path: Path) -> dict[str, tuple[F64, F64, float]]:
    """Turbine type -> (wind speeds m/s, power kW, rated kW)."""
    points: dict[str, list[tuple[float, float]]] = {}
    rated: dict[str, float] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            points.setdefault(row["turbine_type"], []).append(
                (float(row["wind_speed_ms"]), float(row["power_kw"]))
            )
            rated[row["turbine_type"]] = float(row["nominal_power_kw"])
    out = {}
    for t, pts in points.items():
        pts.sort()
        out[t] = (np.array([v for v, _ in pts]), np.array([p for _, p in pts]), rated[t])
    return out


def air_density(temp_c: F64, altitude_m: F64) -> F64:
    pressure = alt2pres(altitude_m)
    return np.asarray(pressure / (R_DRY * (temp_c + 273.15)), dtype=np.float64)


def turbine_power_kw(curve: tuple[F64, F64, float], wind_ms: F64, density: F64) -> F64:
    speeds, power, rated = curve
    v_eq = wind_ms * (density / RHO_REF) ** (1.0 / 3.0)
    p = np.interp(v_eq, speeds, power, left=0.0, right=0.0)
    return np.clip(p, 0.0, rated)


def wind_energy_kwh(
    curves_path: Path,
    turbine_types: list[str | None],
    counts: F64,
    hub_height_m: F64,
    wind10_ms: F64,
    shear_alpha: F64,
    temp2m_c: F64,
    site_elevation_m: F64,
    availability: F64,
) -> F64:
    """Farm energy per interval [n_loc, n_steps]; rows without wind assets are zero."""
    curves = power_curves(curves_path)
    out = np.zeros_like(wind10_ms)
    hub = hub_height_m.reshape(-1, 1)
    v_hub = wind10_ms * (hub / 10.0) ** shear_alpha.reshape(-1, 1)
    temp_hub = temp2m_c - 0.0065 * (hub - 2.0)
    density = air_density(temp_hub, site_elevation_m.reshape(-1, 1) + hub)
    for t in sorted({x for x in turbine_types if x}):
        rows = np.array([i for i, x in enumerate(turbine_types) if x == t])
        p = turbine_power_kw(curves[t], v_hub[rows], density[rows])
        out[rows] = p * counts[rows].reshape(-1, 1) * DT_H
    return out * availability
