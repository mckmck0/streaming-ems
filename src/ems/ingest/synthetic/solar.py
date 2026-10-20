"""Vectorised solar geometry and clear-sky irradiance for many sites at once.

pvlib's high-level helpers work per location; ``pvlib.spa.solar_position_numpy`` is element-wise, so
it broadcasts a 1-D time axis against ``(n_loc, 1)`` site columns. Linke turbidity and altitude come
from the data files pvlib ships, read once for the whole batch.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import h5py
import numpy as np
import pvlib
from numpy.typing import NDArray
from pvlib import atmosphere, clearsky, irradiance, spa

F64 = NDArray[np.float64]


@dataclass(frozen=True)
class SolarGeometry:
    apparent_zenith: F64  # degrees, [n_loc, n_steps]
    zenith: F64
    azimuth: F64
    dni_extra: F64  # [n_steps]
    airmass_absolute: F64
    cs_ghi: F64
    cs_dni: F64
    cs_dhi: F64


@lru_cache(maxsize=1)
def _linke_table() -> NDArray[np.uint8]:
    path = Path(pvlib.__file__).parent / "data" / "LinkeTurbidities.h5"
    with h5py.File(path, "r") as f:
        return np.asarray(f["LinkeTurbidity"][:])  # 2160 x 4320 x 12, value = 20 x TL


def linke_turbidity(lat: F64, lon: F64, month: NDArray[np.int64]) -> F64:
    """Monthly Linke turbidity per site and step (same grid lookup as pvlib), [n_loc, n_steps]."""
    table = _linke_table()
    rows = np.clip(
        np.round((90.0 - lat) * table.shape[0] / 180.0).astype(int), 0, table.shape[0] - 1
    )
    cols = np.clip(
        np.round((lon + 180.0) * table.shape[1] / 360.0).astype(int), 0, table.shape[1] - 1
    )
    monthly = table[rows, cols, :].astype(np.float64) / 20.0  # [n_loc, 12]
    return monthly[:, month - 1]


def solar_geometry(
    unixtime_mid: F64,
    day_of_year: NDArray[np.int64],
    month: NDArray[np.int64],
    lat: F64,
    lon: F64,
    elevation_m: F64,
) -> SolarGeometry:
    """Geometry at interval midpoints for sites ``lat``/``lon``/``elevation_m`` (1-D, n_loc)."""
    lat_c = lat.reshape(-1, 1)
    lon_c = lon.reshape(-1, 1)
    elev_c = elevation_m.reshape(-1, 1)
    pressure_pa = atmosphere.alt2pres(elev_c)
    theta, theta0, _e, _e0, phi, _eot = spa.solar_position_numpy(
        unixtime_mid, lat_c, lon_c, elev_c, pressure_pa / 100.0, 12.0, 67.0, 0.5667, 1
    )
    apparent = np.asarray(theta, dtype=np.float64)
    zenith = np.asarray(theta0, dtype=np.float64)
    azimuth = np.asarray(phi, dtype=np.float64)
    dni_extra = np.asarray(irradiance.get_extra_radiation(day_of_year), dtype=np.float64)
    am_rel = atmosphere.get_relative_airmass(apparent)
    am_abs = np.asarray(atmosphere.get_absolute_airmass(am_rel, pressure_pa), dtype=np.float64)
    tl = linke_turbidity(lat, lon, month)
    with np.errstate(divide="ignore", invalid="ignore"):  # night steps; masked below
        cs = clearsky.ineichen(
            apparent, am_abs, tl, altitude=elev_c, dni_extra=dni_extra.reshape(1, -1)
        )
    night = apparent >= 90.0
    cs_ghi = np.where(night, 0.0, np.nan_to_num(np.asarray(cs["ghi"], dtype=np.float64)))
    cs_dni = np.where(night, 0.0, np.nan_to_num(np.asarray(cs["dni"], dtype=np.float64)))
    cs_dhi = np.where(night, 0.0, np.nan_to_num(np.asarray(cs["dhi"], dtype=np.float64)))
    return SolarGeometry(
        apparent_zenith=apparent,
        zenith=zenith,
        azimuth=azimuth,
        dni_extra=dni_extra,
        airmass_absolute=np.nan_to_num(am_abs, nan=0.0),
        cs_ghi=cs_ghi,
        cs_dni=cs_dni,
        cs_dhi=cs_dhi,
    )


def decompose(ghi: F64, zenith: F64, day_of_year: NDArray[np.int64]) -> tuple[F64, F64]:
    """Erbs split of GHI into DNI and DHI, closed so that GHI = DHI + DNI cos(z)."""
    out = irradiance.erbs(ghi, zenith, day_of_year.reshape(1, -1))
    dhi = np.clip(np.nan_to_num(np.asarray(out["dhi"], dtype=np.float64)), 0.0, None)
    dhi = np.minimum(dhi, ghi)
    cosz = np.cos(np.radians(zenith))
    dni = np.where(cosz > 0.065, (ghi - dhi) / np.maximum(cosz, 0.065), 0.0)
    dni = np.clip(dni, 0.0, None)
    dhi = np.where(cosz > 0.065, dhi, ghi)  # sun near the horizon: all diffuse
    return dni, dhi
