"""PV output with the pvlib chain, vectorised over sites: POA, cell temperature, PVWatts DC and
AC.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray
from pvlib import inverter, irradiance, pvsystem, temperature

from ems.ingest.synthetic.weather import Truth

F64 = NDArray[np.float64]
DT_H = 0.25


@dataclass(frozen=True)
class PvArrays:
    """Per-site PV parameters (1-D); sites without PV have kwp 0."""

    kwp: F64
    tilt_deg: F64
    azimuth_deg: F64
    ac_kw: F64
    gamma_pdc: F64
    losses: F64


def seasonal_derate(month: NDArray[np.int64]) -> F64:
    """Soiling and snow as calibrated monthly derates (pvlib's models need precipitation input)."""
    snow = np.where(np.isin(month, [12, 1, 2]), 0.08, np.where(month == 3, 0.03, 0.0))
    soiling = 0.02
    return 1.0 - snow - soiling


def pv_energy_kwh(truth: Truth, pv: PvArrays) -> F64:
    """AC energy per interval [n_loc, n_steps] in kWh; zero at night and for sites without PV."""
    g = truth.geometry
    tilt = pv.tilt_deg.reshape(-1, 1)
    az = pv.azimuth_deg.reshape(-1, 1)
    poa = irradiance.get_total_irradiance(
        tilt,
        az,
        g.apparent_zenith,
        g.azimuth,
        truth.dni,
        truth.ghi,
        truth.dhi,
        dni_extra=g.dni_extra.reshape(1, -1),
        model="haydavies",
    )
    poa_global = np.clip(np.nan_to_num(np.asarray(poa["poa_global"], dtype=np.float64)), 0.0, None)
    month = truth.axis.calendar(truth.axis.unix_mid)[1]
    effective = poa_global * seasonal_derate(month)[None, :] * (1 - pv.losses.reshape(-1, 1))
    t_cell = temperature.faiman(poa_global, truth.temp_c, truth.wind10)
    pdc0 = (pv.kwp * 1000.0).reshape(-1, 1)
    pdc = pvsystem.pvwatts_dc(
        effective_irradiance=effective,
        temp_cell=t_cell,
        pdc0=pdc0,
        gamma_pdc=pv.gamma_pdc.reshape(-1, 1),
    )
    eta = 0.96
    inv_pdc0 = np.maximum(pv.ac_kw * 1000.0 / eta, 1e-9).reshape(-1, 1)  # clips at the AC rating
    pac = inverter.pvwatts(np.clip(pdc, 0.0, None), inv_pdc0, eta_inv_nom=eta)
    pac = np.where(
        (pdc0 > 0) & (g.cs_ghi > 0), np.clip(np.asarray(pac, dtype=np.float64), 0.0, None), 0.0
    )
    return pac / 1000.0 * DT_H
