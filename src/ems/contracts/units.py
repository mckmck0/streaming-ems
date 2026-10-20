"""Canonical units of the wire contract and conversions at the boundary."""

from __future__ import annotations

ENERGY_UNIT = "kWh"
POWER_UNIT = "kW"

_ENERGY_TO_KWH = {"Wh": 1e-3, "0.1Wh": 1e-4, "kWh": 1.0, "MWh": 1e3}
_POWER_TO_KW = {"W": 1e-3, "kW": 1.0, "MW": 1e3}


def to_kwh(value: float, unit: str) -> float:
    return value * _ENERGY_TO_KWH[unit]


def to_kw(value: float, unit: str) -> float:
    return value * _POWER_TO_KW[unit]


def kw_to_kwh(power_kw: float, dt_h: float) -> float:
    """Power becomes energy only through the interval length."""
    return power_kw * dt_h


def price_per_mwh_to_per_kwh(price: float) -> float:
    """Wire prices are per MWh in native currency; the domain uses per kWh."""
    return price / 1000.0
