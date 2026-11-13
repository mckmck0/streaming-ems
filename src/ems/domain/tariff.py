"""Purchase side of the bill: marginal per-kWh prices and fixed monthly charges (gross PLN).

    energy = (1 + VAT) · (seller energy price[zone] + excise)
    dist   = (1 + VAT) · (variable network fee[zone] + quality fee + OZE fee + cogeneration fee)

VAT enters once per price and nothing is rounded (invoices round to the grosz; risk D11).
Versions and national periods are picked by the civil month of ``t``, zones on tariff time
(``calendar``). The deposit may offset only ``energy`` (RES act art. 4c ust. 2-3); ``dist`` and the
fixed charges are always paid.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date

from ems.domain.calendar import ZoneScheme, civil_month, month_index, zone_at

Rates = tuple[tuple[str, float], ...]  # (zone, PLN/kWh net), in the scheme's zone order


def rate(rates: Rates, zone: str) -> float:
    for name, value in rates:
        if name == zone:
            return value
    raise KeyError(f"no rate for zone {zone!r}")


@dataclass(frozen=True)
class OperatorVersion:
    """One DSO tariff version for one group; valid from ``valid_from`` until the next version."""

    id: str
    valid_from: str  # "YYYY-MM", civil
    variable_fee_pln_per_kwh: Rates
    fixed_fee_pln_per_month: float  # 3-phase
    subscription_fee_pln_per_month: float  # 1-month billing period


@dataclass(frozen=True)
class SellerVersion:
    id: str
    valid_from: str
    energy_pln_per_kwh: Rates


@dataclass(frozen=True)
class QualityPeriod:
    id: str
    valid_from: str
    quality_fee_pln_per_kwh: float


@dataclass(frozen=True)
class National:
    """Components set once for the country, all net except ``vat`` (a fraction)."""

    vat: float
    excise_pln_per_kwh: float
    oze_fee_pln_per_kwh: float
    cogeneration_fee_pln_per_kwh: float
    quality: tuple[QualityPeriod, ...]
    capacity_band_kwh: tuple[float, float, float]  # 500, 1200, 2800
    capacity_fee_pln_per_month: tuple[float, float, float, float]
    holidays: frozenset[date]
    valid_until: str  # first month not covered, "2027-01"


def _pick[V: (OperatorVersion, SellerVersion, QualityPeriod)](
    versions: tuple[V, ...], month: str, until: str, what: str
) -> V:
    m = month_index(month)
    if m >= month_index(until) or m < month_index(versions[0].valid_from):
        raise ValueError(f"no {what} version covers {month}")
    chosen = versions[0]
    for v in versions:
        if month_index(v.valid_from) <= m:
            chosen = v
    return chosen


@dataclass(frozen=True)
class SiteTariff:
    """Everything that prices one site's purchases: its group's operator and seller versions."""

    operator: str
    seller: str
    group: str
    scheme: ZoneScheme
    operator_versions: tuple[OperatorVersion, ...]
    seller_versions: tuple[SellerVersion, ...]
    national: National

    def operator_version(self, month: str) -> OperatorVersion:
        return _pick(self.operator_versions, month, self.national.valid_until, self.operator)

    def seller_version(self, month: str) -> SellerVersion:
        return _pick(self.seller_versions, month, self.national.valid_until, self.seller)

    def quality_period(self, month: str) -> QualityPeriod:
        return _pick(self.national.quality, month, self.national.valid_until, "quality fee")


@dataclass(frozen=True)
class BuyPrice:
    energy_pln_per_kwh: float  # gross, offsettable by the deposit
    dist_pln_per_kwh: float  # gross, always paid
    zone: str
    operator_version: str
    seller_version: str
    quality_period: str


def buy_price(tariff: SiteTariff, t: int) -> BuyPrice:
    month = civil_month(t)
    nat = tariff.national
    zone = zone_at(t, tariff.scheme, nat.holidays)
    op = tariff.operator_version(month)
    sel = tariff.seller_version(month)
    quality = tariff.quality_period(month)
    gross = 1.0 + nat.vat
    energy = gross * (rate(sel.energy_pln_per_kwh, zone) + nat.excise_pln_per_kwh)
    dist = gross * (
        rate(op.variable_fee_pln_per_kwh, zone)
        + quality.quality_fee_pln_per_kwh
        + nat.oze_fee_pln_per_kwh
        + nat.cogeneration_fee_pln_per_kwh
    )
    return BuyPrice(energy, dist, zone, op.id, sel.id, quality.id)


@dataclass(frozen=True)
class FixedInputs:
    """Per-site inputs of the fixed charges; constant per run (risk D7)."""

    annual_consumption_kwh: float  # proxy for the capacity band: the catalog's annual base load


@dataclass(frozen=True)
class FixedCharges:
    network_pln: float  # DSO fixed fee, 3-phase
    subscription_pln: float  # DSO subscription fee, 1-month period (0 from 2026-10-01)
    capacity_pln: float  # capacity fee by consumption band
    operator_version: str

    @property
    def total_pln(self) -> float:
        return math.fsum((self.network_pln, self.subscription_pln, self.capacity_pln))


def capacity_band(annual_kwh: float, national: National) -> int:
    """< 500 kWh → 0; 500-1,200 inclusive → 1; > 1,200-2,800 inclusive → 2; > 2,800 → 3."""
    low, mid, high = national.capacity_band_kwh
    if annual_kwh < low:
        return 0
    if annual_kwh <= mid:
        return 1
    if annual_kwh <= high:
        return 2
    return 3


def fixed_charges(tariff: SiteTariff, month: str, inputs: FixedInputs) -> FixedCharges:
    nat = tariff.national
    op = tariff.operator_version(month)
    gross = 1.0 + nat.vat
    band = capacity_band(inputs.annual_consumption_kwh, nat)
    return FixedCharges(
        network_pln=gross * op.fixed_fee_pln_per_month,
        subscription_pln=gross * op.subscription_fee_pln_per_month,
        capacity_pln=gross * nat.capacity_fee_pln_per_month[band],
        operator_version=op.id,
    )
