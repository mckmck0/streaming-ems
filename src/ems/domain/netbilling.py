"""Net-billing: cohort and price basis, export valuation, RCEm entries and the synthetic proxy.

RES act (consolidated Dz.U. 2026 poz. 68): art. 4b ust. 1-2a (cohorts and the one-way switch),
ust. 2 pkt 2 (negative prices count as 0; assumes the clause of ust. 12 was accepted), ust. 6-7
(RCEm), art. 4c (deposit), art. 4 ust. 11-11a (refund cap). Details and sources: 12-domain-model.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from itertools import pairwise

from ems.domain.calendar import month_add, month_of

NET_BILLING_FROM = date(2022, 4, 1)  # first feed-in before this: net-metering, out of scope
RCE_ONLY_FROM = date(2024, 7, 1)  # first feed-in from this: RCE, no choice


class PriceBasis(StrEnum):
    RCE = "RCE"
    RCEM = "RCEm"


@dataclass(frozen=True)
class Cohort:
    """``net_billing`` of a site: first feed-in date and an optional switch to RCE (``YYYY-MM``)."""

    first_feed_in: date
    rce_from: str | None = None

    def __post_init__(self) -> None:
        if self.first_feed_in < NET_BILLING_FROM:
            raise ValueError(
                f"first_feed_in {self.first_feed_in} is net-metering (before {NET_BILLING_FROM}),"
                " which is out of scope"
            )
        if self.rce_from is None:
            return
        if self.first_feed_in >= RCE_ONLY_FROM:
            raise ValueError(
                f"rce_from is only for first feed-in before {RCE_ONLY_FROM}; this cohort is RCE"
            )
        if not _is_month(self.rce_from):
            raise ValueError(f"rce_from must be YYYY-MM, got {self.rce_from!r}")
        if self.rce_from <= month_of(self.first_feed_in):
            raise ValueError("rce_from must be after the month of first_feed_in (art. 4b ust. 2a)")


def _is_month(s: str) -> bool:
    parts = s.split("-")
    return (
        len(s) == 7
        and len(parts) == 2
        and parts[0].isdigit()
        and parts[1].isdigit()
        and 1 <= int(parts[1]) <= 12
    )


def basis_for(cohort: Cohort, month: str) -> PriceBasis:
    if cohort.first_feed_in >= RCE_ONLY_FROM:
        return PriceBasis.RCE
    if cohort.rce_from is not None and month >= cohort.rce_from:
        return PriceBasis.RCE
    return PriceBasis.RCEM


@dataclass(frozen=True)
class NetBillingRules:
    deposit_factor: float  # 1.23 (art. 4c ust. 4)
    refund_cap_rcem: float  # 0.20 of the value before 1.23 (art. 4 ust. 11a)
    refund_cap_rce: float  # 0.30
    validity_months: int  # 12 from assignment (art. 4c ust. 5)

    def refund_cap(self, basis: PriceBasis) -> float:
        return self.refund_cap_rce if basis is PriceBasis.RCE else self.refund_cap_rcem


def floored(price_pln_per_kwh: float) -> float:
    """max(P, 0) without a signed zero."""
    return price_pln_per_kwh if price_pln_per_kwh > 0.0 else 0.0


@dataclass(frozen=True)
class Publication:
    value_pln_per_kwh: float
    published: date  # civil date


@dataclass(frozen=True)
class RcemEntry:
    """Every publication of one month's RCEm, oldest first; ``source`` is ``pse`` or ``proxy``."""

    month: str
    publications: tuple[Publication, ...]
    source: str

    def __post_init__(self) -> None:
        if self.source not in ("pse", "proxy"):
            raise ValueError(f"RCEm {self.month}: unknown source {self.source!r}")
        if not self.publications:
            raise ValueError(f"RCEm {self.month}: no publication")
        dates = [p.published for p in self.publications]
        if any(b <= a for a, b in pairwise(dates)):
            raise ValueError(f"RCEm {self.month}: publications must have increasing dates")
        if month_of(dates[0]) <= self.month:
            raise ValueError(f"RCEm {self.month}: published {dates[0]}, before the month ended")

    @property
    def billing_value(self) -> float:
        """The value sellers settle on: the latest publication (corrections re-settle)."""
        return self.publications[-1].value_pln_per_kwh

    def known_on(self, day: date) -> Publication | None:
        """The newest publication made before ``day``.

        PSE gives a publication date without a time, so a value counts as known from the next
        civil day: a strategy never sees it earlier than it could have.
        """
        found = None
        for p in self.publications:
            if p.published < day:
                found = p
        return found


@dataclass(frozen=True)
class RcemTable:
    entries: tuple[RcemEntry, ...]  # sorted by month, one per month

    def __post_init__(self) -> None:
        months = [e.month for e in self.entries]
        if months != sorted(set(months)):
            raise ValueError("RCEm entries must be unique and sorted by month")
        for a, b in pairwise(months):
            if b != month_add(a, 1):
                raise ValueError(f"RCEm months must be contiguous: {a} is followed by {b}")

    def entry(self, month: str) -> RcemEntry:
        for e in self.entries:
            if e.month == month:
                return e
        raise ValueError(f"no RCEm for {month}")

    def billing(self, month: str) -> float:
        return self.entry(month).billing_value

    def signal(self, day: date) -> tuple[RcemEntry, Publication]:
        """The newest month known on ``day``, at its latest publication known then."""
        for e in reversed(self.entries):
            p = e.known_on(day)
            if p is not None:
                return e, p
        raise ValueError(f"no RCEm published before {day}")


def rcem_proxy(rce_pln_per_kwh: Sequence[float], weights: Sequence[float]) -> float:
    """Σ w·max(RCE, 0) / Σ w over one month (the synthetic stand-in for PSE's RCEm)."""
    if len(rce_pln_per_kwh) != len(weights):
        raise ValueError("RCE and weights differ in length")
    den = math.fsum(weights)
    if den == 0.0:
        raise ValueError("RCEm proxy: weights sum to zero")
    num = math.fsum(w * floored(p) for p, w in zip(rce_pln_per_kwh, weights, strict=True))
    return num / den
