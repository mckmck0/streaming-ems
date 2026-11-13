"""Resolve a catalog site into a domain ``SiteModel``, and build RCEm tables for a run.

Everything here fails before any settlement (AC-009-02, P11) and names the site; nothing falls
back to another tariff.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from ems.domain.battery import BatteryParams
from ems.domain.calendar import civil_month, month_add
from ems.domain.netbilling import Cohort, Publication, RcemEntry, RcemTable, rcem_proxy
from ems.domain.reference import SiteModel
from ems.domain.tariff import FixedInputs
from ems.ingest.catalog.sites import Site, TariffGroup
from ems.tariffs.config import TariffBook

SETTLED_GROUPS = (TariffGroup.G11, TariffGroup.G12, TariffGroup.G12W)
LINE_TO_NEUTRAL_KV = 0.23  # contracted power above 0.23 kV x fuse needs a 3-phase connection


class SiteConfigError(ValueError):
    """A site cannot be settled; the message starts with its location id."""


def site_model(site: Site, book: TariffBook, window: tuple[int, int]) -> SiteModel:
    """The site's tariff, cohort, fixed-charge inputs and battery, checked for [t0, t1)."""

    def fail(msg: str) -> SiteConfigError:
        return SiteConfigError(f"{site.location_id}: {msg}")

    bess = site.bess
    if bess is None or not site.ems_managed:
        raise fail("not EMS-managed (no EMS-controlled battery); nothing to settle")
    if site.tariff_group not in SETTLED_GROUPS:
        raise fail(f"tariff group {site.tariff_group} is out of scope (G11, G12, G12w only)")
    if site.contracted_kw <= LINE_TO_NEUTRAL_KV * site.fuse_a:
        raise fail(
            f"a 1-phase connection ({site.contracted_kw} kW on {site.fuse_a} A) is out of scope:"
            " only the 3-phase fixed fee is sourced"
        )
    try:
        tariff = book.site_tariff(site.dso, site.tariff_group.value)
    except KeyError as exc:
        raise fail(str(exc.args[0])) from exc
    if site.net_billing is None:
        raise fail("no net_billing cohort")
    try:
        cohort = Cohort(site.net_billing.first_feed_in, site.net_billing.rce_from)
    except ValueError as exc:
        raise fail(f"net_billing: {exc}") from exc
    t0, t1 = window
    for t in (t0, t1 - 1):
        month = civil_month(t)
        try:
            tariff.operator_version(month)
            tariff.seller_version(month)
            tariff.quality_period(month)
        except ValueError as exc:
            raise fail(str(exc)) from exc
    return SiteModel(
        location_id=site.location_id,
        tariff=tariff,
        cohort=cohort,
        fixed_inputs=FixedInputs(site.annual_base_load_kwh),
        battery=BatteryParams(
            capacity_kwh=bess.capacity_kwh,
            p_max_kw=bess.p_max_kw,
            eta=bess.eta,
            soc_min_kwh=bess.soc_min_kwh,
            soc_max_kwh=bess.soc_max_kwh,
        ),
        rules=book.rules,
    )


def real_rcem(entries: Sequence[RcemEntry], months: Sequence[str]) -> RcemTable:
    """PSE values only, for a real-data run: every loaded month, so the signal always finds the
    newest publication, after checking that the run's billing ``months`` are there (AC-009-08).
    """
    have = {e.month for e in entries}
    missing = [m for m in months if m not in have]
    if missing:
        raise ValueError(f"no published RCEm for {', '.join(missing)}")
    return RcemTable(tuple(entries))


def proxy_rcem(t0: int, rce_pln_per_kwh: Sequence[float], weights: Sequence[float]) -> RcemTable:
    """The synthetic run's RCEm: one proxy per civil month, weighted by the national PV index and
    "published" on the 11th of the next month. The series must cover whole months.
    """
    n = len(rce_pln_per_kwh)
    if n == 0 or len(weights) != n:
        raise ValueError("RCE and weights must be non-empty and of equal length")
    if civil_month(t0 - 1) == civil_month(t0) or civil_month(t0 + n - 1) == civil_month(t0 + n):
        raise ValueError("the RCEm proxy needs whole months of RCE and weights")
    entries: list[RcemEntry] = []
    start = 0
    for i in range(1, n + 1):
        if i == n or civil_month(t0 + i) != civil_month(t0 + start):
            month = civil_month(t0 + start)
            nxt = month_add(month, 1)
            published = date(int(nxt[:4]), int(nxt[5:]), 11)
            value = rcem_proxy(rce_pln_per_kwh[start:i], weights[start:i])
            entries.append(RcemEntry(month, (Publication(value, published),), "proxy"))
            start = i
    return RcemTable(tuple(entries))
