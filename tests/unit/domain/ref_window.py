"""Test windows of the spec 009 reference, shared by the spec 011 domain tests (not a test module).

``spec009_window`` is the window of ``test_reference.py``, built the same way: site PL-LOC-000001
(Enea G12, an RCE cohort) over local days 2026-10-24 … 2026-11-02 (the 100-slot civil day, the
October close), an RCEm variant of the same site, and RCE and the PV-index weights per local day as
the generator emits them (``day_prices``), with proxy RCEm for September to November.
``test_reference.py`` keeps its own copy: AC-011-02 requires the spec 009 tests to pass unchanged.

``september_window`` crosses the civil month end of 2026-09-30/10-01, where Enea's DSO version
changes (the subscription fee drops to 0). Its ``rcem`` model switches from RCEm to RCE in October
(``Cohort(2023-05-01, "2026-10")``) with the published RCEm of ``config/tariffs/rcem.yaml``, as
``tests/property/test_reference.py`` does: September's close must use September's fixed charges
and basis, whatever step closes it.

Both are cached per process: the synthetic chunk and three months of prices take about a second.
"""

from __future__ import annotations

import dataclasses
import struct
from datetime import date, timedelta
from functools import cache
from pathlib import Path

from ems.contracts.time import local_day_bounds, quarter_index
from ems.domain.netbilling import Cohort, PriceBasis, RcemTable
from ems.domain.reference import QuarterIn, SiteModel
from ems.domain.settlement import Tranche
from ems.domain.units import pln_per_mwh_to_pln_per_kwh
from ems.ingest.catalog.build import load_overrides
from ems.ingest.generator import SyntheticModel
from ems.ingest.synthetic.prices import day_prices, national_pv_index
from ems.tariffs import load_book, load_rcem, proxy_rcem, real_rcem, site_model

CONFIG = Path(__file__).resolve().parents[3] / "config"
SEED = 4242
SITE_ID = "PL-LOC-000001"
SOC0_KWH = 5.0
FIRST, LAST = date(2026, 10, 24), date(2026, 11, 2)

# RCE: the old tranche covers October and expires with a capped refund. RCEm: it is used up and the
# newer one follows it (``test_reference.py``).
OPENING = {
    PriceBasis.RCE: (
        Tranche("2025-10", "2025-11", 615.0, 500.0, 20.0, PriceBasis.RCE),
        Tranche("2026-09", "2026-10", 49.2, 40.0, 12.0, PriceBasis.RCE),
    ),
    PriceBasis.RCEM: (
        Tranche("2025-10", "2025-11", 36.9, 10.0, 6.0, PriceBasis.RCEM),
        Tranche("2026-09", "2026-10", 492.0, 400.0, 80.0, PriceBasis.RCEM),
    ),
}
# Carried into September; the first expires at September's close (as in the property test).
SEPTEMBER_OPENING = (
    Tranche("2025-09", "2025-10", 50.0, 30.0, 9.0, PriceBasis.RCEM),
    Tranche("2026-08", "2026-09", 20.0, 20.0, 5.0, PriceBasis.RCE),
)


def local_t(day: date, end: bool = False) -> int:
    """Quarter index of the start (or end) of a Europe/Warsaw day."""
    start, stop = local_day_bounds(day)
    return quarter_index(stop if end else start)


@dataclasses.dataclass(frozen=True)
class Window:
    rce: SiteModel
    rcem: SiteModel
    proxies: RcemTable
    inputs: tuple[QuarterIn, ...]

    def model(self, basis: PriceBasis) -> SiteModel:
        return self.rce if basis is PriceBasis.RCE else self.rcem


def _daily(synth: SyntheticModel, first: date, last: date) -> tuple[list[float], list[float]]:
    """RCE (PLN/kWh) and PV-index weights per local day, as the generator emits them."""
    rce: list[float] = []
    weights: list[float] = []
    day = first
    while day <= last:
        rce += [
            pln_per_mwh_to_pln_per_kwh(float(p))
            for p in day_prices(day, SEED, synth.price_params, synth.weather_params)
        ]
        t0, t1 = local_t(day), local_t(day, end=True)
        weights += [
            float(w)
            for w in national_pv_index(t0, t1 - t0, SEED, synth.price_params, synth.weather_params)
        ]
        day += timedelta(days=1)
    return rce, weights


def _inputs(synth: SyntheticModel, first: date, last: date) -> tuple[QuarterIn, ...]:
    t0, t1 = local_t(first), local_t(last, end=True)
    chunk = synth.chunk(t0, t1 - t0, [])
    rce, _ = _daily(synth, first, last)
    return tuple(
        QuarterIn(t0 + i, chunk.generation(0, i), chunk.consumption(0, i), rce[i])
        for i in range(t1 - t0)
    )


@cache
def spec009_window() -> Window:
    """The window of ``test_reference.py``: 964 quarter-hours, October closes inside."""
    site = load_overrides(CONFIG / "sites.yaml", SEED)[SITE_ID]
    t0, t1 = local_t(FIRST), local_t(LAST, end=True)
    model = site_model(site, load_book(CONFIG / "tariffs"), (t0, t1))
    synth = SyntheticModel([site], SEED, CONFIG)
    chunk = synth.chunk(t0, t1 - t0, [])
    # Whole months for the proxies: September (signal), October and November (ledger).
    m0 = local_t(date(2026, 9, 1))
    rce, weights = _daily(synth, date(2026, 9, 1), date(2026, 11, 30))
    inputs = tuple(
        QuarterIn(t0 + i, chunk.generation(0, i), chunk.consumption(0, i), rce[t0 + i - m0])
        for i in range(t1 - t0)
    )
    rcem_model = dataclasses.replace(model, cohort=Cohort(date(2023, 5, 1)))
    return Window(model, rcem_model, proxy_rcem(m0, rce, weights), inputs)


@cache
def september_window() -> Window:
    """Local days 2026-09-30 … 2026-10-01; ``rcem`` is RCEm in September and RCE from October."""
    first, last = date(2026, 9, 30), date(2026, 10, 1)
    site = load_overrides(CONFIG / "sites.yaml", SEED)[SITE_ID]
    t0, t1 = local_t(first), local_t(last, end=True)
    model = site_model(site, load_book(CONFIG / "tariffs"), (t0, t1))
    synth = SyntheticModel([site], SEED, CONFIG)
    switch = dataclasses.replace(model, cohort=Cohort(date(2023, 5, 1), "2026-10"))
    rcem = real_rcem(load_rcem(CONFIG / "tariffs" / "rcem.yaml"), ["2026-08", "2026-09", "2026-10"])
    return Window(model, switch, rcem, _inputs(synth, first, last))


def bits(x: object) -> object:
    """A value with every float replaced by its IEEE 754 bytes: equal bits, not just ``==``
    (``0.0 == -0.0``)."""
    if isinstance(x, float):
        return struct.pack("<d", x)
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        return (
            type(x).__name__,
            tuple((f.name, bits(getattr(x, f.name))) for f in dataclasses.fields(x)),
        )
    if isinstance(x, tuple | list):
        return tuple(bits(v) for v in x)
    return x
