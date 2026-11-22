"""Shared builders for the assembler tests (spec 011): run parameters for the managed sites of the
10-household catalog, price curves of whole local days, a price rule written from the criteria, and
the clean generated day as the Kafka sink would publish it. Not a test module; the unit and property
tests import it.
"""

from __future__ import annotations

import dataclasses
import functools
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import Curve
from ems.contracts.envelope import MeasurementEvent, PriceEvent, WeatherForecastEvent, to_json_bytes
from ems.contracts.time import (
    WARSAW,
    from_quarter_index,
    local_date,
    local_day_bounds,
    quarter_index,
)
from ems.domain.netbilling import RcemTable
from ems.domain.reference import SiteModel
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import SyntheticModel, emissions, load_generator_config, load_sites
from ems.tariffs import load_book, load_rcem, site_model

type Event = MeasurementEvent | WeatherForecastEvent | PriceEvent

REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config"
LOCATIONS = 10
RUN_ID = "r1"
# The clean day of AC-011-23 and the prototype in plan.md (Facts): 2026-10-20T00:00Z, one day.
DAY_START = datetime(2026, 10, 20, tzinfo=UTC)
INGESTED_AT = datetime(2026, 10, 22, tzinfo=UTC)  # outside identity; fixed for determinism
PV_SITE, NO_PV_SITE, PV_SITE_2 = "PL-LOC-000001", "PL-LOC-000002", "PL-LOC-000004"
STRATEGIES = ("NoBattery", "SelfConsumption")


def step_at(text: str) -> int:
    """The quarter-hour index of a UTC instant written as ``YYYY-MM-DDTHH:MM[:SS]``."""
    return quarter_index(datetime.fromisoformat(text).replace(tzinfo=UTC))


def seconds_at(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


def day_slots(day: date) -> tuple[int, int]:
    """``(t0, n)``: the first quarter-hour of the local delivery day and its number of slots."""
    start, end = local_day_bounds(day)
    t0 = quarter_index(start)
    return t0, quarter_index(end) - t0


# ── run parameters ────────────────────────────────────────────────────────────────────────────────


@functools.cache
def _catalog() -> dict[str, Site]:
    cfg = dataclasses.replace(
        load_generator_config(CONFIG / "generator.yaml"), synthetic_locations=LOCATIONS
    )
    return {s.location_id: s for s in load_sites(CONFIG, cfg)}


@functools.cache
def rcem_table() -> RcemTable:
    return RcemTable(load_rcem(CONFIG / "tariffs" / "rcem.yaml"))


@functools.cache
def model_for(location_id: str, first_step: int, end_step: int) -> SiteModel:
    return site_model(
        _catalog()[location_id], load_book(CONFIG / "tariffs"), (first_step, end_step)
    )


def has_pv(location_id: str) -> bool:
    return _catalog()[location_id].pv is not None


@functools.cache
def run_params(
    location_id: str = PV_SITE,
    first_step: int = quarter_index(DAY_START),
    end_step: int = quarter_index(DAY_START) + 96,
    late_slots: int = 8,
    run_id: str | None = RUN_ID,
    strategies: tuple[str, ...] = STRATEGIES,
    live: str = "SelfConsumption",
) -> RunParams:
    """A real ``RunParams`` for a managed site, as ``runs.managed_params`` is to build it."""
    model = model_for(location_id, first_step, end_step)
    return RunParams(
        run_id=run_id,
        location_id=location_id,
        household_key=f"{run_id}:{location_id}" if run_id else location_id,
        market_key=f"{run_id}:PL:RCE" if run_id else "PL:RCE",
        first_step=first_step,
        end_step=end_step,
        late_slots=late_slots,
        strategies=strategies,
        live=live,
        has_pv=has_pv(location_id),
        initial_soc_kwh=model.battery.capacity_kwh / 2,
        model=model,
        rcem=rcem_table(),
    )


# ── curves ────────────────────────────────────────────────────────────────────────────────────────


def local_clock(t: int) -> tuple[int, int]:
    """Europe/Warsaw clock (hour, minute) at the start of quarter-hour ``t``."""
    local = from_quarter_index(t).astimezone(WARSAW)
    return local.hour, local.minute


def local_day(t: int) -> date:
    return local_date(from_quarter_index(t))


def price_of(curves: Iterable[Curve], q: int) -> tuple[float, bool]:
    """``(price, replaced)`` of step ``q`` by AC-011-18 and -19, written from the criteria: among
    the curves visible at ``q``, the own day's newest publication (ties: first accepted); else the
    same local clock quarter-hour of the nearest earlier day, at most two days back, whose curve has
    it (first copy on a 100-slot day); else 0."""
    visible = [c for c in curves if c.visible_step <= q]

    def best(cs: list[Curve]) -> Curve:
        return min(cs, key=lambda c: (-c.issued_s, c.seq))

    own = [c for c in visible if c.t0 <= q < c.t0 + len(c.rce_pln_per_kwh)]
    if own:
        c = best(own)
        return c.rce_pln_per_kwh[q - c.t0], False
    clock = local_clock(q)
    for back in (1, 2):
        earlier = [c for c in visible if c.day == local_day(q) - timedelta(days=back)]
        if not earlier:
            continue
        c = best(earlier)
        for i, price in enumerate(c.rce_pln_per_kwh):
            if local_clock(c.t0 + i) == clock:
                return price, True
    return 0.0, True


def day_curve(
    seq: int,
    day: date,
    visible_step: int,
    price: Callable[[int], float] | None = None,
    issued_s: int = 0,
) -> Curve:
    """An accepted curve of local delivery day ``day``; slot ``i`` costs ``price(i)`` PLN/kWh
    (default ``seq * 1000 + i``, so every value names its curve and slot)."""
    t0, n = day_slots(day)
    fn = price if price is not None else (lambda i: float(seq * 1000 + i))
    return Curve(seq, t0, day, tuple(fn(i) for i in range(n)), issued_s, visible_step)


# ── the clean generated day ───────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Generated:
    """Events in emission order with their generator stream; ``raw`` gives a key's records as the
    Kafka sink publishes them (``to_json_bytes``), in partition order."""

    events: tuple[tuple[str, Event], ...]

    def of(self, stream: str, location_id: str | None = None) -> tuple[Event, ...]:
        return tuple(
            e
            for s, e in self.events
            if s == stream and (location_id is None or e.location_id == location_id)
        )

    def raw(self, stream: str, location_id: str | None = None) -> tuple[bytes, ...]:
        return tuple(to_json_bytes(e) for e in self.of(stream, location_id))


@functools.cache
def generated(start: datetime = DAY_START, days: int = 1, run_id: str | None = RUN_ID) -> Generated:
    cfg = dataclasses.replace(
        load_generator_config(CONFIG / "generator.yaml"), synthetic_locations=LOCATIONS
    )
    model = SyntheticModel(load_sites(CONFIG, cfg), cfg.seed, CONFIG)
    end = start + timedelta(days=days)
    return Generated(
        tuple(
            (stream, build(INGESTED_AT))
            for _, stream, build in emissions(model, cfg, start, end, run_id)
        )
    )
