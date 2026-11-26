"""The actors' pure transitions as an in-memory network (spec 011, AC-011-36; plan, Test strategy).

Restate is replaced by a loop that keeps what the runtime keeps and nothing more:
- each object's ``state`` as bytes (``serde_for`` of its state type), decoded before and encoded
  after every transition, as ``shell`` stores it;
- each Kafka topic's records per key, delivered in partition order (one queue per topic and key);
- the messages in flight, each encoded with ``serde_for(type(message))`` by the sender and decoded
  by the receiver with the serde of the type its handler takes. Restate promises no order between
  messages of separate invocations (V5), so any message in flight may be delivered next.

An ``Order`` picks the next delivery: a seed, a weight per source and how a message is picked from
those in flight. A message to an object that was never configured fails the run (``shell.handle``
fails such an invocation, and the run could not finish).

Spec 012 (plan, Test strategy) adds two sources, each ready only while it has something to give:
- a fake wall clock (``now_s``). A send with ``delay_s`` (the household's deadline) is held with its
  due time and goes in flight only once a clock tick passes it, as Restate delivers a delayed send;
  a timed handler (``shell.handle_timed``) gets ``now_s`` as its journaled time;
- operator calls: ``set_strategies`` through the HTTP ingress, queued by ``change`` after the added
  strategies are configured, as the runner does (AC-012-17), and delivered in the order sent.
Households configured with ``mode="live"`` (``live``) run the deadline chain; a sim network has
neither source, so its deliveries are drawn exactly as before. With ``timing_log``, every commit is
also marked in the timing log, as ``shell`` does after its last write (AC-012-19, -20).

Records of keys that are not managed are not delivered to a transition: on the real service they
only update the ingress log and the counter (AC-011-28), which this network does not model.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import logging
import random
import signal
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ems.actors import battery, ems, household, messages, meter, strategy, tariff, weather
from ems.actors.battery import BatteryState
from ems.actors.ems import EmsState
from ems.actors.household import HouseholdState
from ems.actors.messages import Ack, Apply, Flows, ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.meter import MeterState
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.actors.strategy import StrategyState
from ems.actors.tariff import TariffState
from ems.actors.weather import WeatherState
from ems.assembler.join import Step
from ems.assembler.mapping import parse
from ems.assembler.offline import Offline, fold
from ems.assembler.params import RunParams
from ems.contracts.envelope import to_json_bytes
from ems.contracts.time import quarter_index
from ems.contracts.topics import route
from ems.domain.actions import POLICIES
from ems.domain.netbilling import RcemTable
from ems.ingest.faults import FaultConfig, apply_faults
from ems.ingest.generator import (
    GeneratorConfig,
    SyntheticModel,
    emissions,
    load_generator_config,
    load_sites,
)
from ems.tariffs import load_book, load_rcem, site_model

REPO = Path(__file__).resolve().parents[1]
CONFIG = REPO / "config"
LOCATIONS = 10  # the 10-household catalog: managed sites 000001 (PV), 000002 (no PV), 000004 (PV)
RUN_ID = "net"
STRATEGIES = ("NoBattery", "SelfConsumption")
LIVE = "SelfConsumption"
INGESTED_AT = datetime(2026, 11, 5, tzinfo=UTC)  # outside identity; fixed for determinism
# The test fault fixture (plan, Plan decisions): config/generator.yaml with raised rates (late 0.15,
# missing 0.03, duplicate 0.03), applied as `ems-gen run --faults --config <fixture>` applies it.
FAULT_FIXTURE = REPO / "tests" / "fixtures" / "generator.faults.yaml"
# The window of the network tests: 6 h across the end of October (local midnight is 23:00Z), so two
# price curves are in play and October closes on the last step at L = 8 (2026-11-01T02:45Z).
START = datetime(2026, 10, 31, 21, tzinfo=UTC)
HOURS = 6
# Faults are keyed by event id, and the run id enters it. Over 6 h, this run id gives at L = 8
# estimates and duplicates with every step released, and at L = 2 corrections and `too_old`
# readings (found by trying net0 … net39; the tests assert the counts, not the id).
FAULT_RUN_ID = "net27"

PRODUCTION = "ems.production.v1"
CONSUMPTION = "ems.consumption.v1"
FORECAST = "ems.weather.forecast.v1"
PRICE = "ems.price.v1"
TOPICS = (PRODUCTION, CONSUMPTION, FORECAST, PRICE)
MESSAGES = "messages"
SOURCES = (*TOPICS, MESSAGES)  # the order of ``Order.weights``
CLOCK = "clock"  # the fake wall clock, ready while a delayed message waits (``Order.clock``)
CONTROL = "control"  # operator calls, ready while one is queued (``Order.control``)
# How far past the earliest due time a clock tick goes: on time, 1 s late, half a step and three
# steps late (a deadline that fires late releases every due step at once, AC-012-07).
TICK_EXTRA_S = (0.0, 1.0, 450.0, 2700.0)
TIMED = ("Household", "on_deadline")  # through ``shell.handle_timed``: it also takes the time

# ── a generated run ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Generated:
    """The records of a generated run as Kafka holds them, and its managed sites' parameters."""

    params: Mapping[str, RunParams]  # household_key -> parameters, managed sites only
    market_key: str
    records: Mapping[tuple[str, str], tuple[bytes, ...]]  # (topic, key) -> values, partition order
    # (topic, key) -> each record's emission time on the generator's schedule, parallel to records
    emitted: Mapping[tuple[str, str], tuple[datetime, ...]] = MappingProxyType({})

    def of(self, topic: str, key: str) -> tuple[bytes, ...]:
        return self.records.get((topic, key), ())

    def offline(self, key: str) -> Offline:
        """The offline side of one managed household (``offline.fold``)."""
        return fold(
            self.params[key],
            self.of(PRODUCTION, key),
            self.of(CONSUMPTION, key),
            self.of(FORECAST, key),
            self.of(PRICE, self.market_key),
        )

    def twin(self, params: RunParams, schedule: Any) -> Offline:
        """The offline twin of one household run with ``params`` and its recorded strategy schedule
        (``offline.fold(..., schedule=...)``, AC-012-16)."""
        key = params.household_key
        return fold(
            params,
            self.of(PRODUCTION, key),
            self.of(CONSUMPTION, key),
            self.of(FORECAST, key),
            self.of(PRICE, self.market_key),
            schedule=schedule,
        )


def _generator_config(path: Path = CONFIG / "generator.yaml") -> GeneratorConfig:
    return dataclasses.replace(load_generator_config(path), synthetic_locations=LOCATIONS)


def scoped(run_id: str | None, subject: str) -> str:
    """The key of an object of the run, as the record keys are: ``[run_id:]subject``."""
    return subject if run_id is None else f"{run_id}:{subject}"


@functools.cache
def managed_params(
    start: datetime, hours: int, late_slots: int, run_id: str | None = RUN_ID
) -> Mapping[str, RunParams]:
    """``RunParams`` for every EMS-managed site of the 10-household catalog over
    ``[start, start + hours)``, built as ``runs.managed_params`` builds them (plan §6)."""
    first = quarter_index(start)
    end_step = quarter_index(start + timedelta(hours=hours))
    book = load_book(CONFIG / "tariffs")
    rcem = RcemTable(tuple(load_rcem(CONFIG / "tariffs" / "rcem.yaml")))
    params: dict[str, RunParams] = {}
    for site in load_sites(CONFIG, _generator_config()):
        if not site.ems_managed or site.bess is None:
            continue
        key = scoped(run_id, site.location_id)
        params[key] = RunParams(
            run_id=run_id,
            location_id=site.location_id,
            household_key=key,
            market_key=scoped(run_id, "PL:RCE"),
            first_step=first,
            end_step=end_step,
            late_slots=late_slots,
            strategies=STRATEGIES,
            live=LIVE,
            has_pv=site.pv is not None,
            initial_soc_kwh=site.bess.capacity_kwh / 2,
            model=site_model(site, book, (first, end_step)),
            rcem=rcem,
        )
    return MappingProxyType(params)


@functools.cache
def generate(
    start: datetime, hours: int, late_slots: int, faults: bool, run_id: str | None = RUN_ID
) -> Generated:
    """``ems-gen run`` over ``[start, start + hours)`` for the 10-household catalog, optionally with
    ``--faults`` and the test fault fixture, and ``RunParams`` for every EMS-managed site.
    """
    cfg = _generator_config(FAULT_FIXTURE if faults else CONFIG / "generator.yaml")
    end = start + timedelta(hours=hours)
    model = SyntheticModel(load_sites(CONFIG, cfg), cfg.seed, CONFIG)
    source = emissions(model, cfg, start, end, run_id)
    if faults:
        fault_cfg = FaultConfig.from_dict({**cfg.faults, "enabled": True})
        source = apply_faults(source, fault_cfg, cfg.seed)
    records: dict[tuple[str, str], list[bytes]] = {}
    emitted: dict[tuple[str, str], list[datetime]] = {}
    for emit_at, _, build in source:
        event = build(INGESTED_AT)
        records.setdefault(route(event), []).append(to_json_bytes(event))
        emitted.setdefault(route(event), []).append(emit_at)
    return Generated(
        managed_params(start, hours, late_slots, run_id),
        scoped(run_id, "PL:RCE"),
        MappingProxyType({k: tuple(v) for k, v in records.items()}),
        MappingProxyType({k: tuple(v) for k, v in emitted.items()}),
    )


def live(params: Mapping[str, RunParams], grace_s: int = 120) -> dict[str, RunParams]:
    """The same households in live mode, with grace ``grace_s`` (``RunParams`` 1.1.0)."""
    return {k: dataclasses.replace(p, mode="live", grace_s=grace_s) for k, p in params.items()}


def with_strategies(
    params: Mapping[str, RunParams], strategies: tuple[str, ...]
) -> dict[str, RunParams]:
    """The same households, each starting with ``strategies``."""
    return {k: dataclasses.replace(p, strategies=strategies) for k, p in params.items()}


# ── the network ─────────────────────────────────────────────────────────────────────────────────

STATE_TYPES: dict[str, type[Any]] = {
    "Household": HouseholdState,
    "Tariff": TariffState,
    "Weather": WeatherState,
    **{name: StrategyState for name in POLICIES},
    "Ems": EmsState,
    "Battery": BatteryState,
    "Meter": MeterState,
}

STARTS: dict[str, Callable[[RunParams], Out[Any]]] = {
    "Household": household.start,
    "Weather": weather.start,
    **{name: strategy.start for name in POLICIES},
    "Ems": ems.start,
    "Battery": battery.start,
    "Meter": meter.start,
}

type Transition = Callable[[RunParams, Any, Any], Out[Any]]


def _strategy_step(name: str) -> Transition:
    return lambda p, s, m: strategy.on_step(name, p, s, m)


# (service, handler) -> (the message type the handler takes, the transition it runs).
HANDLERS: dict[tuple[str, str], tuple[type[Any], Transition]] = {
    ("Household", "on_prices"): (PriceUpdate, household.on_prices),
    ("Household", "on_forecast"): (ForecastUpdate, household.on_forecast),
    **{(name, "on_step"): (Step, _strategy_step(name)) for name in POLICIES},
    ("Ems", "on_step"): (Step, ems.on_step),
    ("Ems", "on_ack"): (Ack, ems.on_ack),
    ("Battery", "apply"): (Apply, battery.on_apply),
    ("Meter", "on_flows"): (Flows, meter.on_flows),
}
# Tariff is keyed by market and never configured: its handlers start from TariffState().
MARKET_HANDLERS: dict[tuple[str, str], tuple[type[Any], Callable[[Any, Any], Out[Any]]]] = {
    ("Tariff", "subscribe"): (Subscribe, tariff.subscribe),
}


@dataclass(frozen=True)
class Order:
    """How the next delivery is picked: a seed, one weight per source (``SOURCES``) and which
    message in flight goes next (``any``, ``oldest`` or ``newest``); the weights of a clock tick and
    of an operator call. A weight 0 holds that source back until nothing else is ready."""

    seed: int
    weights: tuple[int, int, int, int, int]
    pick: str = "any"
    clock: int = 1
    control: int = 1


@dataclass(frozen=True)
class InFlight:
    service: str
    handler: str
    key: str
    raw: bytes  # the message as the sender encoded it
    delay_s: float | None = None  # ``send_delay``, journaled with the send


@dataclass(frozen=True)
class Commit:
    """What one transition wrote and sent, as bytes (the journal of a handler)."""

    service: str
    key: str
    state: bytes
    sends: tuple[InFlight, ...]


class Network:
    """Every object of the managed households of ``gen``, configured, with the run's records queued.

    ``params`` replaces the households' parameters (live mode, other strategies); ``now_s`` starts
    the fake clock (default: the start of the earliest first step). ``commits`` keeps every
    transition's written state and sends, in order; ``sent`` every message.
    """

    def __init__(
        self,
        gen: Generated,
        params: Mapping[str, RunParams] | None = None,
        *,
        now_s: float | None = None,
        timing_log: bool = False,
    ) -> None:
        self.gen = gen
        self.households: dict[str, RunParams] = dict(gen.params if params is None else params)
        self.params: dict[tuple[str, str], RunParams] = {}
        self.stored: dict[tuple[str, str], bytes] = {}
        self.queues: dict[tuple[str, str], deque[bytes]] = {
            k: deque(v) for k, v in sorted(gen.records.items()) if v
        }
        self.in_flight: list[InFlight] = []
        self.delayed: list[tuple[float, InFlight]] = []  # (due time, message), in send order
        self.controls: deque[tuple[str, bytes]] = deque()  # (household key, StrategyChange)
        self.commits: list[Commit] = []
        self.sent: list[Send] = []
        self.deliveries = 0
        self.ticks = 0
        self.timing_log = timing_log
        starts = [900.0 * p.first_step for p in self.households.values()]
        self.now_s = (min(starts) if starts else 0.0) if now_s is None else now_s
        for p in self.households.values():
            for service in ("Household", "Weather", *p.strategies, "Ems", "Battery", "Meter"):
                self.configure(service, p)

    # -- objects --

    def configure(self, service: str, params: RunParams) -> None:
        """``shell.configure``: store the parameters and the start state once, then send."""
        at = (service, params.household_key)
        stored = self.params.get(at)
        if stored is not None:
            if stored != params:
                raise AssertionError(f"{at}: configured again with other parameters")
            return
        self.params[at] = params
        self._commit(service, params.household_key, STARTS[service](params), "configure", None)

    def state(self, service: str, key: str) -> Any:
        raw = self.stored.get((service, key), b"")
        return serde_for(STATE_TYPES[service]).deserialize(raw)

    def _commit(
        self, service: str, key: str, out: Out[Any], handler: str, in_t: int | None
    ) -> None:
        state, sends = out
        assert isinstance(state, STATE_TYPES[service]), (service, type(state))
        raw = serde_for(STATE_TYPES[service]).serialize(state)
        self.stored[(service, key)] = raw
        encoded = tuple(
            InFlight(
                s.service,
                s.handler,
                s.key,
                serde_for(type(s.message)).serialize(s.message),
                # ``Send.delay_s`` arrives with spec 012 (T22); until then no send has a delay.
                s.delay_s,
            )
            for s in sends
        )
        self.commits.append(Commit(service, key, raw, encoded))
        self.sent.extend(sends)
        for m in encoded:
            if m.delay_s is None:
                self.in_flight.append(m)
            else:
                self.delayed.append((self.now_s + m.delay_s, m))
        if self.timing_log:
            # Imported here, like the other spec 012 modules: a sim network needs none of them.
            from ems.actors import timing

            timing.mark((service, handler), key, f"inv_{len(self.commits)}", in_t, sends)

    # -- deliveries --

    def deliver_message(self, index: int) -> None:
        m = self.in_flight.pop(index)
        self.deliveries += 1
        at = (m.service, m.handler)
        if at in MARKET_HANDLERS:
            input_type, market_fn = MARKET_HANDLERS[at]
            message = serde_for(input_type).deserialize(m.raw)
            assert message is not None
            state = self.state(m.service, m.key) or TariffState()
            self._commit(m.service, m.key, market_fn(state, message), m.handler, message.t)
            return
        if at not in HANDLERS and at != TIMED:
            raise AssertionError(f"a message to {at}, which no handler takes")
        params = self.params.get((m.service, m.key))
        if params is None:
            raise AssertionError(f"a message to {at} at {m.key}, which is not configured")
        state = self.state(m.service, m.key)
        if at == TIMED:
            deadline = serde_for(messages.Deadline).deserialize(m.raw)
            assert deadline is not None
            out = household.on_deadline(params, state, deadline, self.now_s)
            self._commit(m.service, m.key, out, m.handler, deadline.t)
            return
        input_type, fn = HANDLERS[at]
        message = serde_for(input_type).deserialize(m.raw)
        assert message is not None
        self._commit(m.service, m.key, fn(params, state, message), m.handler, message.t)

    def deliver_record(self, topic: str, key: str) -> None:
        """The next record of ``topic`` at ``key``, through its Kafka-fed handler."""
        raw = self.queues[(topic, key)].popleft()
        self.deliveries += 1
        event = parse(raw)
        if topic == PRICE:
            state = self.state("Tariff", key) or TariffState()
            self._commit("Tariff", key, tariff.on_record(state, event), "on_price", None)
            return
        service = "Weather" if topic == FORECAST else "Household"
        params = self.params.get((service, key))
        if params is None:
            return  # an unmanaged site: only the ingress log and the counter (not modelled)
        fn = weather.on_record if service == "Weather" else household.on_record
        handler = "on_forecast" if service == "Weather" else "on_measurement"
        self._commit(service, key, fn(params, self.state(service, key), event), handler, None)

    # -- the clock and the operator (spec 012) --

    def change(self, key: str, t: int, strategies: tuple[str, ...]) -> None:
        """Queue ``set_strategies(t, strategies)`` for one household, as the runner sends it
        (AC-012-17): first every added strategy is configured with ``strategy_params``."""
        from ems.assembler.schedule import Change, strategy_params

        p = self.households[key]
        names = tuple(sorted(set(strategies)))
        for name in names:
            if name in POLICIES and (name, key) not in self.params:
                self.configure(name, strategy_params(p, Change(t, names)))
        body = serde_for(messages.StrategyChange).serialize(messages.StrategyChange(t, strategies))
        self.controls.append((key, body))

    def deliver_control(self) -> None:
        """The next operator call: ``Household.set_strategies`` (``shell.handle``)."""
        key, raw = self.controls.popleft()
        self.deliveries += 1
        params = self.params.get(("Household", key))
        if params is None:
            raise AssertionError(f"set_strategies at {key}, which is not configured")
        change = serde_for(messages.StrategyChange).deserialize(raw)
        assert change is not None
        out = household.on_change(params, self.state("Household", key), change)
        self._commit("Household", key, out, "set_strategies", change.t)

    def tick(self, extra_s: float) -> None:
        """The fake clock moves on to the earliest due time plus ``extra_s``; every delayed message
        due by then goes in flight, in due order."""
        self.ticks += 1
        self.now_s = max(self.now_s, min(due for due, _ in self.delayed)) + extra_s
        due = sorted((d for d in self.delayed if d[0] <= self.now_s), key=lambda d: d[0])
        self.delayed = [d for d in self.delayed if d[0] > self.now_s]
        self.in_flight.extend(m for _, m in due)

    def pending(self) -> bool:
        return (
            bool(self.in_flight)
            or bool(self.delayed)
            or bool(self.controls)
            or any(self.queues.values())
        )

    def run(
        self,
        order: Order,
        limit: int | None = None,
        after: Callable[[Network], None] | None = None,
    ) -> None:
        """Deliver everything (or ``limit`` deliveries), in the order ``order`` picks; ``after`` is
        called after each delivery."""
        rng = random.Random(order.seed)
        weights = dict(zip(SOURCES, order.weights, strict=True))
        weights |= {CLOCK: order.clock, CONTROL: order.control}
        while limit is None or self.deliveries < limit:
            ready = [s for s in (*SOURCES, CLOCK, CONTROL) if self._ready(s)]
            if not ready:
                return
            w = [weights[s] for s in ready]
            source = rng.choices(ready, weights=w if any(w) else None)[0]
            if source == CLOCK:
                self.tick(rng.choice(TICK_EXTRA_S))
            elif source == CONTROL:
                self.deliver_control()
            elif source == MESSAGES:
                n = len(self.in_flight)
                index = {"oldest": 0, "newest": n - 1}.get(order.pick)
                self.deliver_message(rng.randrange(n) if index is None else index)
            else:
                keys = [k for (t, k), q in self.queues.items() if t == source and q]
                self.deliver_record(source, rng.choice(keys))
            if after is not None:
                after(self)

    def _ready(self, source: str) -> bool:
        if source == MESSAGES:
            return bool(self.in_flight)
        if source == CLOCK:
            return bool(self.delayed)
        if source == CONTROL:
            return bool(self.controls)
        return any(q for (t, _), q in self.queues.items() if t == source)


@contextlib.contextmanager
def timing_on(path: Path) -> Iterator[Any]:
    """The timing log on, written to ``path``, for one block (AC-012-19); afterwards the listener
    is stopped, and the handlers and the SIGTERM handler it installed in this process are removed.
    """
    from ems.actors import timing

    logger = logging.getLogger("ems.timing")
    handlers = list(logger.handlers)
    sigterm = signal.getsignal(signal.SIGTERM)
    timing.start(path)
    try:
        yield timing
    finally:
        timing.stop()
        for h in [h for h in logger.handlers if h not in handlers]:
            logger.removeHandler(h)
        signal.signal(signal.SIGTERM, sigterm)
