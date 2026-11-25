"""AC-011-36 (with AC-011-30, -34, -35): every transition as an in-memory network, any order.

The three managed households of the 10-household catalog over 6 h across the end of October
(``tests/network.py``), with every object configured, the run's Kafka records queued per topic and
key, and every message encoded, held in flight and delivered in an order hypothesis picks (a seed,
a weight per source, and oldest, newest or any message next). The data are generated once per
module; hypothesis draws only the delivery order.

WHILE SelfConsumption is live, whenever ``Battery`` (or ``Meter``) and the SelfConsumption object
have applied the same steps, the battery's SoC equals the virtual SoC (and the meter's accumulator,
kept flows and closed months equal the virtual ledger). At the end, for every managed household:
the household's join, and every strategy's state, equal the offline fold's (``offline.fold``);
``Ems`` has commanded every released step; nothing is held anywhere.

Three runs: without faults at L = 8, and with the rates of the test fault fixture at L = 2
(corrections, ``too_old`` readings) and at L = 8 (estimates, duplicates, no correction). Each
fixture first asserts that its data show what it is for.

Spec 012 (T21; AC-012-11, -13 … -16, -20), with the network's operator calls and fake clock:
- sim with strategy changes: every household starts with SelfConsumption only; NoBattery is added
  at +8 (23:00Z, when November starts in Warsaw) and removed at +16 (01:00Z), each call delivered
  at a point hypothesis picks (accepted, or refused when its hour was already released). At the
  end the network equals the offline twin given each household's recorded schedule;
- live with the fake clock (no twin, risk A4): steps released in order, every step of the window
  and none at or past ``end_step``; each strategy equals ``step_strategy`` folded over the steps
  its household sent it; ``Battery`` and ``Meter`` equal the live strategy's shadow; at most one
  ``Deadline`` per household in flight or waiting, after every delivery;
- the timing log on and off: identical commits, and one line per commit.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
from types import ModuleType
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from ems.actors import strategy
from ems.actors.reorder import Inbox
from ems.assembler.offline import Offline, start_strategy, step_strategy
from ems.domain.actions import POLICIES
from tests.network import (
    FAULT_RUN_ID,
    HOURS,
    RUN_ID,
    START,
    Generated,
    Network,
    Order,
    generate,
    live,
    timing_on,
    with_strategies,
)

weights = st.sampled_from((1, 4, 32, 256))
orders = st.builds(
    Order,
    seed=st.integers(min_value=0, max_value=2**32 - 1),
    weights=st.tuples(weights, weights, weights, weights, weights),
    pick=st.sampled_from(("any", "oldest", "newest")),
)
# (production, consumption, forecast, price, messages)
FIFO = Order(0, (1, 1, 1, 1, 1), "oldest")
PRICES_LAST = Order(1, (256, 256, 256, 1, 256), "any")
PRICES_FIRST_MESSAGES_LIFO = Order(2, (1, 1, 1, 256, 1), "newest")
PRODUCTION_AHEAD = Order(3, (256, 1, 1, 1, 1), "newest")
MESSAGES_LAST = Order(4, (32, 32, 32, 32, 1), "newest")


def _counts(gen: Generated) -> dict[str, int]:
    out = dict.fromkeys(("steps", "corrections", "estimated", "duplicate", "too_old", "months"), 0)
    for key in gen.params:
        off = gen.offline(key)
        out["steps"] += len(off.steps)
        out["corrections"] += sum(len(s.corrections) for s in off.steps)
        out["estimated"] += sum(s.pv_estimated + s.load_estimated for s in off.steps)
        sides = [off.join.consumption, off.join.production]
        out["duplicate"] += sum(s.stream.duplicate for s in sides if s is not None)
        out["too_old"] += sum(s.stream.too_old for s in sides if s is not None)
        out["months"] += len(off.strategies["SelfConsumption"].months)
    return out


@pytest.fixture(scope="module")
def clean() -> Generated:
    gen = generate(START, HOURS, 8, False, RUN_ID)
    counts = _counts(gen)
    assert len(gen.params) == 3
    assert counts["steps"] == 3 * 4 * HOURS  # every step released
    assert counts["months"] == 3  # October closes in every household (on the last step)
    assert counts["corrections"] == counts["estimated"] == counts["duplicate"] == 0
    return gen


@pytest.fixture(scope="module")
def faults_l2() -> Generated:
    gen = generate(START, HOURS, 2, True, FAULT_RUN_ID)
    counts = _counts(gen)
    assert counts["corrections"] > 0 and counts["too_old"] > 0, counts
    return gen


@pytest.fixture(scope="module")
def faults_l8() -> Generated:
    gen = generate(START, HOURS, 8, True, FAULT_RUN_ID)
    counts = _counts(gen)
    assert counts["estimated"] > 0 and counts["duplicate"] > 0, counts
    assert counts["corrections"] == 0, counts
    return gen


def _parity_while_running(net: Network) -> None:
    """After a delivery to an object of a household: wherever the real path and the live
    strategy have applied the same steps, they hold the same SoC and ledger."""
    last = net.commits[-1]
    if last.service in ("Household", "Weather", "Tariff", "Ems"):
        return
    p = net.params.get((last.service, last.key))
    if p is None:
        return
    live = net.state(p.live, last.key)
    applied = live.steps.expected
    b = net.state("Battery", last.key)
    if b.commands.expected == applied:
        assert b.soc_kwh == live.result.state.soc_kwh, (last.key, applied)
    m = net.state("Meter", last.key)
    if m.flows.expected == applied:
        assert (m.acc, m.recent, m.months) == (
            live.result.state.acc,
            live.result.recent,
            live.result.months,
        ), (last.key, applied)


def _held(state: Any) -> list[str]:
    return [
        f.name
        for f in dataclasses.fields(state)
        if isinstance(getattr(state, f.name), Inbox) and getattr(state, f.name).held
    ]


def _end_checks(gen: Generated, net: Network) -> None:
    assert not net.pending()
    for key, p in sorted(gen.params.items()):
        off: Offline = gen.offline(key)
        assert off.steps, key
        h = net.state("Household", key)
        assert h.join == off.join, key
        for name in p.strategies:
            s = net.state(name, key)
            assert s.result == off.strategies[name], (key, name)
            assert s.steps == Inbox(expected=off.join.next), (key, name)
        live = off.strategies[p.live]
        b = net.state("Battery", key)
        assert b.soc_kwh == live.state.soc_kwh, key
        m = net.state("Meter", key)
        assert (m.acc, m.recent, m.months) == (live.state.acc, live.recent, live.months), key
        e = net.state("Ems", key)
        assert (e.sent, e.waiting) == (off.join.next - 1, ()), key
        assert e.soc_kwh == b.soc_kwh, key
        for service in ("Household", *p.strategies, "Ems", "Battery", "Meter"):
            assert _held(net.state(service, key)) == [], (service, key)


def _run(gen: Generated, order: Order) -> Network:
    net = Network(gen)
    net.run(order, after=_parity_while_running)
    _end_checks(gen, net)
    return net


def test_ac_011_30_the_example_orders_reorder_every_receiver_that_can_be(clean: Generated) -> None:
    """Not vacuous: over the explicit orders, every inbox that can see messages out of order does
    (``Battery.commands`` and ``Ems.acks`` cannot: one command is in flight at a time)."""
    reordered: dict[str, int] = {}
    for order in (FIFO, PRICES_LAST, PRICES_FIRST_MESSAGES_LIFO, PRODUCTION_AHEAD, MESSAGES_LAST):
        net = _run(clean, order)
        for service, key in net.stored:
            state = net.state(service, key)
            for f in dataclasses.fields(state):
                inbox = getattr(state, f.name)
                if isinstance(inbox, Inbox):
                    name = f"{service}.{f.name}"
                    reordered[name] = reordered.get(name, 0) + inbox.reordered
    assert {name for name, n in reordered.items() if n} == {
        "Household.prices",
        "Household.forecasts",
        "NoBattery.steps",
        "SelfConsumption.steps",
        "Ems.steps",
        "Meter.flows",
    }


@settings(max_examples=50)
@given(order=orders)
@example(order=FIFO)
@example(order=PRICES_LAST)
@example(order=PRICES_FIRST_MESSAGES_LIFO)
@example(order=PRODUCTION_AHEAD)
@example(order=MESSAGES_LAST)
def test_ac_011_36_any_delivery_order_ends_equal_to_offline_without_faults(
    clean: Generated, order: Order
) -> None:
    _run(clean, order)


@settings(max_examples=50)
@given(order=orders)
@example(order=FIFO)
@example(order=PRICES_LAST)
@example(order=PRICES_FIRST_MESSAGES_LIFO)
@example(order=PRODUCTION_AHEAD)
@example(order=MESSAGES_LAST)
def test_ac_011_36_any_delivery_order_ends_equal_to_offline_with_corrections_at_l2(
    faults_l2: Generated, order: Order
) -> None:
    _run(faults_l2, order)


@settings(max_examples=50)
@given(order=orders)
@example(order=FIFO)
@example(order=PRICES_LAST)
@example(order=PRICES_FIRST_MESSAGES_LIFO)
@example(order=PRODUCTION_AHEAD)
@example(order=MESSAGES_LAST)
def test_ac_011_36_any_delivery_order_ends_equal_to_offline_with_estimates_at_l8(
    faults_l8: Generated, order: Order
) -> None:
    _run(faults_l8, order)


# ── spec 012: strategy changes, the fake clock, the timing log (T21) ────────────────────────────

T_ADD, T_REMOVE = 8, 16  # steps after first_step: 23:00Z and 01:00Z
BOTH = ("NoBattery", "SelfConsumption")
SINGLE = ("SelfConsumption",)
# A weight 0 holds operator calls back until nothing else is ready (``Order``).
control_weights = st.sampled_from((0, 1, 4, 32, 256))
orders_012 = st.builds(
    Order,
    seed=st.integers(min_value=0, max_value=2**32 - 1),
    weights=st.tuples(weights, weights, weights, weights, weights),
    pick=st.sampled_from(("any", "oldest", "newest")),
    clock=weights,
    control=control_weights,
)
CONTROLS_LAST = Order(6, (1, 1, 1, 1, 1), "any", control=0)
CONTROLS_EARLY = Order(7, (1, 1, 1, 1, 1), "oldest", control=256)
MESSAGES_FIRST = Order(8, (1, 1, 1, 1, 256), "oldest", clock=1)
CLOCK_FIRST = Order(9, (32, 32, 32, 32, 32), "any", clock=256)
# Where the fake clock starts, after the start of the window: an hour before it, at its start,
# three hours and 7.5 s in (a catch-up), and an hour after its end.
START_OFFSETS_S = (-3600.0, 0.0, 3 * 3600.0 + 7.5, 7 * 3600.0)


def _schedule() -> ModuleType:
    return importlib.import_module("ems.assembler.schedule")


def _changes(net: Network) -> Network:
    for key, p in sorted(net.households.items()):
        net.change(key, p.first_step + T_ADD, BOTH)
        net.change(key, p.first_step + T_REMOVE, SINGLE)
    return net


def _sim_with_changes(gen: Generated) -> Network:
    return _changes(Network(gen, with_strategies(gen.params, SINGLE)))


def _live_network(
    gen: Generated, start_offset_s: float, grace_s: int, changes: bool, **kwargs: Any
) -> Network:
    params = live(with_strategies(gen.params, SINGLE) if changes else gen.params, grace_s)
    first = min(p.first_step for p in params.values())
    net = Network(gen, params, now_s=900.0 * first + start_offset_s, **kwargs)
    return _changes(net) if changes else net


def _shadow_checks(net: Network, key: str, live_result: Any, end: int) -> None:
    """``Battery`` and ``Meter`` equal the live strategy's virtual battery and ledger; ``Ems``
    commanded every step; nothing is held anywhere."""
    b = net.state("Battery", key)
    assert b.soc_kwh == live_result.state.soc_kwh, key
    m = net.state("Meter", key)
    assert (m.acc, m.recent, m.months) == (
        live_result.state.acc,
        live_result.recent,
        live_result.months,
    ), key
    e = net.state("Ems", key)
    assert (e.sent, e.waiting) == (end - 1, ()), key
    for service in sorted({name for name, k in net.params if k == key}):
        assert _held(net.state(service, key)) == [], (service, key)


def _twin_checks(gen: Generated, net: Network) -> None:
    schedule = _schedule()
    assert not net.pending()
    for key, p in sorted(net.households.items()):
        h = net.state("Household", key)
        assert len(h.schedule.changes) + len(h.schedule.refused) == 2, key
        twin = gen.twin(p, h.schedule)
        assert h.join == twin.join, key
        windows = schedule.windows(p, h.schedule)
        for name, (start, stop) in windows.items():
            s = net.state(name, key)
            assert s.result == twin.strategies[name], (key, name)
            until = twin.join.next if stop is None else min(stop, twin.join.next)
            assert s.steps == Inbox(expected=max(start, until)), (key, name)
        for (name, k), sp in net.params.items():
            if k == key and name in POLICIES and name not in windows:
                # configured for a change that was refused: never fed
                assert net.state(name, key) == strategy.start(sp)[0], (key, name)
        _shadow_checks(net, key, twin.strategies[p.live], twin.join.next)


def _run_sim_with_changes(gen: Generated, order: Order) -> Network:
    net = _sim_with_changes(gen)
    net.run(order, after=_parity_while_running)
    _twin_checks(gen, net)
    return net


@settings(max_examples=30)
@given(order=orders_012)
@example(order=FIFO)
@example(order=CONTROLS_LAST)
@example(order=CONTROLS_EARLY)
@example(order=PRICES_FIRST_MESSAGES_LIFO)
def test_ac_012_16_sim_with_changes_in_any_order_equals_the_twin_without_faults(
    clean: Generated, order: Order
) -> None:
    _run_sim_with_changes(clean, order)


@settings(max_examples=30)
@given(order=orders_012)
@example(order=FIFO)
@example(order=CONTROLS_LAST)
@example(order=CONTROLS_EARLY)
@example(order=PRODUCTION_AHEAD)
def test_ac_012_16_sim_with_changes_in_any_order_equals_the_twin_with_corrections_at_l2(
    faults_l2: Generated, order: Order
) -> None:
    _run_sim_with_changes(faults_l2, order)


def test_ac_012_13_changes_sent_first_feed_each_strategy_its_own_steps(
    faults_l2: Generated,
) -> None:
    """Both calls handled before any record (as the runner sends them, AC-012-17): accepted; the
    added strategy gets exactly the steps of [+8, +16) and drops the corrections those steps carry
    for slots before +8 (AC-012-14), which the fault data hold."""
    schedule = _schedule()
    net = _sim_with_changes(faults_l2)
    while net.controls:
        net.deliver_control()
    net.run(FIFO, after=_parity_while_running)
    _twin_checks(faults_l2, net)
    dropped = expected_dropped = 0
    for key, p in sorted(net.households.items()):
        t_add, t_remove = p.first_step + T_ADD, p.first_step + T_REMOVE
        h = net.state("Household", key)
        assert h.schedule.changes == (
            schedule.Change(t_add, BOTH),
            schedule.Change(t_remove, SINGLE),
        )
        assert h.schedule.refused == ()
        sent = [
            s.message
            for s in net.sent
            if (s.service, s.handler, s.key) == ("NoBattery", "on_step", key)
        ]
        assert sorted(s.t for s in sent) == list(range(t_add, t_remove)), key
        dropped += net.state("NoBattery", key).result.dropped_corrections
        expected_dropped += sum(
            1
            for s in faults_l2.offline(key).steps
            if t_add <= s.t < t_remove
            for c in s.corrections
            if c.t < t_add
        )
    assert dropped == expected_dropped > 0


def test_ac_012_12_changes_after_their_hour_is_released_are_refused(clean: Generated) -> None:
    """Operator calls held until every step is released: both are refused ``released``, the
    configured NoBattery is never fed, and the run equals the twin without changes."""
    net = _run_sim_with_changes(clean, CONTROLS_LAST)
    for key in net.households:
        h = net.state("Household", key)
        assert h.schedule.changes == ()
        assert [r.reason for r in h.schedule.refused] == ["released", "released"], key


def _one_deadline_pending(net: Network) -> None:
    pending: dict[str, int] = {}
    for m in [*net.in_flight, *(m for _, m in net.delayed)]:
        if (m.service, m.handler) == ("Household", "on_deadline"):
            pending[m.key] = pending.get(m.key, 0) + 1
    assert all(n <= 1 for n in pending.values()), pending


def _live_after(net: Network) -> None:
    _parity_while_running(net)
    _one_deadline_pending(net)


def _live_checks(net: Network) -> None:
    assert not net.pending()
    for key, p in sorted(net.households.items()):
        h = net.state("Household", key)
        assert (h.clock.expected, h.clock.stopped, h.clock.stale) == (None, True, 0), key
        assert h.clock.firings >= 1, key
        assert h.join.next == p.end_step, key  # every step of the window released
        to_ems = [
            s.message.t
            for s in net.sent
            if (s.service, s.handler, s.key) == ("Ems", "on_step", key)
        ]
        assert to_ems == list(range(p.first_step, p.end_step)), key  # in order, none past the end
        for (name, k), sp in sorted(net.params.items()):
            if k != key or name not in POLICIES:
                continue
            got = sorted(
                (
                    s.message
                    for s in net.sent
                    if (s.service, s.handler, s.key) == (name, "on_step", key)
                ),
                key=lambda step: step.t,
            )
            ts = [step.t for step in got]
            assert ts == list(range(sp.first_step, sp.first_step + len(ts))), (key, name)
            folded = start_strategy(sp)
            for step in got:
                folded = step_strategy(sp, name, folded, step)
            state = net.state(name, key)
            assert state.result == folded, (key, name)
            assert state.steps == Inbox(expected=sp.first_step + len(ts)), (key, name)
        _shadow_checks(net, key, net.state(p.live, key).result, p.end_step)


def _run_live(
    gen: Generated, order: Order, start_offset_s: float, grace_s: int, changes: bool
) -> Network:
    net = _live_network(gen, start_offset_s, grace_s, changes)
    net.run(order, after=_live_after)
    _live_checks(net)
    return net


@settings(max_examples=30)
@given(
    order=orders_012,
    start_offset_s=st.sampled_from(START_OFFSETS_S),
    grace_s=st.sampled_from((0, 120, 3600)),
    changes=st.booleans(),
)
@example(order=FIFO, start_offset_s=0.0, grace_s=120, changes=True)
@example(order=MESSAGES_FIRST, start_offset_s=7 * 3600.0, grace_s=120, changes=False)
@example(order=CLOCK_FIRST, start_offset_s=-3600.0, grace_s=0, changes=True)
@example(order=CONTROLS_EARLY, start_offset_s=3 * 3600.0 + 7.5, grace_s=3600, changes=True)
def test_ac_012_16_live_with_a_fake_clock_keeps_the_invariants_without_faults(
    clean: Generated, order: Order, start_offset_s: float, grace_s: int, changes: bool
) -> None:
    _run_live(clean, order, start_offset_s, grace_s, changes)


@settings(max_examples=30)
@given(
    order=orders_012,
    start_offset_s=st.sampled_from(START_OFFSETS_S),
    grace_s=st.sampled_from((0, 120, 3600)),
    changes=st.booleans(),
)
@example(order=FIFO, start_offset_s=0.0, grace_s=120, changes=True)
@example(order=MESSAGES_FIRST, start_offset_s=7 * 3600.0, grace_s=120, changes=False)
@example(order=PRODUCTION_AHEAD, start_offset_s=3 * 3600.0 + 7.5, grace_s=0, changes=True)
def test_ac_012_16_live_with_a_fake_clock_keeps_the_invariants_with_corrections_at_l2(
    faults_l2: Generated, order: Order, start_offset_s: float, grace_s: int, changes: bool
) -> None:
    _run_live(faults_l2, order, start_offset_s, grace_s, changes)


def test_ac_012_16_a_live_run_after_the_window_closes_slots_on_estimates(clean: Generated) -> None:
    """Not vacuous: messages go first, so each first deadline fires an hour after the window, before
    most readings: it releases every step at once on estimates, and the chain stops there."""
    net = _run_live(clean, MESSAGES_FIRST, 7 * 3600.0, 120, False)
    estimated = 0
    for key in net.households:
        h = net.state("Household", key)
        assert h.clock.firings == 1, key
        estimated += h.join.consumption.stream.estimated
    assert estimated > 0


def test_ac_012_16_a_live_run_from_the_window_start_ticks_the_clock(clean: Generated) -> None:
    """Not vacuous: from the start of the window the chain runs one firing per step on delayed
    sends; the clock ticks and every later deadline waits at least 1 s."""
    net = _run_live(clean, FIFO, 0.0, 120, True)
    assert net.ticks > 0
    delayed = [s for s in net.sent if s.handler == "on_deadline" and s.delay_s is not None]
    assert delayed and all((s.delay_s or 0.0) >= 1.0 for s in delayed)
    for key in net.households:
        assert net.state("Household", key).clock.firings > 1, key
        assert key in {s.key for s in delayed}


@settings(max_examples=5)
@given(order=orders_012)
@example(order=FIFO)
@example(order=CONTROLS_EARLY)
def test_ac_012_20_commits_are_identical_with_the_timing_log_on_and_off(
    clean: Generated, tmp_path_factory: pytest.TempPathFactory, order: Order
) -> None:
    path = tmp_path_factory.mktemp("timing") / "timing.jsonl"
    with timing_on(path):
        on = _live_network(clean, 3 * 3600.0 + 7.5, 120, True, timing_log=True)
        on.run(order)
    off = _live_network(clean, 3 * 3600.0 + 7.5, 120, True)
    off.run(order)
    assert on.commits == off.commits
    assert on.sent == off.sent
    assert on.stored == off.stored
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert [(line["svc"], line["key"]) for line in lines] == [
        (c.service, c.key) for c in on.commits
    ]
    assert [[svc for svc, _ in line["sent"]] for line in lines] == [
        [m.service for m in c.sends] for c in on.commits
    ]
