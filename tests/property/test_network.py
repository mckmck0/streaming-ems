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
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from ems.actors.reorder import Inbox
from ems.assembler.offline import Offline
from tests.network import FAULT_RUN_ID, HOURS, RUN_ID, START, Generated, Network, Order, generate

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
