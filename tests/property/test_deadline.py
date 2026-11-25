"""Spec 012, T18 (AC-012-07, -08, -11): the deadline's next delay, for any firing time.

A live household of the network window (site PL-LOC-000001, 24 steps from 2026-10-31T21:00Z)
that has applied its price snapshot, with its chain expecting some step. Hypothesis draws the step
expected, the grace ``G`` and the firing time ``now_s`` from a day before the window to well after
its end (fractions of a second included). For every firing:
- it is counted once, and never as stale;
- the steps it releases are in order and below ``end_step``;
- once the clock step reaches ``end_step`` the chain stops and nothing more is sent;
- otherwise exactly one ``Deadline(nxt)`` goes out, with ``nxt = max(k, first_step) + 1``, a delay
  of at least 1 s, and a due time at which the clock step has reached ``nxt`` (so the next firing is
  not early); the firing is early exactly when ``nxt`` is the step it was sent for.
"""

from __future__ import annotations

import dataclasses
import math
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ems.actors import household, messages, tariff
from ems.actors.household import HouseholdState
from ems.actors.messages import Subscribe
from ems.actors.tariff import TariffState
from ems.assembler.mapping import parse
from ems.assembler.params import RunParams
from tests.network import HOURS, PRICE, RUN_ID, START, generate, managed_params

PV = f"{RUN_ID}:PL-LOC-000001"
DAY_S = 86_400.0


@pytest.fixture(scope="module")
def started() -> tuple[RunParams, HouseholdState]:
    p = dataclasses.replace(managed_params(START, HOURS, 8)[PV], mode="live")
    gen = generate(START, HOURS, 8, False)
    prices = TariffState()
    for raw in gen.of(PRICE, gen.market_key):
        prices, _ = tariff.on_record(prices, parse(raw))
    _, (snapshot,) = tariff.subscribe(prices, Subscribe(1, PV))
    state, _ = household.on_prices(p, household.start(p)[0], snapshot.message)
    assert state.clock.expected == p.first_step
    return p, state


@given(
    expected_offset=st.integers(min_value=0, max_value=24),  # steps after first_step
    grace_s=st.integers(min_value=0, max_value=3600),
    now_offset_s=st.floats(min_value=-DAY_S, max_value=30 * 3600.0, allow_nan=False),
)
def test_ac_012_07_the_next_deadline_is_at_least_1_s_away_and_not_early(
    started: tuple[RunParams, HouseholdState],
    expected_offset: int,
    grace_s: int,
    now_offset_s: float,
) -> None:
    p0, state0 = started
    p = dataclasses.replace(p0, grace_s=grace_s)
    first, end = p.first_step, p.end_step
    t = first + expected_offset
    state = dataclasses.replace(state0, clock=household.Clock(expected=t))
    now = 900.0 * first + now_offset_s

    new, out = household.on_deadline(p, state, messages.Deadline(t), now)

    k = min(math.floor((now - grace_s) / 900), end)
    steps = [s.message.t for s in out if s.service == "Ems"]
    assert steps == sorted(steps) and all(s < end for s in steps)
    deadlines: list[Any] = [s for s in out if s.handler == "on_deadline"]
    assert (new.clock.firings, new.clock.stale) == (1, 0)
    if k >= end:
        assert deadlines == []
        assert (new.clock.expected, new.clock.stopped) == (None, True)
        return
    (d,) = deadlines
    nxt = max(k, first) + 1
    assert (d.service, d.key, d.message) == ("Household", PV, messages.Deadline(nxt))
    assert d.delay_s is not None and d.delay_s >= household.MIN_DELAY_S == 1.0
    assert math.floor((now + d.delay_s - grace_s) / 900) >= nxt  # the next firing is on time
    assert new.clock.expected == nxt and not new.clock.stopped
    assert new.clock.early == int(nxt == t)
