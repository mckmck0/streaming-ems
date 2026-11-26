"""Spec 012, T18 and T19: the household's deadline chain and its strategy changes, as pure
transitions (AC-012-07 … -09, -11 … -13).

Inputs are real: the run parameters of site PL-LOC-000001 (PV) of the 10-household catalog and the
records of the generated 6 h window of ``tests/network.py`` (24 steps from 2026-10-31T21:00Z, so
``first_step`` and ``end_step`` start UTC hours). Expected values come from the functions the
criteria name (``join.on_clock``, ``schedule.accept``, ``schedule.strategies_at``), never from the
transition under test; the times are chosen so that every delay is an exact float.

The deadline (plan §3): a firing for the expected step reads ``now_s`` (journaled), computes the
clock step ``c = ⌊(now_s - G) / 900⌋`` and ``k = min(c, end_step)``, runs ``join.on_clock(k)``,
routes the released steps and, unless ``k`` reached ``end_step``, sends itself ``Deadline(nxt)``
with ``nxt = max(k, first_step) + 1``, delayed to ``900·nxt + G + 1 s`` (at least 1 s). Here ``G``
is 120 s unless a test says otherwise.

The new names (``household.Clock``, ``household.on_deadline``, ``household.on_change``,
``messages.Deadline``, ``messages.StrategyChange``, ``Send.delay_s``, ``ems.assembler.schedule``)
are looked up when a test runs, so the rest of the suite still collects before they exist.
"""

from __future__ import annotations

import dataclasses
import importlib
from types import ModuleType
from typing import Any

import pytest

from ems.actors import household, messages, tariff, weather
from ems.actors.household import HouseholdState
from ems.actors.messages import ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.serde import serde_for
from ems.actors.shell import Send
from ems.actors.tariff import TariffState
from ems.assembler import join
from ems.assembler.join import Step
from ems.assembler.mapping import parse
from ems.assembler.params import RunParams
from tests.network import (
    CONSUMPTION,
    FORECAST,
    HOURS,
    PRICE,
    PRODUCTION,
    RUN_ID,
    START,
    generate,
    managed_params,
)

PV = f"{RUN_ID}:PL-LOC-000001"
G = 120  # s, the default grace
STEP_S = 900


@pytest.fixture(scope="module")
def schedule() -> ModuleType:
    return importlib.import_module("ems.assembler.schedule")


def _params(mode: str = "sim", **changes: Any) -> RunParams:
    p = managed_params(START, HOURS, 8)[PV]
    if mode != "sim":
        changes["mode"] = mode
    return dataclasses.replace(p, **changes) if changes else p


def _gen() -> Any:
    return generate(START, HOURS, 8, False)


def _price_events() -> list[Any]:
    gen = _gen()
    return [parse(raw) for raw in gen.of(PRICE, gen.market_key)]


def _snapshot(key: str = PV) -> PriceUpdate:
    state = TariffState()
    for event in _price_events():
        state, _ = tariff.on_record(state, event)
    _, (send,) = tariff.subscribe(state, Subscribe(1, key))
    return send.message


def _forecast_updates(p: RunParams) -> list[ForecastUpdate]:
    state, out = weather.start(p)[0], []
    for raw in _gen().of(FORECAST, p.household_key):
        state, sends = weather.on_record(p, state, parse(raw))
        out += [s.message for s in sends]
    return out


def _records(p: RunParams) -> list[Any]:
    """The meter records of the household, production then consumption, in partition order."""
    gen = _gen()
    return [
        parse(raw) for topic in (PRODUCTION, CONSUMPTION) for raw in gen.of(topic, p.household_key)
    ]


def _deadline(t: int, delay_s: float | None = None) -> Send:
    return Send("Household", "on_deadline", PV, messages.Deadline(t), delay_s=delay_s)


def _fire(p: RunParams, state: HouseholdState, t: int, now_s: float) -> Any:
    return household.on_deadline(p, state, messages.Deadline(t), now_s)


def _routed(steps: tuple[Step, ...], strategies: tuple[str, ...]) -> list[Send]:
    return [Send(target, "on_step", PV, s) for s in steps for target in (*strategies, "Ems")]


def _split(out: tuple[Send, ...]) -> tuple[list[Send], list[Send]]:
    """The step sends and the deadline sends of one transition, each in send order."""
    steps = [s for s in out if s.handler == "on_step"]
    deadlines = [s for s in out if s.handler == "on_deadline"]
    assert len(steps) + len(deadlines) == len(out), out
    return steps, deadlines


def _clock(**fields: Any) -> Any:
    return household.Clock(**fields)


def _started(p: RunParams) -> HouseholdState:
    """A live household that has applied its price snapshot: its chain expects ``first_step``."""
    state, out = household.on_prices(p, household.start(p)[0], _snapshot())
    assert _split(out)[1] == [_deadline(p.first_step)]
    return state


def _at(t: int, offset_s: float = 0.0) -> float:
    """``offset_s`` after the start of step ``t``, in Unix seconds."""
    return float(STEP_S * t) + offset_s


# ── the chain starts on the snapshot, in live mode only (AC-012-07, -09) ───────────────────────


@pytest.mark.parametrize("mode", ["sim", "live"])
def test_ac_012_07_configure_starts_no_chain(schedule: ModuleType, mode: str) -> None:
    p = _params(mode)
    state, sends = household.start(p)
    assert sends == (Send("Tariff", "subscribe", p.market_key, Subscribe(p.first_step, PV)),)
    assert state.clock == household.Clock()
    assert state.clock.expected is None and not state.clock.stopped
    assert (state.clock.firings, state.clock.early, state.clock.stale) == (0, 0, 0)
    assert state.schedule == schedule.Schedule()


def test_ac_012_07_a_live_household_starts_its_chain_on_the_price_snapshot() -> None:
    p = _params("live")
    snap = _snapshot()
    state, out = household.on_prices(p, household.start(p)[0], snap)
    sim_state, sim_out = household.on_prices(_params(), household.start(_params())[0], snap)
    assert sim_out == ()  # the snapshot alone releases nothing (no forecast, no reading)
    assert out == (_deadline(p.first_step),)  # without delay
    assert out[0].delay_s is None
    assert state.clock == _clock(expected=p.first_step)
    assert state.join == sim_state.join and state.prices == sim_state.prices


def test_ac_012_07_updates_before_the_snapshot_start_no_chain() -> None:
    """A curve update that reaches the household before its snapshot is held; only the snapshot
    starts the chain, once."""
    p = _params("live")
    first, second = _price_events()
    state1, _ = tariff.on_record(tariff.subscribe(TariffState(), Subscribe(1, "other"))[0], first)
    _, (update,) = tariff.on_record(state1, second)
    assert not update.message.snapshot

    held, out = household.on_prices(p, household.start(p)[0], update.message)
    assert out == () and held.clock == household.Clock()
    state, out = household.on_prices(p, held, _snapshot())
    assert _split(out)[1] == [_deadline(p.first_step)]
    assert state.clock == _clock(expected=p.first_step)


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("second snapshot", id="a second snapshot"),
        pytest.param("stopped", id="after the chain stopped"),
        pytest.param("expected", id="while a deadline is expected"),
    ],
)
def test_ac_012_07_a_snapshot_starts_at_most_one_chain(case: str) -> None:
    p = _params("live")
    if case == "second snapshot":
        state = _started(p)
    else:
        clock = _clock(stopped=True) if case == "stopped" else _clock(expected=p.first_step + 3)
        state = dataclasses.replace(household.start(p)[0], clock=clock)
    new, out = household.on_prices(p, state, _snapshot())
    assert _split(out)[1] == []
    assert new.clock == state.clock


def test_ac_012_09_a_sim_household_never_schedules_a_deadline() -> None:
    """The whole window in sim: snapshot, forecasts and every record; every step is released and
    no send is a deadline or has a delay."""
    p = _params()
    state, sends = household.start(p)
    all_sends = list(sends)
    state, out = household.on_prices(p, state, _snapshot())
    all_sends += out
    for update in _forecast_updates(p):
        state, out = household.on_forecast(p, state, update)
        all_sends += out
    for event in _records(p):
        state, out = household.on_record(p, state, event)
        all_sends += out
    assert state.join.next == p.end_step  # every step released
    assert [s for s in all_sends if s.handler == "on_deadline"] == []
    assert all(s.delay_s is None for s in all_sends)
    assert state.clock == household.Clock()


# ── stale deadlines (AC-012-08) ─────────────────────────────────────────────────────────────────


def _stopped(p: RunParams) -> HouseholdState:
    state, out = _fire(p, _started(p), p.first_step, _at(p.end_step, G))
    assert state.clock.stopped and _split(out)[1] == []
    return state


STALE = [
    pytest.param(lambda p: (household.start(p)[0], p.first_step), id="before the chain started"),
    pytest.param(lambda p: (_started(p), p.first_step + 1), id="another step"),
    pytest.param(
        lambda p: (
            _fire(p, _started(p), p.first_step, _at(p.first_step + 2, G + 1))[0],
            p.first_step,
        ),
        id="an earlier step",
    ),
    pytest.param(lambda p: (_stopped(p), p.end_step), id="after the chain stopped"),
]


@pytest.mark.parametrize("case", STALE)
def test_ac_012_08_a_deadline_for_another_step_is_counted_and_sends_nothing(case: Any) -> None:
    p = _params("live")
    state, t = case(p)
    assert state.clock.expected != t
    # Late enough that a firing would release every step: a stale one releases nothing.
    new, out = _fire(p, state, t, _at(p.end_step + 4, G))
    assert out == ()
    assert new == dataclasses.replace(
        state, clock=dataclasses.replace(state.clock, stale=state.clock.stale + 1)
    )
    assert new.join == state.join


# ── firings (AC-012-07) ─────────────────────────────────────────────────────────────────────────


def test_ac_012_07_a_late_firing_releases_every_due_step_at_once() -> None:
    """Catch-up: ``now`` is 37 s after step +10 started plus G + 1 s, so 10 steps are due. One
    firing closes their slots on estimates, releases them, then lines up with the next boundary
    plus G."""
    p = _params("live")
    state = _started(p)
    first = p.first_step
    now = _at(first + 10, G + 1 + 37)
    expected_join, steps = join.on_clock(p, state.join, first + 10)
    assert [s.t for s in steps] == list(range(first, first + 10))  # the case needs a catch-up

    new, out = _fire(p, state, first, now)
    routed, deadlines = _split(out)
    assert routed == _routed(steps, p.strategies)
    assert deadlines == [_deadline(first + 11, 863.0)]  # 900·(first+11) + 121 - now
    assert now + 863.0 == _at(first + 11, G + 1)
    assert new == dataclasses.replace(
        state, join=expected_join, clock=_clock(expected=first + 11, firings=1)
    )


def test_ac_012_07_an_on_time_firing_moves_the_chain_one_step() -> None:
    p = _params("live")
    first = p.first_step
    state, _ = _fire(p, _started(p), first, _at(first + 2, G + 1))
    assert state.clock == _clock(expected=first + 3, firings=1)
    expected_join, steps = join.on_clock(p, state.join, first + 3)

    new, out = _fire(p, state, first + 3, _at(first + 3, G + 1))
    routed, deadlines = _split(out)
    assert routed == _routed(steps, p.strategies)
    assert deadlines == [_deadline(first + 4, 900.0)]
    assert new == dataclasses.replace(
        state, join=expected_join, clock=_clock(expected=first + 4, firings=2)
    )


@pytest.mark.parametrize("lead_s", [1.25, 5.0, 899.0])
def test_ac_012_07_an_early_firing_reschedules_the_same_step(lead_s: float) -> None:
    """The Docker VM clock leads the host: the deadline for ``first + 3`` fires ``lead_s`` before
    its time, so the clock step has not reached it. It counts the firing as early, releases nothing
    new and sends the same step again after the lead (at least 1 s)."""
    p = _params("live")
    first = p.first_step
    state, _ = _fire(p, _started(p), first, _at(first + 2, G + 1))
    assert state.clock.expected == first + 3
    expected_join, steps = join.on_clock(p, state.join, first + 2)
    assert steps == ()

    new, out = _fire(p, state, first + 3, _at(first + 3, G + 1) - lead_s)
    assert out == (_deadline(first + 3, lead_s),)
    assert out[0].delay_s is not None and out[0].delay_s >= household.MIN_DELAY_S == 1.0
    assert new == dataclasses.replace(
        state, join=expected_join, clock=_clock(expected=first + 3, firings=2, early=1)
    )


@pytest.mark.parametrize(
    "now",
    [
        pytest.param(lambda p: _at(p.end_step, G), id="the last step ended G ago"),
        pytest.param(lambda p: _at(p.end_step + 96, G + 1), id="a day after the end"),
    ],
)
def test_ac_012_07_the_chain_stops_at_end_step(now: Any) -> None:
    p = _params("live")
    first = p.first_step
    state, _ = _fire(p, _started(p), first, _at(first + 2, G + 1))
    expected_join, steps = join.on_clock(p, state.join, p.end_step)  # capped at end_step
    assert [s.t for s in steps] == list(range(first + 2, p.end_step))

    new, out = _fire(p, state, first + 3, now(p))
    routed, deadlines = _split(out)
    assert routed == _routed(steps, p.strategies)
    assert deadlines == []
    assert new == dataclasses.replace(
        state, join=expected_join, clock=_clock(expected=None, stopped=True, firings=2)
    )
    assert max(s.message.t for s in routed) == p.end_step - 1  # nothing at or past end_step


@pytest.mark.parametrize("grace_s", [0, 120, 3600])
def test_ac_012_07_a_window_in_the_future_fires_next_at_the_first_steps_end_plus_g(
    grace_s: int,
) -> None:
    """The first deadline fires an hour before the window: it releases nothing and waits until the
    first step has ended G ago, plus the 1 s margin."""
    p = _params("live", grace_s=grace_s)
    first = p.first_step
    state = _started(p)
    now = _at(first, -3600.0)
    c = (STEP_S * first - 3600 - grace_s) // STEP_S
    expected_join, steps = join.on_clock(p, state.join, c)
    assert steps == ()

    new, out = _fire(p, state, first, now)
    (send,) = out
    assert send == _deadline(first + 1, float(STEP_S + 3600 + grace_s + 1))
    assert now + send.delay_s == _at(first + 1, grace_s + 1)
    assert new == dataclasses.replace(
        state, join=expected_join, clock=_clock(expected=first + 1, firings=1)
    )


def test_ac_012_07_every_firing_that_is_not_stale_is_counted() -> None:
    """The whole chain from a window that starts 10 min in the future, each deadline delivered at
    its delay, with one early firing and one stale deadline in between: 26 firings (the first, one
    per step, the early one), one early, one stale; every step released once, in order, and none at
    or past ``end_step``."""
    p = _params("live")
    first, end = p.first_step, p.end_step
    state = _started(p)
    t, now = first, _at(first, -600.0)
    released: list[int] = []
    deliveries = 0
    while t is not None:
        if t == first + 5 and state.clock.early == 0:
            now -= 30.0  # this one fires 30 s early
        if t == first + 7 and state.clock.stale == 0:
            state, out = _fire(p, state, first + 2, now)  # an old deadline arrives first
            assert out == ()
        state, out = _fire(p, state, t, now)
        deliveries += 1
        routed, deadlines = _split(out)
        released += [s.message.t for s in routed if s.service == "Ems"]
        if not deadlines:
            break
        (d,) = deadlines
        assert d.delay_s is not None and d.delay_s >= 1.0
        t, now = d.message.t, now + d.delay_s
    assert deliveries == 26
    assert state.clock == _clock(expected=None, stopped=True, firings=26, early=1, stale=1)
    assert released == list(range(first, end))
    assert state.join.next == end


def test_ac_012_11_a_retried_firing_writes_the_same_bytes_and_sends_the_same_messages() -> None:
    p = _params("live")
    state = _started(p)
    now = _at(p.first_step + 6, G + 1 + 0.5)
    runs = [_fire(p, state, p.first_step, now) for _ in range(2)]
    state_bytes = [serde_for(HouseholdState).serialize(s) for s, _ in runs]
    sent = [
        [
            (
                x.service,
                x.handler,
                x.key,
                serde_for(type(x.message)).serialize(x.message),
                x.delay_s,
            )
            for x in out
        ]
        for _, out in runs
    ]
    assert state_bytes[0] == state_bytes[1]
    assert sent[0] == sent[1] and len(sent[0]) > 1


# ── strategy changes (AC-012-12, -13) ───────────────────────────────────────────────────────────


def _single(mode: str = "sim") -> RunParams:
    """The household starting with SelfConsumption only (live), so NoBattery can be added."""
    return _params(mode, strategies=("SelfConsumption",))


def _change(p: RunParams, state: HouseholdState, t: int, strategies: tuple[str, ...]) -> Any:
    return household.on_change(p, state, messages.StrategyChange(t, strategies))


def test_ac_012_12_set_strategies_is_accepted_and_stored(schedule: ModuleType) -> None:
    p = _single()
    state = household.start(p)[0]
    t = p.first_step + 8
    strategies = ("SelfConsumption", "NoBattery", "NoBattery")  # unsorted, with a repeat
    expected = schedule.accept(p, state.schedule, state.join.next, t, strategies)

    new, out = _change(p, state, t, strategies)
    assert out == ()
    assert new.schedule == expected
    assert new.schedule.changes == (schedule.Change(t, ("NoBattery", "SelfConsumption")),)
    assert new.schedule.refused == ()
    assert dataclasses.replace(new, schedule=state.schedule) == state


def _released_state(p: RunParams) -> HouseholdState:
    """Every step released (sim): the next unreleased step is ``end_step``."""
    state = household.on_prices(p, household.start(p)[0], _snapshot())[0]
    for update in _forecast_updates(p):
        state = household.on_forecast(p, state, update)[0]
    for event in _records(p):
        state = household.on_record(p, state, event)[0]
    assert state.join.next == p.end_step
    return state


def _with(p: RunParams, *changes: tuple[int, tuple[str, ...]]) -> HouseholdState:
    state = household.start(p)[0]
    for t, strategies in changes:
        state, _ = _change(p, state, p.first_step + t, strategies)
    assert len(state.schedule.changes) == len(changes) and state.schedule.refused == ()
    return state


BOTH = ("NoBattery", "SelfConsumption")
REFUSED = [
    pytest.param(
        _single, lambda p: household.start(p)[0], 9, BOTH, "not_on_hour", id="not_on_hour"
    ),
    pytest.param(_single, _released_state, 8, BOTH, "released", id="released"),
    pytest.param(_single, lambda p: household.start(p)[0], 24, BOTH, "after_end", id="after_end"),
    pytest.param(
        _single,
        lambda p: _with(p, (8, BOTH)),
        8,
        ("SelfConsumption",),
        "not_after_pending",
        id="same t",
    ),
    pytest.param(
        _single,
        lambda p: _with(p, (8, BOTH)),
        4,
        BOTH,
        "not_after_pending",
        id="before a pending t",
    ),
    pytest.param(
        _single,
        lambda p: household.start(p)[0],
        8,
        ("Foo", "SelfConsumption"),
        "unknown_policy",
        id="unknown_policy",
    ),
    pytest.param(
        _single,
        lambda p: household.start(p)[0],
        8,
        ("NoBattery",),
        "live_removed",
        id="live_removed",
    ),
    pytest.param(
        lambda: _params(),
        lambda p: _with(p, (4, ("SelfConsumption",))),
        8,
        BOTH,
        "re_added",
        id="re_added",
    ),
]


@pytest.mark.parametrize(("make_params", "make_state", "offset", "strategies", "reason"), REFUSED)
def test_ac_012_12_set_strategies_is_refused_with_its_reason(
    schedule: ModuleType,
    make_params: Any,
    make_state: Any,
    offset: int,
    strategies: tuple[str, ...],
    reason: str,
) -> None:
    p = make_params()
    state = make_state(p)
    t = p.first_step + offset
    expected = schedule.accept(p, state.schedule, state.join.next, t, strategies)

    new, out = _change(p, state, t, strategies)
    assert out == ()
    assert new.schedule == expected
    assert new.schedule.changes == state.schedule.changes  # nothing else changes
    assert len(new.schedule.refused) == len(state.schedule.refused) + 1
    refusal = new.schedule.refused[-1]
    assert (refusal.t, refusal.reason) == (t, reason)
    assert dataclasses.replace(new, schedule=state.schedule) == state


def _targets(out: tuple[Send, ...]) -> dict[int, set[str]]:
    """The services each released step went to, by step; steps go out in ``t`` order."""
    routed = [s for s in out if s.handler == "on_step"]
    ts = [s.message.t for s in routed]
    assert ts == sorted(ts)
    by_t: dict[int, set[str]] = {}
    for s in routed:
        assert s.key == PV
        by_t.setdefault(s.message.t, set()).add(s.service)
    return by_t


def test_ac_012_13_one_sim_batch_across_two_changes_routes_each_step_to_its_own_set(
    schedule: ModuleType,
) -> None:
    """NoBattery joins at +8 (23:00Z) and leaves at +16 (01:00Z). Forecasts and every reading come
    first, the price snapshot last, so the snapshot releases all 24 steps in one batch."""
    p = _single()
    first = p.first_step
    state = _with(p, (8, BOTH), (16, ("SelfConsumption",)))
    for update in _forecast_updates(p):
        state, out = household.on_forecast(p, state, update)
        assert out == ()
    for event in _records(p):
        state, out = household.on_record(p, state, event)
        assert out == ()
    state, out = household.on_prices(p, state, _snapshot())

    by_t = _targets(out)
    assert sorted(by_t) == list(range(first, p.end_step))  # one batch, across both changes
    for t, services in by_t.items():
        expected = {*schedule.strategies_at(p, state.schedule, t), "Ems"}
        assert services == expected, t
        assert services == (
            {"NoBattery", "SelfConsumption", "Ems"}
            if first + 8 <= t < first + 16
            else {"SelfConsumption", "Ems"}
        ), t
    steps = {s.message.t: s.message for s in out}
    assert (
        tuple(steps[t] for t in sorted(steps)) == _gen().offline(PV).steps
    )  # the join is unchanged


def test_ac_012_13_one_live_catch_up_across_a_change_routes_each_step_to_its_own_set(
    schedule: ModuleType,
) -> None:
    """Live: NoBattery joins at +4; one late firing releases steps +0 … +7 at once."""
    p = _single("live")
    first = p.first_step
    state = _started(p)
    state, _ = _change(p, state, first + 4, BOTH)
    assert state.schedule.changes == (schedule.Change(first + 4, BOTH),)

    _, out = _fire(p, state, first, _at(first + 8, G + 1))
    by_t = _targets(out)
    assert sorted(by_t) == list(range(first, first + 8))
    for t, services in by_t.items():
        assert services == (
            {"NoBattery", "SelfConsumption", "Ems"}
            if t >= first + 4
            else {"SelfConsumption", "Ems"}
        ), t
