"""AC-012-12 … -15: a household's strategy schedule (spec 012, plan §3 "Schedule").

``accept(params, schedule, next_step, t, strategies)`` sorts and deduplicates the names, then
checks, in this order, and appends either the ``Change`` or a ``Refusal(t, names, reason)``:

| reason | refused when |
|---|---|
| ``not_on_hour`` | ``t`` does not start a UTC hour |
| ``released`` | ``t`` is below the next unreleased step |
| ``after_end`` | ``t`` is at or after ``end_step`` |
| ``not_after_pending`` | ``t`` is not after the last accepted change |
| ``unknown_policy`` | a name is not a policy |
| ``live_removed`` | the live strategy is not in the set |
| ``re_added`` | a name an earlier change dropped comes back |

``strategies_at`` gives the set in force at a step, ``windows`` each strategy's ``[start, stop)``
(``stop`` None while it runs), and ``strategy_params`` the parameters an added strategy runs with
(``first_step = t``, the run's initial SoC), validated like any ``RunParams``.

The run: site PL-LOC-000001, 2026-10-20 x 1 day, ``L`` = 8, live strategy SelfConsumption.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from ems.assembler.params import RunParams
from ems.assembler.schedule import (
    Change,
    Refusal,
    Schedule,
    accept,
    strategies_at,
    strategy_params,
    windows,
)
from tests.unit.assembler.cases import PV_SITE, STRATEGIES, run_params, step_at

FIRST = step_at("2026-10-20T00:00")
END = FIRST + 96
H = 4  # quarter-hours per hour
SC, NB = "SelfConsumption", "NoBattery"
BOTH = (NB, SC)  # sorted


def both() -> RunParams:
    """Starts with NoBattery and SelfConsumption."""
    return run_params(PV_SITE, FIRST, END, 8, strategies=STRATEGIES)


def sc_only() -> RunParams:
    """Starts with SelfConsumption alone."""
    return run_params(PV_SITE, FIRST, END, 8, strategies=(SC,))


def changes(*items: tuple[int, tuple[str, ...]]) -> Schedule:
    return Schedule(changes=tuple(Change(t, names) for t, names in items))


# ── the types ─────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("tp", "names"),
    [
        (Change, ("t", "strategies")),
        (Refusal, ("t", "strategies", "reason")),
        (Schedule, ("changes", "refused")),
    ],
    ids=["Change", "Refusal", "Schedule"],
)
def test_ac_012_12_types_are_frozen_with_pinned_fields(
    tp: type[Any], names: tuple[str, ...]
) -> None:
    """``Schedule`` is stored in ``HouseholdState``: its fields are part of the stored contract."""
    assert tp.__dataclass_params__.frozen
    assert tuple(f.name for f in dataclasses.fields(tp)) == names


def test_ac_012_12_an_empty_schedule() -> None:
    assert Schedule() == Schedule(changes=(), refused=())


# ── accept ────────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_12_an_accepted_change_is_appended() -> None:
    p = both()
    s = accept(p, Schedule(), FIRST, FIRST + 4 * H, (SC,))
    assert s == Schedule(changes=(Change(FIRST + 4 * H, (SC,)),), refused=())
    s = accept(p, s, FIRST + 2, FIRST + 5 * H, (SC,))  # a no-op change is still a change
    assert s.changes == (Change(FIRST + 4 * H, (SC,)), Change(FIRST + 5 * H, (SC,)))
    assert s.refused == ()


@pytest.mark.parametrize(
    "names",
    [(SC, NB), (NB, SC), (SC, NB, NB), (SC, SC, NB, SC)],
    ids=["reversed", "sorted", "a duplicate", "duplicates"],
)
def test_ac_012_12_names_are_sorted_and_deduplicated(names: tuple[str, ...]) -> None:
    s = accept(both(), Schedule(), FIRST, FIRST + H, names)
    assert s.changes == (Change(FIRST + H, BOTH),)
    assert type(s.changes[0].strategies) is tuple and type(s.changes) is tuple


def test_ac_012_12_the_bounds_that_are_accepted() -> None:
    """``t`` equal to the next unreleased step, the run's first step, the last hour of the window,
    and one hour after the pending change."""
    p = both()
    assert accept(p, Schedule(), FIRST + 2 * H, FIRST + 2 * H, BOTH).refused == ()
    assert accept(p, Schedule(), FIRST, FIRST, BOTH).refused == ()
    assert accept(p, Schedule(), FIRST, END - H, BOTH).refused == ()
    pending = changes((FIRST + 3 * H, (SC,)))
    assert accept(p, pending, FIRST, FIRST + 4 * H, (SC,)).refused == ()


REFUSED: list[Any] = [
    pytest.param(both, (), FIRST, FIRST + 5, (SC,), "not_on_hour", id="off the hour"),
    pytest.param(both, (), FIRST, FIRST + H + 3, (SC,), "not_on_hour", id="off the hour (3)"),
    pytest.param(both, (), FIRST + 2 * H, FIRST + H, (SC,), "released", id="released"),
    pytest.param(both, (), FIRST + 2 * H + 1, FIRST + 2 * H, (SC,), "released", id="just released"),
    pytest.param(both, (), FIRST, END, (SC,), "after_end", id="at end_step"),
    pytest.param(both, (), FIRST, END + H, (SC,), "after_end", id="after end_step"),
    pytest.param(
        both,
        ((FIRST + 2 * H, (SC,)),),
        FIRST,
        FIRST + 2 * H,
        BOTH,
        "not_after_pending",
        id="same t",
    ),
    pytest.param(
        both, ((FIRST + 2 * H, (SC,)),), FIRST, FIRST + H, BOTH, "not_after_pending", id="before"
    ),
    pytest.param(both, (), FIRST, FIRST + H, ("Mpc", SC), "unknown_policy", id="unknown policy"),
    pytest.param(both, (), FIRST, FIRST + H, (NB,), "live_removed", id="live removed"),
    pytest.param(both, (), FIRST, FIRST + H, (), "live_removed", id="empty set"),
    pytest.param(
        both, ((FIRST + H, (SC,)),), FIRST, FIRST + 2 * H, BOTH, "re_added", id="initial re-added"
    ),
    pytest.param(
        sc_only,
        ((FIRST + H, BOTH), (FIRST + 2 * H, (SC,))),
        FIRST,
        FIRST + 3 * H,
        BOTH,
        "re_added",
        id="added, removed, re-added",
    ),
]


@pytest.mark.parametrize(("make", "prior", "next_step", "t", "names", "reason"), REFUSED)
def test_ac_012_12_each_refusal_reason(
    make: Any,
    prior: tuple[tuple[int, tuple[str, ...]], ...],
    next_step: int,
    t: int,
    names: tuple[str, ...],
    reason: str,
) -> None:
    schedule = changes(*prior)
    s = accept(make(), schedule, next_step, t, names)
    assert s.changes == schedule.changes  # a refusal changes nothing else
    assert s.refused == (Refusal(t, tuple(sorted(set(names))), reason),)


ORDER: list[Any] = [
    pytest.param(
        Schedule(), END + 9, END + 1, (SC,), "not_on_hour", id="off the hour, released, after end"
    ),
    pytest.param(Schedule(), END + H, END, (SC,), "released", id="released, after end"),
    pytest.param(
        changes((END + H, (SC,))), FIRST, END, (SC,), "after_end", id="after end, not after pending"
    ),
    pytest.param(
        changes((FIRST + 2 * H, (SC,))),
        FIRST,
        FIRST + 2 * H,
        ("Mpc",),
        "not_after_pending",
        id="not after pending, unknown, live removed",
    ),
    pytest.param(Schedule(), FIRST, FIRST + H, ("Mpc",), "unknown_policy", id="unknown, no live"),
    pytest.param(
        changes((FIRST + H, (SC,))),
        FIRST,
        FIRST + 2 * H,
        (NB,),
        "live_removed",
        id="no live, re-add",
    ),
]


@pytest.mark.parametrize(("schedule", "next_step", "t", "names", "reason"), ORDER)
def test_ac_012_12_reasons_are_checked_in_order(
    schedule: Schedule, next_step: int, t: int, names: tuple[str, ...], reason: str
) -> None:
    """Each case breaks the named condition and the ones after it: the first one wins. (A pending
    change at or after ``end_step`` cannot be accepted; it is built by hand to reach the pair.)"""
    s = accept(both(), schedule, next_step, t, names)
    assert s.changes == schedule.changes
    assert [x.reason for x in s.refused] == [reason]


def test_ac_012_12_refusals_accumulate_and_do_not_count_as_changes() -> None:
    """A refused removal of NoBattery does not drop it, so a later change that keeps it is no
    re-add; refusals are kept in arrival order."""
    p = both()
    s = accept(p, Schedule(), FIRST, FIRST + 5, (SC,))
    s = accept(p, s, FIRST, FIRST + H, (NB,))
    s = accept(p, s, FIRST, FIRST + 2 * H, BOTH)
    assert s.changes == (Change(FIRST + 2 * H, BOTH),)
    assert s.refused == (
        Refusal(FIRST + 5, (SC,), "not_on_hour"),
        Refusal(FIRST + H, (NB,), "live_removed"),
    )


def test_ac_012_12_adding_a_strategy_for_the_first_time_is_no_re_add() -> None:
    p = sc_only()
    s = accept(p, Schedule(), FIRST, FIRST + H, BOTH)
    s = accept(p, s, FIRST, FIRST + 2 * H, BOTH)  # kept, not dropped
    s = accept(p, s, FIRST, FIRST + 3 * H, (SC,))  # dropped here
    assert s.refused == ()
    assert [c.t for c in s.changes] == [FIRST + H, FIRST + 2 * H, FIRST + 3 * H]
    s = accept(p, s, FIRST, FIRST + 4 * H, BOTH)
    assert s.refused == (Refusal(FIRST + 4 * H, BOTH, "re_added"),)


# ── strategies_at ─────────────────────────────────────────────────────────────────────────────────


def test_ac_012_13_strategies_at_without_changes_is_the_initial_set() -> None:
    p = both()
    for t in (FIRST - 4, FIRST, FIRST + 1, END - 1):
        assert strategies_at(p, Schedule(), t) == STRATEGIES


def test_ac_012_13_strategies_at_across_several_changes() -> None:
    p = sc_only()
    s = changes((FIRST + 2 * H, BOTH), (FIRST + 5 * H, (SC,)))
    expected = {
        FIRST: (SC,),
        FIRST + 2 * H - 1: (SC,),
        FIRST + 2 * H: BOTH,  # a change applies from its own step on
        FIRST + 5 * H - 1: BOTH,
        FIRST + 5 * H: (SC,),
        END - 1: (SC,),
    }
    assert {t: strategies_at(p, s, t) for t in expected} == expected


def test_ac_012_13_refusals_do_not_change_the_set() -> None:
    p = both()
    s = Schedule(refused=(Refusal(FIRST + H, (SC,), "released"),))
    assert strategies_at(p, s, FIRST + 2 * H) == STRATEGIES


# ── windows ───────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_14_15_windows_of_the_initial_strategies() -> None:
    assert windows(both(), Schedule()) == {NB: (FIRST, None), SC: (FIRST, None)}


def test_ac_012_14_15_windows_of_an_added_then_removed_strategy() -> None:
    s = changes((FIRST + 2 * H, BOTH), (FIRST + 5 * H, (SC,)))
    assert windows(sc_only(), s) == {SC: (FIRST, None), NB: (FIRST + 2 * H, FIRST + 5 * H)}


def test_ac_012_14_15_windows_of_a_removed_initial_strategy() -> None:
    s = changes((FIRST + 3 * H, (SC,)))
    assert windows(both(), s) == {NB: (FIRST, FIRST + 3 * H), SC: (FIRST, None)}


def test_ac_012_14_15_a_change_that_keeps_the_set_moves_no_window() -> None:
    s = changes((FIRST + H, BOTH), (FIRST + 2 * H, BOTH), (FIRST + 3 * H, (SC,)))
    assert windows(sc_only(), s) == {SC: (FIRST, None), NB: (FIRST + H, FIRST + 3 * H)}


# ── strategy_params ───────────────────────────────────────────────────────────────────────────────


def test_ac_012_14_strategy_params_starts_at_t_with_the_runs_initial_soc() -> None:
    p = sc_only()
    q = strategy_params(p, Change(FIRST + 2 * H, BOTH))
    assert q == dataclasses.replace(p, first_step=FIRST + 2 * H, strategies=BOTH)
    assert (q.initial_soc_kwh, q.model, q.end_step, q.live) == (
        p.initial_soc_kwh,
        p.model,
        p.end_step,
        p.live,
    )


@pytest.mark.parametrize(
    "change",
    [
        Change(FIRST + 2 * H + 1, BOTH),  # off the hour
        Change(FIRST + 2 * H, (NB,)),  # the live strategy missing
        Change(FIRST + 2 * H, ("Mpc", SC)),  # an unknown policy
        Change(END, BOTH),  # no step left
    ],
    ids=["off the hour", "live missing", "unknown policy", "at end_step"],
)
def test_ac_012_14_strategy_params_validates(change: Change) -> None:
    with pytest.raises(ValueError):
        strategy_params(sc_only(), change)
