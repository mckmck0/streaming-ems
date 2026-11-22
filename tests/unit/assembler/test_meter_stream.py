"""AC-011-08 … -13: one meter stream under the allowed-lateness model of ``01-architecture.md``
("Late and missing measurements"), at the boundaries of its four cases, counted in positions of the
stream's own records (``L`` = ``late_slots``):

1. the reading of slot ``q`` arrives before the stream delivered ``q + L``: used, not estimated;
2. the stream delivers ``q + L`` without it: ``q`` closes on the previous value, ``estimated``;
3. it arrives after that while the newest slot is below ``q + 2L``: a ``Late`` for step newest + 1;
4. later still (newest ≥ ``q + 2L``): ``too_old``, ignored.

Plus a gap at the start (estimate 0), a jump after a gap that must not lose the reading behind it,
duplicate and conflict, and records outside ``[first_step, end_step)``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable

import pytest

from ems.assembler.mapping import Reading
from ems.assembler.meter_stream import Closed, Late, MeterStream, meter_apply

FIRST, END = 100, 200


def r(t: int, kwh: float | None = None, eid: str | None = None, skipped: int = 0) -> Reading:
    """Slot ``FIRST + t``; its value defaults to ``t + 0.5`` kWh and its id to ``e<t>``."""
    return Reading(
        "consumption",
        FIRST + t,
        eid if eid is not None else f"e{t}",
        kwh if kwh is not None else t + 0.5,
        skipped,
    )


def c(t: int, kwh: float, estimated: bool = False) -> Closed:
    return Closed(FIRST + t, kwh, estimated)


def late(t: int, kwh: float, at: int) -> Late:
    return Late(FIRST + t, kwh, FIRST + at)


def feed(
    records: Iterable[Reading], late_slots: int, s: MeterStream | None = None
) -> tuple[MeterStream, list[Closed], list[Late]]:
    s = s if s is not None else MeterStream(closed=FIRST)
    closed: list[Closed] = []
    lates: list[Late] = []
    for rec in records:
        s, out, lt = meter_apply(s, FIRST, END, late_slots, rec)
        closed += out
        if lt is not None:
            lates.append(lt)
    return s, closed, lates


def test_a_stream_starts_empty() -> None:
    s = MeterStream(closed=FIRST)
    assert (s.newest, s.last_kwh, s.slots) == (None, 0.0, ())
    counters = (s.before_start, s.after_end, s.too_old, s.duplicate, s.conflict, s.estimated)
    assert (*counters, s.late, s.skipped) == (0,) * 8


def test_ac_011_08_in_order_readings_close_at_once() -> None:
    s, closed, lates = feed([r(0), r(1), r(2)], 2)
    assert closed == [c(0, 0.5), c(1, 1.5), c(2, 2.5)]
    assert lates == [] and s.closed == FIRST + 3 and s.newest == FIRST + 2
    assert s.last_kwh == 2.5 and s.estimated == 0


@pytest.mark.parametrize("late_slots", [1, 2, 3, 8])
def test_ac_011_08_a_reading_before_q_plus_l_is_used(late_slots: int) -> None:
    """Slot 1 arrives when the stream has delivered every slot up to 1 + L - 1 but itself."""
    s, closed, _ = feed([r(0), *(r(t) for t in range(2, late_slots + 1))], late_slots)
    assert closed == [c(0, 0.5)]  # slot 1 waits
    s, closed, lates = feed([r(1)], late_slots, s)
    assert closed[0] == c(1, 1.5) and lates == []
    assert [x.t - FIRST for x in closed] == list(range(1, late_slots + 1))
    assert not any(x.estimated for x in closed) and s.estimated == 0


@pytest.mark.parametrize("late_slots", [1, 2, 3, 8])
def test_ac_011_10_slot_q_is_estimated_when_the_stream_delivers_q_plus_l(late_slots: int) -> None:
    s, closed, _ = feed([r(0)], late_slots)
    if late_slots > 1:
        s, closed, _ = feed([r(late_slots)], late_slots, s)
        assert closed == []  # newest = 1 + L - 1: slot 1 still waits
    s, closed, _ = feed([r(late_slots + 1)], late_slots, s)
    assert closed[0] == c(1, 0.5, estimated=True)  # the value used for slot 0
    assert s.estimated == 1


def test_ac_011_10_an_estimate_repeats_the_previous_estimate() -> None:
    s, closed, _ = feed([r(0, 0.7), r(5)], 2)  # newest 5 closes 1, 2, 3 on estimates
    assert closed == [c(0, 0.7), c(1, 0.7, True), c(2, 0.7, True), c(3, 0.7, True)]
    assert s.estimated == 3 and s.closed == FIRST + 4


def test_ac_011_10_a_gap_at_the_start_closes_on_zero() -> None:
    s, closed, _ = feed([r(2)], 2)
    assert closed == [c(0, 0.0, estimated=True)]
    s, closed, _ = feed([r(1)], 2, s)
    assert closed == [c(1, 1.5), c(2, 2.5)]
    assert s.estimated == 1 and s.late == 0


def test_ac_011_09_a_jump_after_a_gap_keeps_the_reading_behind_it() -> None:
    """L = 2, slots 1 then 5: slot 1 is at newest - 2L when 5 arrives, so it must close before the
    prune drops it."""
    s, closed, _ = feed([r(1, 0.9)], 2)
    assert closed == []
    s, closed, _ = feed([r(5)], 2, s)
    assert closed == [c(0, 0.0, True), c(1, 0.9), c(2, 0.9, True), c(3, 0.9, True)]
    assert s.closed == FIRST + 4 and [t for t, _, _ in s.slots] == [FIRST + 5]


@pytest.mark.parametrize("late_slots", [1, 2, 3, 8])
def test_ac_011_11_a_late_reading_right_after_its_estimate(late_slots: int) -> None:
    """Lower bound: estimated when newest = q + L, arrives at once: ``at_step`` = q + L + 1."""
    s, _, _ = feed([r(0), r(late_slots + 1)], late_slots)
    s, closed, lates = feed([r(1, 4.0)], late_slots, s)
    assert lates == [late(1, 4.0, late_slots + 2)] and closed == []
    assert s.late == 1 and s.estimated == 1


@pytest.mark.parametrize("late_slots", [1, 2, 3, 8])
def test_ac_011_11_a_late_reading_at_the_last_moment(late_slots: int) -> None:
    """Upper bound: newest = q + 2L - 1 when it arrives: ``at_step`` = q + 2L."""
    q = 1
    newest = q + 2 * late_slots - 1
    s, _, _ = feed([r(0), r(newest)], late_slots)
    assert s.closed > FIRST + q  # q was closed on an estimate
    before = s
    s, closed, lates = feed([r(q, 4.0)], late_slots, s)
    assert lates == [late(q, 4.0, q + 2 * late_slots)] and closed == []
    assert s.newest == before.newest and s.closed == before.closed
    assert s.last_kwh == before.last_kwh  # a correction changes no later estimate


@pytest.mark.parametrize("late_slots", [1, 2, 3, 8])
def test_ac_011_12_a_reading_from_q_plus_2l_on_is_too_old(late_slots: int) -> None:
    q = 1
    s, _, _ = feed([r(0), r(q + 2 * late_slots)], late_slots)
    s2, closed, lates = feed([r(q, 4.0)], late_slots, s)
    assert closed == [] and lates == []
    assert s2 == dataclasses.replace(s, too_old=1)


def test_ac_011_11_the_late_value_is_kept_for_its_slot_and_a_copy_is_a_duplicate() -> None:
    s, _, _ = feed([r(0), r(3)], 2)
    s, _, lates = feed([r(1, 4.0)], 2, s)
    assert lates == [late(1, 4.0, 4)]
    s2, closed, lates = feed([r(1, 4.0)], 2, s)
    assert (closed, lates) == ([], []) and s2 == dataclasses.replace(s, duplicate=1)


def test_ac_011_13_duplicate_and_conflict() -> None:
    s, _, _ = feed([r(0, 1.0, "a"), r(2, 2.0, "b")], 3)
    dup, closed, lates = feed([r(0, 1.0, "a")], 3, s)
    assert (closed, lates) == ([], []) and dup == dataclasses.replace(s, duplicate=1)
    pending, closed, lates = feed([r(2, 2.0, "b")], 3, s)  # slot 2 not closed yet
    assert (closed, lates) == ([], []) and pending == dataclasses.replace(s, duplicate=1)
    other, closed, lates = feed([r(0, 9.0, "z")], 3, s)
    assert (closed, lates) == ([], []) and other == dataclasses.replace(s, conflict=1)
    # first reading wins: slot 2 still closes with b's value
    _, closed, _ = feed([r(2, 7.0, "y"), r(1, 1.5, "c")], 3, s)
    assert closed == [c(1, 1.5), c(2, 2.0)]


def test_ac_011_12_outside_the_window_is_counted_and_ignored() -> None:
    s, _, _ = feed([r(0), r(1)], 2)
    for rec, counter in (
        (Reading("consumption", FIRST - 1, "x", 1.0, 0), "before_start"),
        (Reading("consumption", FIRST - 50, "y", 1.0, 0), "before_start"),
        (Reading("consumption", END, "z", 1.0, 0), "after_end"),
        (Reading("consumption", END + 7, "w", 1.0, 0), "after_end"),
    ):
        s2, closed, lt = meter_apply(s, FIRST, END, 2, rec)
        assert (closed, lt) == ((), None)
        assert s2 == dataclasses.replace(s, **{counter: getattr(s, counter) + 1})
    s2, closed, _ = meter_apply(s, FIRST, END, 2, Reading("consumption", END - 1, "v", 1.0, 0))
    assert s2.newest == END - 1 and s2.after_end == 0


def test_the_last_slot_of_the_window_closes_on_its_reading() -> None:
    _, closed, _ = feed([r(t) for t in range(END - FIRST)], 1)
    assert [x.t for x in closed] == list(range(FIRST, END))


def test_skipped_entries_are_counted_when_the_reading_is_stored() -> None:
    s, _, _ = feed([r(0, skipped=2), r(1, skipped=1)], 2)
    assert s.skipped == 3
    s2, _, _ = feed([r(1, skipped=5)], 2, s)  # a duplicate adds nothing
    assert s2.skipped == 3 and s2.duplicate == 1


def test_ac_011_09_slots_stay_sorted_and_at_most_2l() -> None:
    s, _, _ = feed([r(3), r(1)], 3)
    assert s.slots == ((FIRST + 1, "e1", 1.5), (FIRST + 3, "e3", 3.5))
    s, _, _ = feed([r(t) for t in range(4, 20)], 3, s)
    assert [t for t, _, _ in s.slots] == list(
        range(FIRST + 14, FIRST + 20)
    )  # (newest - 2L, newest]


def test_counters_of_a_mixed_run() -> None:
    # L = 2: 0; 3 (1 estimated, 2 waits); 1 late (at 4); 2 closes 2 and 3; 9 (4 … 7 estimated,
    # slots up to 5 pruned); 4 too old (4 <= 9 - 4); 6 late (at 10); 9 duplicate; 6 conflict.
    records = [r(0), r(3), r(1), r(2), r(9), r(4), r(6), r(9), r(6, 1.0, "q")]
    s, closed, lates = feed(records, 2)
    assert closed == [
        c(0, 0.5),
        c(1, 0.5, True),
        c(2, 2.5),
        c(3, 3.5),
        c(4, 3.5, True),
        c(5, 3.5, True),
        c(6, 3.5, True),
        c(7, 3.5, True),
    ]
    assert lates == [late(1, 1.5, 4), late(6, 6.5, 10)]
    assert (s.estimated, s.late, s.too_old, s.duplicate, s.conflict) == (5, 2, 1, 1, 1)
