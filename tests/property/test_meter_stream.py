"""AC-011-08 … -13 as properties of one meter stream, for any arrival order of a set of slots with
gaps, lateness, duplicates and conflicts (``L`` in 1 … 3, streams of up to 24 slots, some of them
outside the run window):

- slots close once each, in ascending order from ``first_step`` (AC-011-09);
- a reading that arrives before its stream delivered ``t + L`` closes unestimated with its value
  (AC-011-08), and an unestimated slot always carries the first reading of the slot (AC-011-13);
- an estimate equals the value the stream closed one slot earlier, 0 before any (AC-011-10);
- a ``Late`` names step newest + 1, lies in ``[t + L + 1, t + 2L]``, is the first reading of a slot
  closed on an estimate, and comes once per slot (AC-011-11);
- a copy of a record inserted anywhere after it changes no output, only one counter (AC-011-13);
- at most ``2L`` slots are kept, sorted; a fold that never prunes gives the same outputs
  (AC-011-09).

Spec 012 (plan §2), at the end of the file:
- without a clock the horizon ``max(newest, closed - 1)`` is the newest reading after every reading
  (AC-012-01), so judging lateness against it changes nothing in sim;
- with non-decreasing ``meter_close`` calls interleaved anywhere, the outputs equal a never-pruned
  oracle of the spec's rules, slots close once and in order, each close reaches its ``until``, and
  every accepted reading is accounted for exactly once: its slot's value, one ``Late``, or still
  waiting; the rest are ``too_old``, ``duplicate`` or ``conflict`` (AC-012-02, -03, -06).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass, field

from hypothesis import given
from hypothesis import strategies as st

from ems.assembler import meter_stream as ms
from ems.assembler.mapping import Reading
from ems.assembler.meter_stream import Closed, Late, MeterStream, meter_apply

BASE = 1_000
SPAN = 24  # slots BASE … BASE + 23
FIRST, END = BASE + 2, BASE + 22  # two slots before the window and two after it
REFUSALS = ("before_start", "after_end", "too_old", "duplicate", "conflict")

kwh_values = st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False)


@st.composite
def arrivals(draw: st.DrawFn) -> tuple[int, list[Reading]]:
    """``(L, records)``: a set of slots, each delayed by up to ``3L`` positions or fully shuffled,
    plus up to four repeats (the same record, or another record of the slot)."""
    late_slots = draw(st.integers(min_value=1, max_value=3))
    slots = sorted(draw(st.sets(st.integers(min_value=0, max_value=SPAN - 1), max_size=SPAN)))
    keyed: list[tuple[int, int, Reading]] = []
    for t in slots:
        delay = draw(st.one_of(st.just(0), st.integers(min_value=0, max_value=3 * late_slots)))
        rec = Reading(
            "consumption",
            BASE + t,
            f"m{t}",
            draw(kwh_values),
            draw(st.integers(min_value=0, max_value=2)),
        )
        keyed.append((t + delay, len(keyed), rec))
    for _ in range(draw(st.integers(min_value=0, max_value=4))):
        if not keyed:
            break
        key, _, rec = draw(st.sampled_from(keyed))
        if not draw(st.booleans()):
            rec = Reading(rec.stream, rec.t, rec.event_id + "-other", draw(kwh_values), 0)
        gap = draw(st.integers(min_value=0, max_value=2 * late_slots))
        keyed.append((key + gap, len(keyed), rec))
    if draw(st.booleans()):
        ordered = sorted(keyed, key=lambda k: (k[0], k[1]))
    else:
        ordered = draw(st.permutations(keyed))
    return late_slots, [rec for _, _, rec in ordered]


@dataclass
class Trace:
    stream: MeterStream
    closed: list[Closed] = field(default_factory=list)
    lates: list[Late] = field(default_factory=list)
    on_time: dict[int, float] = field(default_factory=dict)  # slot -> value, arrived before t + L
    first: dict[int, float] = field(default_factory=dict)  # slot -> value of its stored reading


def fold(late_slots: int, records: list[Reading]) -> Trace:
    """Apply ``records`` one by one, checking the per-step properties on the way."""
    trace = Trace(MeterStream(closed=FIRST))
    for rec in records:
        before = trace.stream
        in_window = FIRST <= rec.t < END
        fresh = (
            in_window
            and rec.t not in trace.first
            and (before.newest is None or rec.t > before.newest - 2 * late_slots)
        )
        if fresh and (before.newest is None or before.newest < rec.t + late_slots):
            trace.on_time[rec.t] = rec.kwh
        if fresh:
            trace.first[rec.t] = rec.kwh
        s, out, lt = meter_apply(before, FIRST, END, late_slots, rec)
        if lt is not None:
            assert before.newest is not None and lt.at_step == before.newest + 1
            assert (lt.t, lt.kwh) == (rec.t, rec.kwh)
            trace.lates.append(lt)
        assert len(s.slots) <= 2 * late_slots
        ts = [t for t, _, _ in s.slots]
        assert ts == sorted(set(ts))
        trace.stream = s
        trace.closed += out
    return trace


def oracle(late_slots: int, records: list[Reading]) -> tuple[list[Closed], list[Late]]:
    """The rules of plan §4 with a slot table that is never pruned."""
    stored: dict[int, float] = {}
    closed, newest, last = FIRST, None, 0.0
    out: list[Closed] = []
    lates: list[Late] = []
    for rec in records:
        if not FIRST <= rec.t < END:
            continue
        if newest is not None and rec.t <= newest - 2 * late_slots:
            continue
        if rec.t in stored:
            continue
        stored[rec.t] = rec.kwh
        if rec.t < closed:
            assert newest is not None
            lates.append(Late(rec.t, rec.kwh, newest + 1))
        newest = rec.t if newest is None else max(newest, rec.t)
        while closed in stored or newest >= closed + late_slots:
            value, estimated = (stored[closed], False) if closed in stored else (last, True)
            out.append(Closed(closed, value, estimated))
            last = value
            closed += 1
    return out, lates


@given(case=arrivals())
def test_ac_011_09_slots_close_once_and_in_order(case: tuple[int, list[Reading]]) -> None:
    late_slots, records = case
    trace = fold(late_slots, records)
    ts = [c.t for c in trace.closed]
    assert ts == list(range(FIRST, FIRST + len(ts)))
    assert trace.stream.closed == FIRST + len(ts) and all(t < END for t in ts)


@given(case=arrivals())
def test_ac_011_08_on_time_readings_are_used_unestimated(case: tuple[int, list[Reading]]) -> None:
    late_slots, records = case
    trace = fold(late_slots, records)
    for c in trace.closed:
        if c.t in trace.on_time:
            assert c == Closed(c.t, trace.on_time[c.t], False)
        if not c.estimated:
            assert c.kwh == trace.first[c.t]


@given(case=arrivals())
def test_ac_011_10_an_estimate_is_the_previous_closed_value(
    case: tuple[int, list[Reading]],
) -> None:
    late_slots, records = case
    trace = fold(late_slots, records)
    previous = 0.0
    for c in trace.closed:
        if c.estimated:
            assert c.kwh == previous
        previous = c.kwh
    assert trace.stream.estimated == sum(c.estimated for c in trace.closed)
    if trace.closed:
        assert trace.stream.last_kwh == trace.closed[-1].kwh


@given(case=arrivals())
def test_ac_011_11_late_readings_correct_a_bounded_later_step(
    case: tuple[int, list[Reading]],
) -> None:
    late_slots, records = case
    trace = fold(late_slots, records)
    estimated = {c.t for c in trace.closed if c.estimated}
    assert len({lt.t for lt in trace.lates}) == len(trace.lates) == trace.stream.late
    for lt in trace.lates:
        assert lt.t + late_slots + 1 <= lt.at_step <= lt.t + 2 * late_slots
        assert lt.t in estimated and lt.kwh == trace.first[lt.t]


@given(case=arrivals(), data=st.data())
def test_ac_011_13_a_copy_after_a_record_changes_no_output(
    case: tuple[int, list[Reading]], data: st.DataObject
) -> None:
    late_slots, records = case
    if not records:
        return
    i = data.draw(st.integers(min_value=0, max_value=len(records) - 1))
    j = data.draw(st.integers(min_value=i + 1, max_value=len(records)))
    with_copy = [*records[:j], records[i], *records[j:]]
    plain, copied = fold(late_slots, records), fold(late_slots, with_copy)
    assert (copied.closed, copied.lates) == (plain.closed, plain.lates)
    zero = dict.fromkeys(REFUSALS, 0)
    assert dataclasses.replace(copied.stream, **zero) == dataclasses.replace(plain.stream, **zero)
    added = [getattr(copied.stream, k) - getattr(plain.stream, k) for k in REFUSALS]
    assert sorted(added) == [0, 0, 0, 0, 1]


@given(case=arrivals())
def test_ac_011_09_pruning_changes_no_output(case: tuple[int, list[Reading]]) -> None:
    late_slots, records = case
    trace = fold(late_slots, records)
    assert (trace.closed, trace.lates) == oracle(late_slots, records)
    s = trace.stream
    in_window = [r for r in records if FIRST <= r.t < END]
    assert s.before_start == sum(r.t < FIRST for r in records)
    assert s.after_end == sum(r.t >= END for r in records)
    assert len(trace.first) + s.too_old + s.duplicate + s.conflict == len(in_window)


# ── the live clock (spec 012) ─────────────────────────────────────────────────────────────────────

COUNTERS = ("before_start", "after_end", "too_old", "duplicate", "conflict", "estimated", "late")
type Event = Reading | int  # a reading, or a clock close up to (excluding) that slot


@dataclass
class Expected:
    """What the spec's rules give for a sequence of readings and clock closes."""

    closed: list[Closed] = field(default_factory=list)
    lates: list[Late] = field(default_factory=list)
    stored: dict[int, tuple[str, float]] = field(default_factory=dict)  # slot -> (event id, kWh)
    counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(COUNTERS, 0))
    skipped: int = 0


def clock_oracle(first: int, end: int, late_slots: int, events: Sequence[Event]) -> Expected:
    """Plan §2 with a slot table that is never pruned. A reading is judged against the horizon
    before it, ``max(newest, closed - 1)``: at or below ``horizon - 2L`` too old; a slot already
    held is a duplicate (same event) or a conflict; otherwise it is stored, and if its slot already
    closed (on an estimate) it is a ``Late`` for step ``horizon + 1``. Slots close while the next
    one is held or the newest reading is ``L`` past it; a clock close also closes every slot below
    its step (the estimate: the value used for the slot before, 0 before any)."""
    exp = Expected()
    closed, newest, last = first, None, 0.0

    def close_next() -> None:
        nonlocal closed, last
        if closed in exp.stored:
            item = Closed(closed, exp.stored[closed][1], False)
        else:
            item = Closed(closed, last, True)
            exp.counts["estimated"] += 1
        exp.closed.append(item)
        last = item.kwh
        closed += 1

    for ev in events:
        if isinstance(ev, int):
            while closed < ev or closed in exp.stored:
                close_next()
            continue
        if ev.t < first:
            exp.counts["before_start"] += 1
            continue
        if ev.t >= end:
            exp.counts["after_end"] += 1
            continue
        horizon = closed - 1 if newest is None else max(newest, closed - 1)
        if ev.t <= horizon - 2 * late_slots:
            exp.counts["too_old"] += 1
            continue
        if ev.t in exp.stored:
            same = exp.stored[ev.t][0] == ev.event_id
            exp.counts["duplicate" if same else "conflict"] += 1
            continue
        exp.stored[ev.t] = (ev.event_id, ev.kwh)
        exp.skipped += ev.skipped
        if ev.t < closed:
            exp.lates.append(Late(ev.t, ev.kwh, horizon + 1))
            exp.counts["late"] += 1
        newest = ev.t if newest is None else max(newest, ev.t)
        while closed in exp.stored or newest >= closed + late_slots:
            close_next()
    return exp


@st.composite
def clocked(draw: st.DrawFn) -> tuple[int, list[Event]]:
    """``(L, events)``: the readings of ``arrivals()`` with up to six clock closes inserted at any
    positions, their steps non-decreasing in ``[FIRST - 2, END]`` (below the first slot, a no-op;
    the household caps the clock at ``end_step``)."""
    late_slots, records = draw(arrivals())
    n = draw(st.sampled_from((1, 2, 3, 4, 6, 0)))
    untils = sorted(draw(st.lists(st.integers(FIRST - 2, END), min_size=n, max_size=n)))
    at = sorted(draw(st.lists(st.integers(0, len(records)), min_size=n, max_size=n)))
    events: list[Event] = []
    k = 0
    for i in range(len(records) + 1):
        while k < n and at[k] == i:
            events.append(untils[k])
            k += 1
        if i < len(records):
            events.append(records[i])
    return late_slots, events


@dataclass
class ClockTrace:
    stream: MeterStream
    closed: list[Closed] = field(default_factory=list)
    lates: list[Late] = field(default_factory=list)
    stored: list[Reading] = field(default_factory=list)  # readings no counter refused
    refused: dict[str, int] = field(default_factory=lambda: dict.fromkeys(REFUSALS, 0))


def fold_clocked(late_slots: int, events: Sequence[Event]) -> ClockTrace:
    """Apply readings and clock closes in order, checking the per-call properties on the way."""
    trace = ClockTrace(MeterStream(closed=FIRST))
    for ev in events:
        before = trace.stream
        if isinstance(ev, int):
            s, out = ms.meter_close(before, ev, late_slots)
            assert s.closed >= ev  # every slot below the clock step is closed
            assert [x.t for x in out] == list(range(before.closed, s.closed))
            # a close reads nothing: only `closed`, `last_kwh`, the slot table and `estimated` move
            moved = {"closed": 0, "last_kwh": 0.0, "slots": (), "estimated": 0}
            assert dataclasses.replace(s, **moved) == dataclasses.replace(before, **moved)
            assert s.estimated - before.estimated == sum(x.estimated for x in out)
            if ev <= before.closed:
                assert (s, out) == (before, ())
            trace.closed += out
        else:
            s, out, lt = meter_apply(before, FIRST, END, late_slots, ev)
            added = [k for k in REFUSALS if getattr(s, k) != getattr(before, k)]
            assert len(added) <= 1 and all(getattr(s, k) == getattr(before, k) + 1 for k in added)
            if added:
                trace.refused[added[0]] += 1
                assert (out, lt) == ((), None) and s.late == before.late
            else:
                trace.stored.append(ev)
            if lt is not None:
                assert (lt.t, lt.kwh) == (ev.t, ev.kwh)
                assert lt.at_step == ms.horizon(before) + 1 >= before.closed
                assert lt.t < lt.at_step <= lt.t + 2 * late_slots
                trace.lates.append(lt)
            trace.closed += out
        h = ms.horizon(s)
        ts = [t for t, _, _ in s.slots]
        assert ts == sorted(set(ts)) and len(ts) <= 2 * late_slots
        assert all(h - 2 * late_slots < t <= h for t in ts)
        assert h >= ms.horizon(before)  # the horizon never falls
        trace.stream = s
    return trace


@given(case=arrivals())
def test_ac_012_01_without_a_clock_the_horizon_is_the_newest_reading(
    case: tuple[int, list[Reading]],
) -> None:
    late_slots, records = case
    s = MeterStream(closed=FIRST)
    for rec in records:
        s, _, _ = meter_apply(s, FIRST, END, late_slots, rec)
        assert ms.horizon(s) == (s.newest if s.newest is not None else FIRST - 1)
    assert fold(late_slots, records).closed == clock_oracle(FIRST, END, late_slots, records).closed


@given(case=clocked())
def test_ac_012_02_03_with_a_clock_the_stream_follows_the_rules(
    case: tuple[int, list[Event]],
) -> None:
    late_slots, events = case
    trace = fold_clocked(late_slots, events)
    exp = clock_oracle(FIRST, END, late_slots, events)
    assert trace.closed == exp.closed
    assert trace.lates == exp.lates
    s = trace.stream
    assert {k: getattr(s, k) for k in COUNTERS} == exp.counts
    assert s.skipped == exp.skipped


@given(case=clocked())
def test_ac_012_06_with_a_clock_every_accepted_reading_is_accounted_for_once(
    case: tuple[int, list[Event]],
) -> None:
    late_slots, events = case
    trace = fold_clocked(late_slots, events)
    s = trace.stream
    ts = [x.t for x in trace.closed]
    assert ts == list(range(FIRST, s.closed)) and s.closed <= END  # once each, in order
    in_window = [ev for ev in events if not isinstance(ev, int) and FIRST <= ev.t < END]
    refused = trace.refused
    assert len(trace.stored) + refused["too_old"] + refused["duplicate"] + refused["conflict"] == (
        len(in_window)
    )
    assert [refused[k] for k in REFUSALS] == [getattr(s, k) for k in REFUSALS]
    closed = {x.t: x for x in trace.closed}
    late_of = {lt.t: lt for lt in trace.lates}
    assert len(late_of) == len(trace.lates) == s.late  # at most one correction per slot
    assert len({r.t for r in trace.stored}) == len(trace.stored)  # a slot is stored once
    for rec in trace.stored:
        if rec.t >= s.closed:  # still waiting for its slot to close
            assert rec.t not in late_of
        elif closed[rec.t].estimated:  # closed before it came: one correction
            assert (late_of[rec.t].t, late_of[rec.t].kwh) == (rec.t, rec.kwh)
        else:  # its slot's value
            assert closed[rec.t].kwh == rec.kwh and rec.t not in late_of
    assert set(late_of) <= {r.t for r in trace.stored}
