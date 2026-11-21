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
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field

from hypothesis import given
from hypothesis import strategies as st

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
