"""AC-012-01, -02, -04 … -06: the household's join under the live clock (spec 012, plan §2).

The cases are those of ``test_interleaving.py``: per-stream sequences of meter readings (gaps,
lateness, duplicates, conflicts, unusable records) and of price and forecast updates as ``Tariff``
and ``Weather`` emit them (a record visible no earlier than the watermark it arrived behind, rising
watermarks), merged in any order that keeps each stream's own order, curves grouped into snapshots
at random cut points.

- Without clock events the clamp of ``on_curves`` and ``on_run`` moves nothing and counts nothing
  (AC-012-01, -04), for single updates and snapshots alike.
- With a fifth stream of non-decreasing clock steps (some before ``first_step``, some past
  ``end_step``) merged anywhere, no transition raises, and:
  - steps are released strictly in order from ``first_step``, none at or after ``end_step``;
  - after each clock event every step below ``min(clock, end_step)`` is released (AC-012-02), and
    the watermarks are the highest of the updates' and the clock's (capped at ``end_step``);
  - each meter side closes exactly what a never-pruned oracle of the rules gives for its readings
    and the clock events, and every accepted reading is accounted for once: its slot's value, a
    correction on a released step, one still waiting for its step, or ``after_end_late``; the rest
    are ``too_old``, ``duplicate`` or ``conflict`` (AC-012-03, -05, -06);
  - each record is visible from the later of its own visibility and the watermark before its
    update (counted when that moves it), and every step reads the price and forecast run a rule
    over *all* records, with those visibilities, gives at its start: no step reads a record
    visible after its start, in particular none that arrived after it was released (AC-012-04,
    -05).

Sizes as in spec 011: 200 examples, windows of 24 slots, ``L`` in 1 … 3.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass, field

from hypothesis import given, settings
from hypothesis import strategies as st

from ems.assembler import join
from ems.assembler.join import Join, MeterSide, Step, on_curves, on_reading, on_run, start
from ems.assembler.mapping import Reading
from ems.assembler.sparse_streams import Curve, Run
from ems.domain.settlement import Correction
from tests.property.test_interleaving import SPAN, Call, Case, cases, fold, merged, streams
from tests.property.test_meter_stream import Event, Expected, clock_oracle
from tests.unit.assembler.cases import price_of

SIDES = ("production", "consumption")


@st.composite
def clocked(draw: st.DrawFn) -> tuple[Case, list[Call]]:
    """A case and one merge of its four streams plus a clock stream: non-decreasing steps from four
    before the window to four past its end."""
    case = draw(cases())
    first = case.params.first_step
    cuts = draw(st.lists(st.booleans(), min_size=len(case.prices), max_size=len(case.prices)))
    per = streams(case, cuts)
    n = draw(st.sampled_from((1, 2, 3, 5, 8, 0)))
    ticks = sorted(draw(st.lists(st.integers(first - 4, first + SPAN + 4), min_size=n, max_size=n)))
    per["clock"] = [Call("clock", covered=c) for c in ticks]
    labels = draw(st.permutations([name for name, calls in per.items() for _ in calls]))
    pending = {name: iter(calls) for name, calls in per.items()}
    return case, [next(pending[name]) for name in labels]


@dataclass
class Folded:
    j: Join
    steps: list[Step] = field(default_factory=list)
    curves: list[Curve] = field(default_factory=list)  # as made visible: max(own, floor)
    runs: list[Run] = field(default_factory=list)
    clamped_price: int = 0
    clamped_forecast: int = 0
    price_floor: int = 0  # the join's price watermark by the rule: updates and clock
    forecast_floor: int = 0
    events: dict[str, list[Event]] = field(default_factory=lambda: {s: [] for s in SIDES})


def fold_clock(case: Case, calls: Sequence[Call]) -> Folded:
    """Apply ``calls`` in order, checking the per-call properties on the way."""
    p = case.params
    out = Folded(start(p))
    sides = SIDES if p.has_pv else ("consumption",)
    for call in calls:
        if call.stream == "prices":
            for c in call.curves:
                visible = max(c.visible_step, out.price_floor)
                out.clamped_price += visible > c.visible_step
                out.curves.append(dataclasses.replace(c, visible_step=visible))
            j, released = on_curves(p, out.j, call.covered, call.curves)
            out.price_floor = max(out.price_floor, call.covered)
        elif call.stream == "runs":
            assert call.run is not None
            visible = max(call.run.visible_step, out.forecast_floor)
            out.clamped_forecast += visible > call.run.visible_step
            out.runs.append(dataclasses.replace(call.run, visible_step=visible))
            j, released = on_run(p, out.j, call.covered, call.run)
            out.forecast_floor = max(out.forecast_floor, call.covered)
        elif call.stream == "clock":
            j, released = join.on_clock(p, out.j, call.covered)
            capped = min(call.covered, p.end_step)
            out.price_floor = max(out.price_floor, capped)
            out.forecast_floor = max(out.forecast_floor, capped)
            for side in sides:
                out.events[side].append(capped)
            assert j.next >= min(max(capped, p.first_step), p.end_step)
        else:
            j, released = on_reading(p, out.j, call.reading)
            if call.reading is not None and call.reading.stream in sides:
                out.events[call.reading.stream].append(call.reading)
        assert [s.t for s in released] == list(range(out.j.next, j.next))
        assert j.next <= p.end_step
        assert (j.price_covered, j.forecast_covered) == (out.price_floor, out.forecast_floor)
        out.j = j
        out.steps += released
    return out


@settings(max_examples=200)
@given(m=merged())
def test_ac_012_04_without_a_clock_the_clamp_moves_nothing(m: tuple[Case, list[Call]]) -> None:
    case, calls = m
    j, _ = fold(case.params, calls)
    assert (j.clamped_price, j.clamped_forecast) == (0, 0)
    assert set(j.curves) <= {c for _, c in case.prices}
    assert set(j.runs) <= {r for _, r in case.runs}
    folded = fold_clock(case, calls)
    assert (folded.clamped_price, folded.clamped_forecast) == (0, 0)


@settings(max_examples=200)
@given(m=clocked())
def test_ac_012_05_06_any_interleaving_with_a_clock_keeps_order_end_step_and_visibility(
    m: tuple[Case, list[Call]],
) -> None:
    case, calls = m
    p = case.params
    out = fold_clock(case, calls)
    j, steps = out.j, out.steps
    assert [s.t for s in steps] == list(range(p.first_step, j.next))
    assert all(s.t < p.end_step for s in steps)

    assert (j.clamped_price, j.clamped_forecast) == (out.clamped_price, out.clamped_forecast)
    for s in steps:
        assert (s.rce_pln_per_kwh, s.price_replaced) == price_of(out.curves, s.t)
        visible = [r for r in out.runs if r.visible_step <= s.t]
        assert s.forecast_issue_s == (
            max(visible, key=lambda r: r.seq).issue_s if visible else None
        )
    assert j.price_replaced == sum(s.price_replaced for s in steps)


def _side(j: Join, name: str) -> MeterSide:
    side = j.production if name == "production" else j.consumption
    assert side is not None
    return side


@settings(max_examples=200)
@given(m=clocked())
def test_ac_012_02_03_06_every_accepted_reading_is_accounted_for_once(
    m: tuple[Case, list[Call]],
) -> None:
    case, calls = m
    p = case.params
    out = fold_clock(case, calls)
    j, steps = out.j, out.steps
    sides = SIDES if p.has_pv else ("consumption",)
    expected: dict[str, Expected] = {
        name: clock_oracle(p.first_step, p.end_step, p.late_slots, out.events[name])
        for name in sides
    }

    # the release rule over what the clock and the streams closed
    limits = [p.end_step - p.first_step, out.price_floor - p.first_step]
    limits += [out.forecast_floor - p.first_step]
    limits += [len(expected[name].closed) for name in sides]
    assert len(steps) == max(0, min(limits))

    dropped = 0
    for name in sides:
        exp, side = expected[name], _side(j, name)
        stream = side.stream
        counters = ("too_old", "duplicate", "conflict", "late", "estimated")
        assert {k: getattr(stream, k) for k in counters} == {k: exp.counts[k] for k in counters}
        got = [
            (s.pv_kwh, s.pv_estimated) if name == "production" else (s.load_kwh, s.load_estimated)
            for s in steps
        ]  # each released step carries its slot's closed value
        assert got == [(c.kwh, c.estimated) for c in exp.closed[: len(steps)]]
        applied = [
            Correction(name, lt.t, lt.kwh) for s in steps for lt in exp.lates if lt.at_step == s.t
        ]
        assert [c for s in steps for c in s.corrections if c.stream == name] == applied
        waiting = tuple(lt for lt in exp.lates if j.next <= lt.at_step < p.end_step)
        assert side.late == waiting
        after_end = sum(lt.at_step >= p.end_step for lt in exp.lates)
        dropped += after_end
        # AC-012-06: each correction once, and the stream's own records add up
        assert len(applied) + len(waiting) + after_end == stream.late
        in_window = [
            r
            for r in out.events[name]
            if isinstance(r, Reading) and p.first_step <= r.t < p.end_step
        ]
        assert len(exp.stored) + stream.too_old + stream.duplicate + stream.conflict == len(
            in_window
        )
    assert j.after_end_late == dropped
    for s in steps:  # production's corrections first
        names = [c.stream for c in s.corrections]
        assert names == sorted(names, key=lambda n: n != "production")
    if not p.has_pv:
        assert j.production is None and all(
            (s.pv_kwh, s.pv_estimated) == (0.0, False) for s in steps
        )
