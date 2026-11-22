"""AC-011-17, -18, -21, -22: a household's steps depend on its streams' contents, not on how they
interleave.

Per-stream sequences are drawn (production and consumption readings with gaps, lateness, duplicates,
conflicts and unusable records; price updates ``(covered, Curve)`` and forecast updates
``(covered, Run)`` as ``Tariff`` and ``Weather`` emit them: a record is visible no earlier than the
watermark it arrived behind, and watermarks only rise). Then any merge that keeps each stream's own
order, and any grouping of consecutive curves into one ``on_curves`` call (the snapshot a late
subscriber gets), must:

- release the steps and end in the ``Join`` of folding the streams one after another (prices,
  forecasts, production, consumption, as ``offline.fold`` does);
- after every prefix, equal the stream-by-stream fold of the stream prefixes it consumed, so a
  prefix releases a prefix of the steps;
- release exactly the steps every needed meter side has closed and both watermarks cover, with the
  closed values, the corrections of each side (production first), and the price and forecast run a
  rule over *all* records of the streams gives at each step's start (so no later record is read);
- never raise.

Sizes (plan, Plan decisions): 200 examples, windows of 24 slots, ``L`` in 1 … 3, windows on the two
DST days among them.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from ems.assembler.join import Join, Step, on_curves, on_reading, on_run, start
from ems.assembler.mapping import Reading
from ems.assembler.meter_stream import Closed, Late, MeterStream, meter_apply
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import Curve, Run
from ems.domain.settlement import Correction
from tests.unit.assembler.cases import (
    NO_PV_SITE,
    PV_SITE,
    day_curve,
    local_day,
    price_of,
    run_params,
    step_at,
)

SPAN = 24
ANCHORS = (
    step_at("2026-10-20T00:00"),
    step_at("2026-10-24T20:00"),  # into the 100-slot day
    step_at("2026-10-25T00:00"),  # local 02:00 CEST: the repeated hour
    step_at("2026-03-29T00:00"),  # the 92-slot day
    step_at("2026-03-30T00:00"),  # after it: 02:xx is two days back
)
# Updates per sparse stream; none is possible but rare (integers would draw 0 often).
UPDATE_COUNTS = st.sampled_from((1, 2, 3, 4, 5, 0))


@dataclass(frozen=True)
class Call:
    """One transition of the join: a reading, a price delivery (one curve, or a snapshot of
    several) with its watermark, or a forecast run with its watermark."""

    stream: str  # "production", "consumption", "prices" or "runs"
    reading: Reading | None = None
    covered: int = 0
    curves: tuple[Curve, ...] = ()
    run: Run | None = None


@dataclass(frozen=True)
class Case:
    params: RunParams
    production: tuple[Reading | None, ...]
    consumption: tuple[Reading | None, ...]
    prices: tuple[tuple[int, Curve], ...]
    runs: tuple[tuple[int, Run], ...]


@st.composite
def meter_records(
    draw: st.DrawFn, stream: str, first: int, late_slots: int
) -> tuple[Reading | None, ...]:
    """Most slots of ``[first - 2, first + SPAN + 2)``, each delayed by up to ``3L`` positions or
    shuffled, with repeats (duplicates and conflicts) and unusable records (None)."""
    ts = range(first - 2, first + SPAN + 2)
    missing = draw(st.sets(st.sampled_from(ts), max_size=6))
    kwh = st.integers(min_value=0, max_value=40).map(lambda n: n / 8)
    keyed: list[tuple[int, int, Reading | None]] = []
    for t in ts:
        if t in missing:
            continue
        delay = draw(st.one_of(st.just(0), st.integers(min_value=0, max_value=3 * late_slots)))
        keyed.append((t + delay, len(keyed), Reading(stream, t, f"{stream}-{t}", draw(kwh), 0)))
    for _ in range(draw(st.integers(min_value=0, max_value=3))):
        if not keyed:
            break
        key, _, rec = draw(st.sampled_from(keyed))
        if rec is not None and draw(st.booleans()):
            rec = Reading(stream, rec.t, rec.event_id + "-b", draw(kwh), 1)
        keyed.append(
            (key + draw(st.integers(min_value=0, max_value=2 * late_slots)), len(keyed), rec)
        )
    for _ in range(draw(st.integers(min_value=0, max_value=2))):
        keyed.append((draw(st.integers(first - 2, first + SPAN + 2)), len(keyed), None))
    if draw(st.integers(min_value=0, max_value=4)) == 0:
        ordered = draw(st.permutations(keyed))
    else:
        ordered = sorted(keyed, key=lambda k: (k[0], k[1]))
    return tuple(rec for _, _, rec in ordered)


@st.composite
def price_updates(draw: st.DrawFn, first: int) -> tuple[tuple[int, Curve], ...]:
    """Curves of the local days around the window, each visible no earlier than the watermark
    before it, with a rising watermark; equal publication seconds are common (ties)."""
    d0, d1 = local_day(first), local_day(first + SPAN - 1)
    days = [d0 + timedelta(days=k) for k in range(-3, (d1 - d0).days + 2)]
    own = [d0 + timedelta(days=k) for k in range((d1 - d0).days + 1)]
    lo, hi = first - 8, first + SPAN + 8
    covered, out = 0, []
    for seq in range(1, draw(UPDATE_COUNTS) + 1):
        visible = draw(_visible(covered, lo, hi))
        covered = max(covered, draw(_watermark(lo, hi)))
        issued = draw(st.integers(min_value=0, max_value=3))
        day = draw(st.one_of(st.sampled_from(own), st.sampled_from(days)))
        out.append((covered, day_curve(seq, day, visible, issued_s=issued)))
    return tuple(out)


@st.composite
def run_updates(draw: st.DrawFn, first: int) -> tuple[tuple[int, Run], ...]:
    """Runs newer by issue time, each visible no earlier than the watermark before it; the
    watermark after a run is at or past its visibility (availability + cadence >= usable)."""
    lo, hi = first - 8, first + SPAN + 8
    covered, out = 0, []
    for seq in range(1, draw(UPDATE_COUNTS) + 1):
        visible = draw(_visible(covered, lo, hi))
        covered = max(covered, visible, draw(_watermark(lo, hi)))
        out.append((covered, Run(seq, 10_800 * seq, visible)))
    return tuple(out)


def _visible(covered: int, lo: int, hi: int) -> st.SearchStrategy[int]:
    """A visibility step no earlier than the watermark at arrival, often exactly at it."""
    floor = max(covered, lo)
    return st.one_of(st.just(floor), st.integers(min_value=floor, max_value=max(covered, hi)))


def _watermark(lo: int, hi: int) -> st.SearchStrategy[int]:
    """A watermark candidate, often past the whole window."""
    return st.one_of(st.just(hi), st.integers(min_value=lo, max_value=hi))


@st.composite
def cases(draw: st.DrawFn) -> Case:
    first = draw(st.sampled_from(ANCHORS))
    late_slots = draw(st.integers(min_value=1, max_value=3))
    site = PV_SITE if draw(st.booleans()) else NO_PV_SITE
    params = run_params(site, first, first + SPAN, late_slots)
    return Case(
        params,
        draw(meter_records("production", first, late_slots)),
        draw(meter_records("consumption", first, late_slots)),
        draw(price_updates(first)),
        draw(run_updates(first)),
    )


def deliveries(updates: Sequence[tuple[int, Curve]], cuts: Sequence[bool]) -> list[Call]:
    """Consecutive curves grouped into ``on_curves`` calls; a cut after update i ends a group."""
    calls: list[Call] = []
    group: list[Curve] = []
    for i, (covered, curve) in enumerate(updates):
        group.append(curve)
        if i == len(updates) - 1 or cuts[i]:
            calls.append(Call("prices", covered=covered, curves=tuple(group)))
            group = []
    return calls


def streams(case: Case, cuts: Sequence[bool]) -> dict[str, list[Call]]:
    return {
        "prices": deliveries(case.prices, cuts),
        "runs": [Call("runs", covered=covered, run=run) for covered, run in case.runs],
        "production": [Call("production", reading=r) for r in case.production],
        "consumption": [Call("consumption", reading=r) for r in case.consumption],
    }


def fold(params: RunParams, calls: Sequence[Call]) -> tuple[Join, list[Step]]:
    j = start(params)
    steps: list[Step] = []
    for call in calls:
        if call.stream == "prices":
            j, out = on_curves(params, j, call.covered, call.curves)
        elif call.stream == "runs":
            assert call.run is not None
            j, out = on_run(params, j, call.covered, call.run)
        else:
            j, out = on_reading(params, j, call.reading)
        steps += out
    return j, steps


def one_by_one(case: Case, used: dict[str, int] | None = None) -> tuple[Join, list[Step]]:
    """The stream-by-stream fold (each curve its own call, prices first, as ``offline.fold``) of
    the first ``used[name]`` records of each stream (price updates counted one by one)."""
    used = used or {}
    one = streams(case, [True] * len(case.prices))
    calls = [
        call
        for name in ("prices", "runs", "production", "consumption")
        for call in one[name][: used.get(name, len(one[name]))]
    ]
    return fold(case.params, calls)


@st.composite
def merged(draw: st.DrawFn) -> tuple[Case, list[Call]]:
    """A case and one merge of its streams that keeps each stream's order, with the price updates
    grouped into deliveries at random cut points."""
    case = draw(cases())
    cuts = draw(st.lists(st.booleans(), min_size=len(case.prices), max_size=len(case.prices)))
    per = streams(case, cuts)
    labels = draw(st.permutations([name for name, calls in per.items() for _ in calls]))
    pending = {name: iter(calls) for name, calls in per.items()}
    return case, [next(pending[name]) for name in labels]


def consumed(calls: Sequence[Call]) -> dict[str, int]:
    """How many records of each stream ``calls`` hold (price updates, not deliveries)."""
    used = dict.fromkeys(("prices", "runs", "production", "consumption"), 0)
    for call in calls:
        used[call.stream] += len(call.curves) if call.stream == "prices" else 1
    return used


def _sides(case: Case) -> dict[str, tuple[list[Closed], list[Late]]]:
    p = case.params
    out: dict[str, tuple[list[Closed], list[Late]]] = {}
    for name, records in (("production", case.production), ("consumption", case.consumption)):
        s = MeterStream(closed=p.first_step)
        closed: list[Closed] = []
        lates: list[Late] = []
        for r in records:
            if r is None:
                continue
            s, c, lt = meter_apply(s, p.first_step, p.end_step, p.late_slots, r)
            closed += c
            lates += [lt] if lt is not None else []
        out[name] = (closed, lates)
    return out


@settings(max_examples=200)
@given(m=merged())
def test_ac_011_21_any_interleaving_gives_the_stream_by_stream_result(
    m: tuple[Case, list[Call]],
) -> None:
    case, calls = m
    assert fold(case.params, calls) == one_by_one(case)


@settings(max_examples=200)
@given(m=merged(), data=st.data())
def test_ac_011_21_every_prefix_releases_a_prefix(
    m: tuple[Case, list[Call]], data: st.DataObject
) -> None:
    case, calls = m
    _, steps = fold(case.params, calls)
    cuts = data.draw(st.lists(st.integers(min_value=0, max_value=len(calls)), max_size=3))
    for k in sorted(set(cuts)):
        j_k, steps_k = fold(case.params, calls[:k])
        assert (j_k, steps_k) == one_by_one(case, consumed(calls[:k]))
        assert steps_k == steps[: len(steps_k)]


@settings(max_examples=200)
@given(m=merged())
def test_ac_011_17_18_22_steps_are_released_on_complete_data_and_read_no_later_record(
    m: tuple[Case, list[Call]],
) -> None:
    case, calls = m
    p = case.params
    j, steps = fold(p, calls)
    sides = _sides(case)
    cons_closed, cons_lates = sides["consumption"]
    prod_closed, prod_lates = sides["production"]
    price_cov = max((c for c, _ in case.prices), default=0)
    run_cov = max((c for c, _ in case.runs), default=0)
    limits = [len(cons_closed), price_cov - p.first_step, run_cov - p.first_step]
    if p.has_pv:
        limits.append(len(prod_closed))
    assert [s.t for s in steps] == list(range(p.first_step, p.first_step + max(0, min(limits))))
    assert j.next == p.first_step + len(steps)

    curves = [c for _, c in case.prices]
    runs = [r for _, r in case.runs]
    for i, s in enumerate(steps):
        load = cons_closed[i]
        assert (s.load_kwh, s.load_estimated) == (load.kwh, load.estimated)
        if p.has_pv:
            assert (s.pv_kwh, s.pv_estimated) == (prod_closed[i].kwh, prod_closed[i].estimated)
        else:
            assert (s.pv_kwh, s.pv_estimated) == (0.0, False)
        expected = [
            Correction(name, lt.t, lt.kwh)
            for name, lates in (("production", prod_lates), ("consumption", cons_lates))
            if p.has_pv or name == "consumption"
            for lt in lates
            if lt.at_step == s.t
        ]
        assert s.corrections == tuple(expected)
        assert (s.rce_pln_per_kwh, s.price_replaced) == price_of(curves, s.t)
        visible = [r for r in runs if r.visible_step <= s.t]
        assert s.forecast_issue_s == (
            max(visible, key=lambda r: r.seq).issue_s if visible else None
        )
    assert j.price_replaced == sum(s.price_replaced for s in steps)
    assert j.unusable == sum(r is None for r in (*case.production, *case.consumption))
    if not p.has_pv:
        assert j.unneeded == sum(r is not None for r in case.production)
