"""AC-011-17 … -22: the join of a household's streams into ordered steps.

- AC-011-17: step ``q`` is released only when every needed meter side has closed ``q``
  (consumption, and production for a site with PV) and both watermarks lie past ``q``, in order.
- AC-011-18: its content: closed values and flags (PV 0, unflagged, without PV), the price, the
  issue time of the newest run visible at ``q``, and the corrections attached to ``q``, production
  first.
- AC-011-19, -20: no visible curve of ``q``'s day: the same local quarter-hour of the nearest
  earlier day (two days at most) whose visible curve has it, else 0, flagged and counted; slot
  ``q - t0`` on the 92- and 100-slot days; local clock matching across both DST days.
- AC-011-22: no step reads a curve or run that becomes visible after its start.
- Pruning: curves of days more than two days before ``next``'s day, and runs older than the last
  one visible at ``next``, are dropped without changing a step.

Spec 012 (plan §2), the live clock, in the last section:
- AC-012-01, -05: a correction that could only attach at or after ``end_step`` is dropped and
  counted in ``Join.after_end_late`` (a subset of the stream's ``late``); spec 011 kept it
  unapplied;
- AC-012-02: ``on_clock`` closes every needed meter side below ``min(clock, end_step)``, raises both
  watermarks to at least that step and releases by the usual rule; a clock before the window
  releases nothing;
- AC-012-03: a reading for a slot the clock closed is a correction for the step after the horizon;
- AC-012-04: a curve or run that arrives behind the join's watermark is made visible from the
  watermark (the one before the update) and counted; at or after it, nothing moves.

Prices in these tests are ``seq * 1000 + slot`` (``cases.day_curve``): each value names its curve.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from datetime import date

import pytest

from ems.assembler import join
from ems.assembler.join import Join, MeterSide, Step, on_curves, on_reading, on_run, start
from ems.assembler.mapping import Reading
from ems.assembler.meter_stream import Late, MeterStream
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import Curve, Run
from ems.domain.settlement import Correction
from tests.unit.assembler.cases import (
    NO_PV_SITE,
    PV_SITE,
    day_curve,
    day_slots,
    local_clock,
    price_of,
    run_params,
    seconds_at,
    step_at,
)

FIRST = step_at("2026-10-20T00:00")
D20 = date(2026, 10, 20)
T0_D20, _ = day_slots(D20)  # 2026-10-19T22:00Z
S00, S03, S06 = (seconds_at(f"2026-10-20T{h}:00") for h in ("00", "03", "06"))
FAR = 10**9  # a watermark past every step of these tests


def pv(t: int) -> float:
    return (t % 1000) / 16


def load(t: int) -> float:
    return 0.25 + (t % 1000) / 32


class Feed:
    """A join and the steps it has released so far."""

    def __init__(self, params: RunParams) -> None:
        self.p = params
        self.j = start(params)
        self.steps: list[Step] = []

    def _take(self, out: tuple[Join, tuple[Step, ...]]) -> tuple[Step, ...]:
        self.j, released = out
        self.steps += released
        return released

    def reading(self, stream: str, t: int, kwh: float | None = None) -> tuple[Step, ...]:
        value = kwh if kwh is not None else (pv(t) if stream == "production" else load(t))
        rec = Reading(stream, t, f"{stream[0]}{t}", value, 0)
        return self._take(on_reading(self.p, self.j, rec))

    def meters(self, ts: Iterable[int]) -> tuple[Step, ...]:
        out: list[Step] = []
        for t in ts:
            if self.p.has_pv:
                out += self.reading("production", t)
            out += self.reading("consumption", t)
        return tuple(out)

    def curves(self, covered: int, *curves: Curve) -> tuple[Step, ...]:
        return self._take(on_curves(self.p, self.j, covered, curves))

    def run(self, covered: int, run: Run) -> tuple[Step, ...]:
        return self._take(on_run(self.p, self.j, covered, run))

    def clock(self, clock_step: int) -> tuple[Step, ...]:
        return self._take(join.on_clock(self.p, self.j, clock_step))


def params(
    location: str = PV_SITE, first: int = FIRST, end: int = FIRST + 96, late_slots: int = 2
) -> RunParams:
    return run_params(location, first, end, late_slots)


def ready(location: str = PV_SITE, late_slots: int = 2) -> Feed:
    """A feed whose price and forecast watermarks already cover the day of 2026-10-20."""
    f = Feed(params(location, late_slots=late_slots))
    f.curves(FAR, day_curve(1, D20, visible_step=FIRST - 40))
    f.run(FAR, Run(1, S00, FIRST - 4))
    return f


# ── start and release ─────────────────────────────────────────────────────────────────────────────


def test_start() -> None:
    side = MeterSide(MeterStream(closed=FIRST))
    assert start(params(PV_SITE)) == Join(next=FIRST, consumption=side, production=side)
    assert start(params(NO_PV_SITE)) == Join(next=FIRST, consumption=side, production=None)


def test_ac_011_17_a_step_waits_for_every_input() -> None:
    f = Feed(params())
    assert f.reading("consumption", FIRST) == ()
    assert f.reading("production", FIRST) == ()  # no price, no forecast
    assert f.curves(step_at("2026-10-20T11:00"), day_curve(1, D20, FIRST - 40)) == ()
    released = f.run(step_at("2026-10-20T05:00"), Run(1, S00, step_at("2026-10-20T02:00")))
    assert [s.t for s in released] == [FIRST]
    assert f.reading("production", FIRST + 1) == ()  # consumption missing
    assert [s.t for s in f.reading("consumption", FIRST + 1)] == [FIRST + 1]
    assert f.j.next == FIRST + 2


def test_ac_011_17_watermarks_must_lie_past_the_step() -> None:
    f = Feed(params())
    f.meters(range(FIRST, FIRST + 4))
    assert f.steps == []
    assert f.curves(FIRST + 2) == ()  # an empty snapshot: the watermark alone
    assert [s.t for s in f.run(FIRST + 1, Run(1, S00, FIRST - 20))] == [FIRST]
    assert [s.t for s in f.run(FIRST + 10, Run(2, S03, FIRST + 1))] == [FIRST + 1]
    assert [s.t for s in f.curves(FIRST + 44, day_curve(1, D20, FIRST - 40))] == [
        FIRST + 2,
        FIRST + 3,
    ]
    assert [s.forecast_issue_s for s in f.steps] == [S00, S03, S03, S03]
    assert (f.j.price_covered, f.j.forecast_covered) == (FIRST + 44, FIRST + 10)


def test_ac_011_17_steps_are_released_in_order_from_the_first() -> None:
    f = ready(late_slots=8)
    f.meters([FIRST - 4, FIRST + 1, FIRST + 2])
    assert f.steps == []
    assert f.j.consumption.stream.before_start == 1
    f.meters([FIRST])
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1, FIRST + 2]


def test_ac_011_17_a_meter_side_waits_for_its_own_closing() -> None:
    f = ready()
    f.meters([FIRST])
    f.reading("consumption", FIRST + 2)  # consumption's slot 1 waits: newest 2 < 1 + L
    f.reading("production", FIRST + 1)
    assert f.j.next == FIRST + 1  # consumption has not closed slot 1
    f.reading("consumption", FIRST + 1)  # consumption closes 1 and 2
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1]  # production has not closed slot 2
    f.reading("production", FIRST + 2)
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1, FIRST + 2]


# ── step content ──────────────────────────────────────────────────────────────────────────────────


def test_ac_011_18_step_content() -> None:
    f = Feed(params())
    f.curves(FAR, day_curve(1, D20, visible_step=FIRST - 40))
    f.run(FIRST + 20, Run(1, S00, FIRST + 8))
    f.meters(range(FIRST, FIRST + 10))
    assert [s.t for s in f.steps] == list(range(FIRST, FIRST + 10))
    first, eighth = f.steps[0], f.steps[8]
    assert first == Step(
        t=FIRST,
        pv_kwh=pv(FIRST),
        load_kwh=load(FIRST),
        rce_pln_per_kwh=1000.0 + (FIRST - T0_D20),
        pv_estimated=False,
        load_estimated=False,
        price_replaced=False,
        forecast_issue_s=None,  # the first run is usable at 02:00
        corrections=(),
    )
    assert eighth == dataclasses.replace(
        first,
        t=FIRST + 8,
        pv_kwh=pv(FIRST + 8),
        load_kwh=load(FIRST + 8),
        rce_pln_per_kwh=1000.0 + (FIRST + 8 - T0_D20),
        forecast_issue_s=S00,
    )
    assert first.schema_version == "1.0.0"
    assert [s.forecast_issue_s is None for s in f.steps] == [True] * 8 + [False] * 2


def test_ac_011_18_estimated_flags() -> None:
    f = ready()
    f.meters([FIRST, FIRST + 1])
    f.reading("production", FIRST + 3)
    f.reading("production", FIRST + 4)  # production's slot 2 closes on slot 1's value
    f.reading("consumption", FIRST + 2)
    f.reading("consumption", FIRST + 3)
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1, FIRST + 2, FIRST + 3]
    two = f.steps[2]
    assert (two.t, two.pv_kwh, two.pv_estimated) == (FIRST + 2, pv(FIRST + 1), True)
    assert (two.load_kwh, two.load_estimated) == (load(FIRST + 2), False)
    assert not f.steps[3].pv_estimated and f.steps[3].pv_kwh == pv(FIRST + 3)
    assert f.j.production is not None and f.j.production.stream.estimated == 1


def test_ac_011_18_a_site_without_pv() -> None:
    f = ready(NO_PV_SITE)
    assert f.j.production is None
    released = f.meters(range(FIRST, FIRST + 3))
    assert [(s.t, s.pv_kwh, s.pv_estimated) for s in released] == [
        (FIRST, 0.0, False),
        (FIRST + 1, 0.0, False),
        (FIRST + 2, 0.0, False),
    ]
    assert [s.load_kwh for s in released] == [load(FIRST), load(FIRST + 1), load(FIRST + 2)]
    before = f.j
    assert f.reading("production", FIRST + 3, 1.0) == ()
    assert f.j == dataclasses.replace(before, unneeded=1)


def test_unusable_records_are_counted() -> None:
    f = ready()
    j, steps = on_reading(f.p, f.j, None)
    assert steps == () and j == dataclasses.replace(f.j, unusable=1)


# ── corrections ───────────────────────────────────────────────────────────────────────────────────


def test_ac_011_18_corrections_production_first_and_removed() -> None:
    f = ready()
    for stream in ("consumption", "production"):  # consumption's late reading arrives first
        for t in (FIRST, FIRST + 2, FIRST + 3):
            f.reading(stream, t)
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1, FIRST + 2, FIRST + 3]
    assert f.steps[1].pv_estimated and f.steps[1].load_estimated
    f.reading("consumption", FIRST + 1, 7.0)
    f.reading("production", FIRST + 1, 3.0)
    assert f.j.consumption.late == (Late(FIRST + 1, 7.0, FIRST + 4),)
    released = f.meters([FIRST + 4])
    assert [s.t for s in released] == [FIRST + 4]
    assert released[0].corrections == (
        Correction("production", FIRST + 1, 3.0),
        Correction("consumption", FIRST + 1, 7.0),
    )
    assert all(s.corrections == () for s in f.steps[:4])
    assert f.j.production is not None
    assert f.j.consumption.late == () and f.j.production.late == ()
    f.meters([FIRST + 5])
    assert f.steps[-1].corrections == ()


def test_ac_012_01_a_correction_due_at_end_step_is_dropped_and_counted() -> None:
    """Spec 011 kept this ``Late`` unapplied; spec 012 drops it and counts it in the join's
    ``after_end_late``, while each stream still counts it in ``late`` (AC-012-01, -05)."""
    end = FIRST + 4
    f = Feed(params(end=end))
    f.curves(FAR, day_curve(1, D20, FIRST - 40))
    f.run(FAR, Run(1, S00, FIRST - 4))
    f.meters([FIRST, FIRST + 2, FIRST + 3])
    f.meters([FIRST + 1])  # late on both sides, for step FIRST + 4 = end
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1, FIRST + 2, FIRST + 3]
    assert all(s.corrections == () for s in f.steps)
    assert f.j.production is not None
    assert f.j.consumption.late == () and f.j.production.late == ()
    assert f.j.after_end_late == 2
    assert (f.j.consumption.stream.late, f.j.production.stream.late) == (1, 1)


def test_ac_012_05_a_correction_due_at_the_last_step_is_applied() -> None:
    """The boundary below: due at ``end_step - 1``, it rides on the last step."""
    end = FIRST + 5
    f = Feed(params(end=end))
    f.curves(FAR, day_curve(1, D20, FIRST - 40))
    f.run(FAR, Run(1, S00, FIRST - 4))
    f.meters([FIRST, FIRST + 2, FIRST + 3])
    f.meters([FIRST + 1])  # late on both sides, for step FIRST + 4 = end - 1
    released = f.meters([FIRST + 4])
    assert [s.t for s in released] == [FIRST + 4]
    assert released[0].corrections == (
        Correction("production", FIRST + 1, pv(FIRST + 1)),
        Correction("consumption", FIRST + 1, load(FIRST + 1)),
    )
    assert f.j.after_end_late == 0


# ── prices ────────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("day", "n"), [(date(2026, 3, 29), 92), (date(2026, 10, 25), 100)])
def test_ac_011_20_dst_days_read_slot_q_minus_t0(day: date, n: int) -> None:
    t0, slots = day_slots(day)
    assert slots == n
    f = Feed(params(first=t0, end=t0 + n, late_slots=1))
    f.curves(FAR, day_curve(1, day, visible_step=t0 - 50))
    f.run(FAR, Run(1, 0, t0 - 10))
    f.meters(range(t0, t0 + n))
    assert [s.t for s in f.steps] == list(range(t0, t0 + n))
    assert [s.rce_pln_per_kwh for s in f.steps] == [1000.0 + i for i in range(n)]
    assert not any(s.price_replaced for s in f.steps) and f.j.price_replaced == 0


def test_ac_011_18_the_latest_publication_wins_ties_to_the_first_accepted() -> None:
    f = Feed(params())
    early = day_curve(1, D20, visible_step=FIRST - 40, issued_s=100)
    same = day_curve(2, D20, visible_step=FIRST - 30, issued_s=100)  # a tie: seq 1 wins
    newer = day_curve(3, D20, visible_step=FIRST + 2, issued_s=200)  # visible from step 2
    f.curves(FAR, early, same, newer)
    f.run(FAR, Run(1, S00, FIRST - 4))
    f.meters(range(FIRST, FIRST + 4))
    slot = FIRST - T0_D20
    assert [s.rce_pln_per_kwh for s in f.steps] == [
        1000.0 + slot,
        1000.0 + slot + 1,
        3000.0 + slot + 2,
        3000.0 + slot + 3,
    ]


def _replacement_run(
    first: int, n: int, curves: tuple[Curve, ...]
) -> tuple[list[Step], Callable[[int], tuple[float, bool]], Join]:
    f = Feed(params(first=first, end=first + n, late_slots=1))
    f.curves(FAR, *curves)
    f.run(FAR, Run(1, 0, first - 10))
    f.meters(range(first, first + n))
    assert [s.t for s in f.steps] == list(range(first, first + n))
    return f.steps, lambda q: price_of(curves, q), f.j


def test_ac_011_19_a_day_with_a_late_curve_uses_the_previous_day() -> None:
    """2026-10-21 published on the day at 08:05 local: until then, 2026-10-20's quarter-hours."""
    d21 = date(2026, 10, 21)
    first, _ = day_slots(d21)
    visible = step_at("2026-10-21T06:15")
    curves = (day_curve(1, D20, FIRST - 40), day_curve(2, d21, visible))
    steps, _, j = _replacement_run(first, 40, curves)
    assert visible - first == 33
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in steps] == [
        (1000.0 + i, True) for i in range(33)
    ] + [(2000.0 + i, False) for i in range(33, 40)]
    assert j.price_replaced == 33


def test_ac_011_20_replacement_after_the_100_slot_day_takes_the_first_copy() -> None:
    d25, d26 = date(2026, 10, 25), date(2026, 10, 26)
    first, _ = day_slots(d26)
    curves = (
        day_curve(1, d25, visible_step=first - 100),
        day_curve(2, d26, visible_step=first + 100),  # arrives with the watermark, too late
        day_curve(3, date(2026, 10, 24), visible_step=first - 200),  # older: never used
    )
    steps, expect, j = _replacement_run(first, 24, curves)
    # 2026-10-26 local 00:00 … 05:45; 2026-10-25 has 02:00 … 02:45 twice (slots 8 … 15)
    assert [local_clock(s.t) for s in steps[8:13]] == [(2, 0), (2, 15), (2, 30), (2, 45), (3, 0)]
    assert [s.rce_pln_per_kwh for s in steps[6:14]] == [
        1006.0,
        1007.0,
        1008.0,  # 02:00, the first copy (CEST)
        1009.0,
        1010.0,
        1011.0,
        1016.0,  # 03:00 CET
        1017.0,
    ]
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in steps] == [expect(s.t) for s in steps]
    assert all(s.price_replaced for s in steps) and j.price_replaced == 24


def test_ac_011_20_replacement_on_the_100_slot_day_matches_both_copies_to_one() -> None:
    d24, d25 = date(2026, 10, 24), date(2026, 10, 25)
    first, _ = day_slots(d25)
    curves = (day_curve(1, d24, visible_step=first - 100), day_curve(2, d25, first + 100))
    steps, expect, _ = _replacement_run(first, 24, curves)
    assert [local_clock(s.t) for s in steps[8:16]] == [
        (2, 0),
        (2, 15),
        (2, 30),
        (2, 45),
        (2, 0),
        (2, 15),
        (2, 30),
        (2, 45),
    ]
    assert [s.rce_pln_per_kwh for s in steps[8:17]] == [
        1008.0,
        1009.0,
        1010.0,
        1011.0,
        1008.0,
        1009.0,
        1010.0,
        1011.0,
        1012.0,  # 03:00
    ]
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in steps] == [expect(s.t) for s in steps]


def test_ac_011_20_replacement_after_the_92_slot_day_goes_two_days_back_for_0200() -> None:
    d28, d29, d30 = date(2026, 3, 28), date(2026, 3, 29), date(2026, 3, 30)
    first, _ = day_slots(d30)
    curves = (
        day_curve(1, d29, visible_step=first - 100),  # no 02:xx
        day_curve(2, d28, visible_step=first - 200),
        day_curve(3, d30, visible_step=first + 100),
    )
    steps, expect, _ = _replacement_run(first, 16, curves)
    assert [s.rce_pln_per_kwh for s in steps] == [
        *(1000.0 + i for i in range(8)),  # 00:00 … 01:45 from the 29th
        *(2008.0 + i for i in range(4)),  # 02:00 … 02:45 from the 28th
        *(1008.0 + i for i in range(4)),  # 03:00 … 03:45: slots 8 … 11 of the 29th
    ]
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in steps] == [expect(s.t) for s in steps]


def test_ac_011_19_two_days_back_then_zero() -> None:
    d18, d19, d21 = date(2026, 10, 18), date(2026, 10, 19), date(2026, 10, 21)
    first, _ = day_slots(D20)
    first += 4  # 2026-10-19T23:00Z, an hour border
    # 20th: no curve; 19th: visible only from step 8; 18th: visible; 17th would be too far.
    curves = (
        day_curve(1, d19, visible_step=first + 8),
        day_curve(2, d18, visible_step=first - 300),
        day_curve(4, d21, visible_step=first + 300),
    )
    steps, expect, j = _replacement_run(first, 12, curves)
    assert [s.rce_pln_per_kwh for s in steps[:8]] == [2004.0 + i for i in range(8)]
    assert [s.rce_pln_per_kwh for s in steps[8:]] == [1012.0 + i for i in range(4)]
    assert all(s.price_replaced for s in steps)
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in steps] == [expect(s.t) for s in steps]
    only_old = (day_curve(5, date(2026, 10, 17), visible_step=first - 400), curves[2])
    steps, _, j = _replacement_run(first, 4, only_old)
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in steps] == [(0.0, True)] * 4
    assert j.price_replaced == 4


# ── forecasts ─────────────────────────────────────────────────────────────────────────────────────


def test_ac_011_14_a_missing_run_holds_the_steps_and_they_see_the_older_run() -> None:
    f = Feed(params(late_slots=1))
    f.curves(FAR, day_curve(1, D20, FIRST - 40))
    f.meters(range(FIRST, FIRST + 40))
    f.run(step_at("2026-10-20T05:00"), Run(1, S00, step_at("2026-10-20T02:00")))
    assert f.j.next == FIRST + 20  # waits at 05:00 for the 03:00 run, which never comes
    f.run(step_at("2026-10-20T11:00"), Run(2, S06, step_at("2026-10-20T08:00")))
    assert f.j.next == FIRST + 40
    issues = [s.forecast_issue_s for s in f.steps]
    assert issues == [None] * 8 + [S00] * 24 + [S06] * 8


def test_ac_011_22_a_run_is_never_read_before_it_is_visible() -> None:
    f = Feed(params(late_slots=1))
    f.curves(FAR, day_curve(1, D20, FIRST - 40))
    f.meters(range(FIRST, FIRST + 6))
    f.run(FIRST + 3, Run(1, S00, FIRST - 8))
    f.run(FAR, Run(2, S03, FIRST + 3))  # its visibility was clamped to the watermark
    assert [s.forecast_issue_s for s in f.steps] == [S00] * 3 + [S03] * 3


# ── pruning ───────────────────────────────────────────────────────────────────────────────────────


def test_pruning_keeps_two_days_of_curves_and_the_runs_still_needed() -> None:
    first = step_at("2026-10-20T20:00")  # local 22:00; 22:00Z starts the 21st
    f = Feed(params(first=first, end=first + 12, late_slots=1))
    days = [date(2026, 10, d) for d in (17, 18, 19, 20, 21)]
    f.curves(FAR, *(day_curve(i + 1, d, first - 400) for i, d in enumerate(days)))
    r1, r2, r3 = Run(1, 1, first - 30), Run(2, 2, first - 5), Run(3, 3, first + 2)
    # Each run arrives with the watermark its stream had after it: never above the next run's
    # visibility, as `forecast_apply` guarantees (spec 012: a run behind the join's watermark
    # would be clamped).
    for r, covered in ((r1, first - 5), (r2, first + 2), (r3, FAR)):
        f.run(covered, r)
    f.meters([first])  # next = first + 1, still the 20th
    assert [c.day for c in f.j.curves] == days[1:]
    assert f.j.runs == (r2, r3)
    f.meters(range(first + 1, first + 8))  # next = first + 8, the 21st
    assert [c.day for c in f.j.curves] == days[2:]
    assert f.j.runs == (r3,)
    assert [s.forecast_issue_s for s in f.steps] == [2, 2, 3, 3, 3, 3, 3, 3]
    assert [s.rce_pln_per_kwh for s in f.steps] == [price_of(f.j.curves, s.t)[0] for s in f.steps]
    assert not any(s.price_replaced for s in f.steps)


# ── the live clock (spec 012) ─────────────────────────────────────────────────────────────────────


def test_ac_012_02_on_clock_closes_raises_both_watermarks_and_releases() -> None:
    """L = 2. Slots 0 and 1 read on both sides, consumption's slot 3 waits behind a missing 2, and
    both watermarks still cover nothing. The clock at 4 closes 2 and 3 on both sides (with the held
    reading where there is one), raises both watermarks to 4 and releases 0 … 3."""
    f = Feed(params())
    f.curves(FIRST, day_curve(1, D20, FIRST - 40))
    f.run(FIRST, Run(1, S00, FIRST - 4))
    f.meters([FIRST, FIRST + 1])
    f.reading("consumption", FIRST + 3)
    assert f.steps == []
    released = f.clock(FIRST + 4)
    assert [(s.t, s.pv_estimated, s.load_estimated) for s in released] == [
        (FIRST, False, False),
        (FIRST + 1, False, False),
        (FIRST + 2, True, True),
        (FIRST + 3, True, False),
    ]
    assert [(s.pv_kwh, s.load_kwh) for s in released[2:]] == [
        (pv(FIRST + 1), load(FIRST + 1)),
        (pv(FIRST + 1), load(FIRST + 3)),
    ]
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in released] == [
        (1000.0 + t - T0_D20, False) for t in range(FIRST, FIRST + 4)
    ]
    assert [s.forecast_issue_s for s in released] == [S00] * 4
    assert (f.j.next, f.j.price_covered, f.j.forecast_covered) == (FIRST + 4, FIRST + 4, FIRST + 4)
    assert f.j.production is not None
    assert (f.j.production.stream.closed, f.j.production.stream.estimated) == (FIRST + 4, 2)
    assert (f.j.consumption.stream.closed, f.j.consumption.stream.estimated) == (FIRST + 4, 1)
    assert f.j.consumption.ready == () and f.j.production.ready == ()


def test_ac_012_02_on_clock_never_lowers_a_watermark() -> None:
    f = ready()
    f.meters([FIRST])
    released = f.clock(FIRST + 3)
    assert [(s.t, s.pv_estimated, s.load_estimated) for s in released] == [
        (FIRST + 1, True, True),
        (FIRST + 2, True, True),
    ]
    assert (f.j.price_covered, f.j.forecast_covered) == (FAR, FAR)


def test_ac_012_05_on_clock_caps_at_end_step() -> None:
    """No reading and no run at all: the clock far past the window closes every slot below
    ``end_step`` on the estimate 0 and releases them, and nothing at or after ``end_step``."""
    end = FIRST + 8
    f = Feed(params(end=end))
    f.curves(FIRST, day_curve(1, D20, FIRST - 40))
    released = f.clock(FIRST + 100)
    assert [s.t for s in released] == list(range(FIRST, end))
    assert all(s.pv_estimated and s.load_estimated for s in released)
    assert all(s.pv_kwh == 0.0 and s.load_kwh == 0.0 for s in released)
    assert all(s.forecast_issue_s is None and not s.price_replaced for s in released)
    assert (f.j.next, f.j.price_covered, f.j.forecast_covered) == (end, end, end)
    assert f.j.production is not None
    assert f.j.consumption.stream.closed == f.j.production.stream.closed == end
    before = f.j
    assert f.clock(FIRST + 200) == () and f.j == before


def test_ac_012_02_a_clock_before_the_window_releases_nothing() -> None:
    """A window that starts in the future: the clock at or before ``first_step`` neither closes a
    slot nor lets a watermark cover a step, even with the first step's readings held."""
    f = Feed(params())
    f.meters([FIRST])
    for clock_step in (FIRST - 50, FIRST - 1, FIRST):
        sides = (f.j.consumption, f.j.production)
        assert f.clock(clock_step) == ()
        assert (f.j.next, f.j.consumption, f.j.production) == (FIRST, *sides)
    assert [s.t for s in f.clock(FIRST + 1)] == [FIRST]


def test_ac_012_02_on_clock_for_a_site_without_pv() -> None:
    f = Feed(params(NO_PV_SITE))
    f.reading("consumption", FIRST)
    released = f.clock(FIRST + 2)
    assert [(s.t, s.pv_kwh, s.pv_estimated, s.load_estimated) for s in released] == [
        (FIRST, 0.0, False, False),
        (FIRST + 1, 0.0, False, True),
    ]
    assert f.j.production is None


def test_ac_012_03_a_reading_for_a_slot_the_clock_closed_corrects_a_later_step() -> None:
    """L = 2, nothing read: the clock at 5 closes 0 … 4 on 0 (horizon 4). Consumption's slot 1 then
    is a correction for step 5; production's slot 0 is at ``horizon - 2L``: too old."""
    f = Feed(params())
    f.curves(FIRST, day_curve(1, D20, FIRST - 40))
    f.run(FIRST, Run(1, S00, FIRST - 4))
    f.clock(FIRST + 5)
    assert [(s.t, s.load_estimated) for s in f.steps] == [
        (t, True) for t in range(FIRST, FIRST + 5)
    ]
    f.reading("consumption", FIRST + 1, 0.75)
    assert f.j.consumption.late == (Late(FIRST + 1, 0.75, FIRST + 5),)
    f.reading("production", FIRST, 0.5)
    assert f.j.production is not None and f.j.production.stream.too_old == 1
    released = f.clock(FIRST + 6)
    assert [s.t for s in released] == [FIRST + 5]
    assert released[0].corrections == (Correction("consumption", FIRST + 1, 0.75),)
    assert f.j.consumption.late == () and f.j.after_end_late == 0


def test_ac_012_05_a_reading_after_the_clock_closed_the_window_is_counted_after_end() -> None:
    end = FIRST + 8
    f = Feed(params(end=end))
    f.clock(FIRST + 100)
    assert f.j.next == end
    f.reading("consumption", end - 1, 0.6)  # a Late for step `end`: dropped
    assert f.j.consumption.late == ()
    assert (f.j.after_end_late, f.j.consumption.stream.late) == (1, 1)
    assert [s.t for s in f.steps] == list(range(FIRST, end))


def test_ac_012_04_a_curve_behind_the_watermark_is_clamped_and_counted() -> None:
    """The clock raised the price watermark to 4 before any curve came, so steps 0 … 3 ran on the
    replacement price. A snapshot then brings a curve visible from step -40 (moved to 4, counted)
    and one visible from step 6 (left as it is): no released step can read either."""
    f = Feed(params())
    f.run(FAR, Run(1, S00, FIRST - 4))
    f.clock(FIRST + 4)
    assert [(s.rce_pln_per_kwh, s.price_replaced) for s in f.steps] == [(0.0, True)] * 4
    behind = day_curve(1, D20, visible_step=FIRST - 40)
    ahead = day_curve(2, D20, visible_step=FIRST + 6, issued_s=100)
    f.curves(FAR, behind, ahead)
    assert f.j.clamped_price == 1 and f.j.clamped_forecast == 0
    assert f.j.curves == (dataclasses.replace(behind, visible_step=FIRST + 4), ahead)
    assert f.j.price_covered == FAR
    f.clock(FIRST + 7)
    assert [(s.t, s.rce_pln_per_kwh, s.price_replaced) for s in f.steps[4:]] == [
        (FIRST + 4, 1000.0 + FIRST + 4 - T0_D20, False),
        (FIRST + 5, 1000.0 + FIRST + 5 - T0_D20, False),
        (FIRST + 6, 2000.0 + FIRST + 6 - T0_D20, False),
    ]


def test_ac_012_04_a_run_behind_the_watermark_is_clamped_and_counted() -> None:
    """The floor is the watermark before each update: 4 (from the clock) for the first run, which
    moves, then 5 (the first run's own watermark) for the second, visible from 6, which does not."""
    f = Feed(params())
    f.curves(FAR, day_curve(1, D20, FIRST - 40))
    f.clock(FIRST + 4)
    assert [s.forecast_issue_s for s in f.steps] == [None] * 4
    behind, ahead = Run(1, S00, FIRST - 4), Run(2, S03, FIRST + 6)
    f.run(FIRST + 5, behind)
    assert f.j.clamped_forecast == 1
    assert f.j.runs == (dataclasses.replace(behind, visible_step=FIRST + 4),)
    f.run(FAR, ahead)
    assert (f.j.clamped_forecast, f.j.runs[-1]) == (1, ahead)
    f.clock(FIRST + 7)
    assert [s.forecast_issue_s for s in f.steps[4:]] == [S00, S00, S03]
    assert f.j.clamped_price == 0


def test_ac_012_04_records_at_or_after_the_watermark_are_not_moved() -> None:
    """Without a clock, every update a sparse stream emits is visible no earlier than the watermark
    it arrived behind (here equal to it): nothing moves and nothing is counted."""
    f = Feed(params())
    c1, c2 = day_curve(1, D20, FIRST - 40), day_curve(2, D20, FIRST + 2, issued_s=100)
    r1, r2 = Run(1, S00, FIRST - 20), Run(2, S03, FIRST + 1)
    f.curves(FIRST + 2, c1)
    f.curves(FIRST + 44, c2)
    f.run(FIRST + 1, r1)
    f.run(FIRST + 10, r2)
    assert (f.j.clamped_price, f.j.clamped_forecast) == (0, 0)
    assert (f.j.curves, f.j.runs) == ((c1, c2), (r1, r2))
    f.meters(range(FIRST, FIRST + 3))
    assert [s.forecast_issue_s for s in f.steps] == [S00, S03, S03]
    assert [s.rce_pln_per_kwh for s in f.steps] == [
        1000.0 + FIRST - T0_D20,
        1000.0 + FIRST + 1 - T0_D20,
        2000.0 + FIRST + 2 - T0_D20,
    ]


def test_ac_012_04_a_clean_feed_clamps_nothing() -> None:
    f = ready()
    f.meters(range(FIRST, FIRST + 4))
    assert (f.j.clamped_price, f.j.clamped_forecast, f.j.after_end_late) == (0, 0, 0)
