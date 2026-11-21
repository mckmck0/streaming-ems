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

Prices in these tests are ``seq * 1000 + slot`` (``cases.day_curve``): each value names its curve.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from datetime import date

import pytest

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


def test_a_correction_for_a_step_never_released_stays_unapplied() -> None:
    end = FIRST + 4
    f = Feed(params(end=end))
    f.curves(FAR, day_curve(1, D20, FIRST - 40))
    f.run(FAR, Run(1, S00, FIRST - 4))
    f.meters([FIRST, FIRST + 2, FIRST + 3])
    f.meters([FIRST + 1])  # late on both sides, for step FIRST + 4 = end
    assert [s.t for s in f.steps] == [FIRST, FIRST + 1, FIRST + 2, FIRST + 3]
    assert all(s.corrections == () for s in f.steps)
    assert f.j.consumption.late == (Late(FIRST + 1, load(FIRST + 1), end),)


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
    for r in (r1, r2, r3):
        f.run(FAR, r)
    f.meters([first])  # next = first + 1, still the 20th
    assert [c.day for c in f.j.curves] == days[1:]
    assert f.j.runs == (r2, r3)
    f.meters(range(first + 1, first + 8))  # next = first + 8, the 21st
    assert [c.day for c in f.j.curves] == days[2:]
    assert f.j.runs == (r3,)
    assert [s.forecast_issue_s for s in f.steps] == [2, 2, 3, 3, 3, 3, 3, 3]
    assert [s.rce_pln_per_kwh for s in f.steps] == [price_of(f.j.curves, s.t)[0] for s in f.steps]
    assert not any(s.price_replaced for s in f.steps)
