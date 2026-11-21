"""The household's join: releases its steps in order, each only when every needed stream covers it.

Step ``q`` is released, strictly in order from the run's first step, when every needed meter stream
(consumption, and production if the site has PV) has closed slot ``q`` and the price and forecast
watermarks are past the end of ``q`` (ADR 0017). It reads only what was visible at its start:
- PV and load of slot ``q``, with their ``estimated`` flags;
- the price of ``q`` from the newest visible curve of its delivery day; without one, the statutory
  replacement (RES act art. 4b ust. 10): the same local quarter-hour of the nearest earlier delivery
  day, at most two days back, with a visible curve; 0 if there is none (ADR 0018);
- the issue time of the newest visible forecast run;
- the late readings due at ``q``, as corrections, production's before consumption's.

How the streams interleave cannot change a step: each stream's state depends only on its own order,
and a record that arrives after a step was released is not visible to that step.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta

from ems.assembler.mapping import CONSUMPTION, PRODUCTION, Reading
from ems.assembler.meter_stream import Closed, Late, MeterStream, meter_apply
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import Curve, Run
from ems.contracts.time import WARSAW, from_quarter_index
from ems.domain.settlement import Correction

REPLACEMENT_DAYS = 2  # how far back the replacement price looks: covers both DST days


@dataclass(frozen=True)
class Step:
    """One released quarter-hour, sent to every strategy and to Ems."""

    t: int
    pv_kwh: float
    load_kwh: float
    rce_pln_per_kwh: float
    pv_estimated: bool
    load_estimated: bool
    price_replaced: bool
    forecast_issue_s: int | None  # the newest run visible at the step's start
    corrections: tuple[Correction, ...]
    schema_version: str = "1.0.0"


@dataclass(frozen=True)
class MeterSide:
    stream: MeterStream
    ready: tuple[Closed, ...] = ()  # closed slots not yet released, from ``Join.next`` on
    late: tuple[Late, ...] = ()  # corrections waiting for their step


@dataclass(frozen=True)
class Join:
    next: int  # the next step to release
    consumption: MeterSide
    production: MeterSide | None  # None: the site has no PV
    price_covered: int = 0  # the price stream's watermark: steps below it are covered
    curves: tuple[Curve, ...] = ()
    forecast_covered: int = 0
    runs: tuple[Run, ...] = ()
    price_replaced: int = 0
    unneeded: int = 0  # production readings of a site without PV
    unusable: int = 0  # meter records the mapping could not use


def start(params: RunParams) -> Join:
    side = MeterSide(MeterStream(closed=params.first_step))
    return Join(
        next=params.first_step, consumption=side, production=side if params.has_pv else None
    )


def on_reading(params: RunParams, j: Join, r: Reading | None) -> tuple[Join, tuple[Step, ...]]:
    if r is None:
        return _release(replace(j, unusable=j.unusable + 1))
    if r.stream == PRODUCTION:
        if j.production is None:
            return _release(replace(j, unneeded=j.unneeded + 1))
        return _release(replace(j, production=_add(params, j.production, r)))
    return _release(replace(j, consumption=_add(params, j.consumption, r)))


def on_curves(
    params: RunParams, j: Join, covered: int, curves: Sequence[Curve]
) -> tuple[Join, tuple[Step, ...]]:
    """Curves in the price stream's order, with the stream's watermark after them."""
    j = replace(j, price_covered=max(j.price_covered, covered), curves=(*j.curves, *curves))
    return _release(j)


def on_run(params: RunParams, j: Join, covered: int, run: Run) -> tuple[Join, tuple[Step, ...]]:
    """A run in the forecast stream's order, with the stream's watermark after it."""
    j = replace(j, forecast_covered=max(j.forecast_covered, covered), runs=(*j.runs, run))
    return _release(j)


def _add(params: RunParams, side: MeterSide, r: Reading) -> MeterSide:
    stream, closed, late = meter_apply(
        side.stream, params.first_step, params.end_step, params.late_slots, r
    )
    return MeterSide(stream, side.ready + closed, side.late + ((late,) if late else ()))


def _covered(j: Join) -> bool:
    q = j.next
    sides = (j.consumption,) if j.production is None else (j.consumption, j.production)
    return (
        all(side.ready and side.ready[0].t == q for side in sides)
        and j.price_covered > q
        and j.forecast_covered > q
    )


def _release(j: Join) -> tuple[Join, tuple[Step, ...]]:
    steps: list[Step] = []
    while _covered(j):
        q = j.next
        load = j.consumption.ready[0]
        pv = j.production.ready[0] if j.production is not None else Closed(q, 0.0, False)
        price, replaced = _price(j.curves, q)
        corrections = _due(j.production, PRODUCTION, q) + _due(j.consumption, CONSUMPTION, q)
        steps.append(
            Step(
                t=q,
                pv_kwh=pv.kwh,
                load_kwh=load.kwh,
                rce_pln_per_kwh=price,
                pv_estimated=pv.estimated,
                load_estimated=load.estimated,
                price_replaced=replaced,
                forecast_issue_s=_forecast_issue(j.runs, q),
                corrections=corrections,
            )
        )
        j = replace(
            j,
            next=q + 1,
            consumption=_pop(j.consumption, q),
            production=None if j.production is None else _pop(j.production, q),
            price_replaced=j.price_replaced + replaced,
        )
    return _prune(j), tuple(steps)


def _due(side: MeterSide | None, stream: str, q: int) -> tuple[Correction, ...]:
    if side is None:
        return ()
    return tuple(Correction(stream, late.t, late.kwh) for late in side.late if late.at_step == q)


def _pop(side: MeterSide, q: int) -> MeterSide:
    return MeterSide(side.stream, side.ready[1:], tuple(x for x in side.late if x.at_step != q))


def _forecast_issue(runs: tuple[Run, ...], q: int) -> int | None:
    for run in reversed(runs):  # visibility rises with the stream's order
        if run.visible_step <= q:
            return run.issue_s
    return None


def _local(t: int) -> datetime:
    return from_quarter_index(t).astimezone(WARSAW)


def _newest(curves: list[Curve]) -> Curve:
    """The latest publication; between equal times, the one accepted first."""
    return max(curves, key=lambda c: (c.issued_s, -c.seq))


def _price(curves: tuple[Curve, ...], q: int) -> tuple[float, bool]:
    """The price of step ``q`` and whether it is the replacement."""
    visible = [c for c in curves if c.visible_step <= q]
    own = [c for c in visible if c.t0 <= q < c.t0 + len(c.rce_pln_per_kwh)]
    if own:
        c = _newest(own)
        return c.rce_pln_per_kwh[q - c.t0], False
    at = _local(q)
    for back in range(1, REPLACEMENT_DAYS + 1):
        day = at.date() - timedelta(days=back)
        same_day = [c for c in visible if c.day == day]
        if same_day:
            c = _newest(same_day)
            i = _slot_at(c, at.hour, at.minute)
            if i is not None:
                return c.rce_pln_per_kwh[i], True
    return 0.0, True


def _slot_at(c: Curve, hour: int, minute: int) -> int | None:
    """The first slot of ``c`` at this local clock time: the first copy on a 100-slot day; None on
    the 92-slot day for 02:00-02:59.
    """
    for i in range(len(c.rce_pln_per_kwh)):
        local = _local(c.t0 + i)
        if (local.hour, local.minute) == (hour, minute):
            return i
    return None


def _prune(j: Join) -> Join:
    """Drop what no later step can read: curves of days more than two days before the day of the
    next step, and runs older than the newest one visible at it.
    """
    oldest: date = _local(j.next).date() - timedelta(days=REPLACEMENT_DAYS)
    curves = tuple(c for c in j.curves if c.day >= oldest)
    runs = j.runs
    for i in range(len(runs) - 1, -1, -1):
        if runs[i].visible_step <= j.next:
            runs = runs[i:]
            break
    return replace(j, curves=curves, runs=runs)
