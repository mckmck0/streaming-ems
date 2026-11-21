"""The two sparse streams: forecast runs (per location) and price curves (per market).

A sparse stream's watermark is the earliest moment its next record can become usable (ADR 0017):
- a forecast run moves it to the run's availability plus the model's run cadence;
- the curve of a new delivery day moves it to the opening of the next publication window (13:00
  local on that day).
A record becomes visible from the later of its own availability and the watermark when it arrived,
so a record that arrives behind the watermark can never change a step that may already have run.

A run not newer than the newest one accepted, and a curve of a day older than the newest one, could
only become visible after a newer record: they are counted and dropped where they arrive.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date

from ems.assembler.mapping import CurveIn, RunIn


@dataclass(frozen=True)
class Run:
    seq: int  # the stream's numbering, in order of acceptance
    issue_s: int
    visible_step: int


@dataclass(frozen=True)
class Curve:
    seq: int
    t0: int
    day: date
    rce_pln_per_kwh: tuple[float, ...]
    issued_s: int
    visible_step: int


@dataclass(frozen=True)
class ForecastStream:
    covered: int = 0  # steps below it are covered; 0 covers nothing
    newest_issue_s: int | None = None
    seq: int = 0
    stale: int = 0  # not newer than the newest run (duplicates included)
    clamped: int = 0  # made visible later than its availability by the watermark


@dataclass(frozen=True)
class PriceStream:
    covered: int = 0
    newest_t0: int | None = None
    newest_ids: tuple[str, ...] = ()  # the curves accepted for the newest day
    seq: int = 0
    duplicate: int = 0
    old_day: int = 0
    clamped: int = 0
    revised: int = 0  # a further curve of the newest day


def forecast_apply(s: ForecastStream, r: RunIn) -> tuple[ForecastStream, Run | None]:
    if s.newest_issue_s is not None and r.issue_s <= s.newest_issue_s:
        return replace(s, stale=s.stale + 1), None
    visible = max(r.usable_step, s.covered)
    seq = s.seq + 1
    stream = ForecastStream(
        covered=max(s.covered, r.next_covered),
        newest_issue_s=r.issue_s,
        seq=seq,
        stale=s.stale,
        clamped=s.clamped + int(visible > r.usable_step),
    )
    return stream, Run(seq, r.issue_s, visible)


def price_apply(s: PriceStream, c: CurveIn) -> tuple[PriceStream, Curve | None]:
    if s.newest_t0 is not None and c.t0 < s.newest_t0:
        return replace(s, old_day=s.old_day + 1), None
    if c.t0 == s.newest_t0 and c.event_id in s.newest_ids:
        return replace(s, duplicate=s.duplicate + 1), None
    visible = max(c.issued_step, s.covered)
    seq = s.seq + 1
    if s.newest_t0 is None or c.t0 > s.newest_t0:
        covered, ids, revised = max(s.covered, c.opens_step), (c.event_id,), s.revised
    else:
        covered, ids, revised = s.covered, (*s.newest_ids, c.event_id), s.revised + 1
    stream = PriceStream(
        covered=covered,
        newest_t0=c.t0,
        newest_ids=ids,
        seq=seq,
        duplicate=s.duplicate,
        old_day=s.old_day,
        clamped=s.clamped + int(visible > c.issued_step),
        revised=revised,
    )
    return stream, Curve(seq, c.t0, c.day, c.rce_pln_per_kwh, c.issued_s, visible)
