"""AC-011-14 … -16: visibility and watermarks of the sparse streams, from content and schedule.

Forecast runs (synthetic: delay 2 h, cadence 3 h): a run is visible from ``max(usable, watermark at
arrival)``, and moves the watermark to its availability plus the cadence. A run not newer by issue
time than the newest accepted one is stale (a duplicate or an overtaken run). A missing run leaves
the watermark where it was until the next one arrives; the join-level half (steps in between see
the older run) is in ``test_join.py``.

Price curves: visible from ``max(publication, watermark at arrival)``; a new newest day moves the
watermark to 13:00 Europe/Warsaw on that day; an older day, a repeated ``event_id`` and a second
curve of the newest day are counted.
"""

from __future__ import annotations

import dataclasses
from datetime import date

from ems.assembler.mapping import CurveIn, RunIn
from ems.assembler.sparse_streams import (
    Curve,
    ForecastStream,
    PriceStream,
    Run,
    forecast_apply,
    price_apply,
)
from tests.unit.assembler.cases import day_slots, seconds_at, step_at


def run_in(issued: str, usable: str, next_covered: str) -> RunIn:
    return RunIn(seconds_at(issued), step_at(usable), step_at(next_covered))


def synthetic(issued_hour: int) -> RunIn:
    """A synthetic run issued on 2026-10-20 at ``issued_hour`` UTC, as ``mapping.run`` maps it."""
    issued = f"2026-10-20T{issued_hour:02d}:00"
    usable = f"2026-10-20T{issued_hour + 2:02d}:00"
    following = f"2026-10-20T{issued_hour + 5:02d}:00"
    return run_in(issued, usable, following)


def curve_in(
    event_id: str, day: date, issued: str, prices: tuple[float, ...] | None = None
) -> CurveIn:
    """A curve of ``day`` published at ``issued`` (UTC); window opens 13:00 local on ``day``."""
    t0, n = day_slots(day)
    stamp = seconds_at(issued)
    opens = step_at(f"{day.isoformat()}T11:00")  # 13:00 CEST (October days before the 25th)
    return CurveIn(
        event_id,
        t0,
        day,
        prices if prices is not None else tuple(float(i) for i in range(n)),
        stamp,
        -(-stamp // 900),
        opens,
    )


# ── forecasts ─────────────────────────────────────────────────────────────────────────────────────


def test_ac_011_14_runs_on_schedule() -> None:
    s, run = forecast_apply(ForecastStream(), synthetic(0))
    assert run == Run(1, seconds_at("2026-10-20T00:00"), step_at("2026-10-20T02:00"))
    assert s == ForecastStream(
        covered=step_at("2026-10-20T05:00"), newest_issue_s=seconds_at("2026-10-20T00:00"), seq=1
    )
    s, run = forecast_apply(s, synthetic(3))
    assert run == Run(2, seconds_at("2026-10-20T03:00"), step_at("2026-10-20T05:00"))
    assert (s.covered, s.seq, s.clamped, s.stale) == (step_at("2026-10-20T08:00"), 2, 0, 0)


def test_ac_011_14_a_missing_run_leaves_the_watermark_until_the_next_one() -> None:
    s, _ = forecast_apply(ForecastStream(), synthetic(0))
    assert s.covered == step_at("2026-10-20T05:00")  # the 03:00 run never arrives
    s, run = forecast_apply(s, synthetic(6))
    assert run == Run(2, seconds_at("2026-10-20T06:00"), step_at("2026-10-20T08:00"))
    assert (s.covered, s.clamped) == (step_at("2026-10-20T11:00"), 0)


def test_ac_011_14_a_late_run_is_used_like_one_on_time() -> None:
    """Lateness in wall time is invisible to the stream: the run keeps its place in its key's
    order, so it maps to the same visibility; the household just waits for it."""
    s, _ = forecast_apply(ForecastStream(), synthetic(0))
    s, run = forecast_apply(s, synthetic(3))
    assert run is not None and run.visible_step == step_at("2026-10-20T05:00")
    assert s.clamped == 0


def test_ac_011_14_an_unscheduled_run_is_visible_at_the_watermark() -> None:
    s, _ = forecast_apply(ForecastStream(), synthetic(0))
    # issued 01:00, usable 03:00, behind the watermark of 05:00
    s, run = forecast_apply(s, run_in("2026-10-20T01:00", "2026-10-20T03:00", "2026-10-20T06:00"))
    assert run == Run(2, seconds_at("2026-10-20T01:00"), step_at("2026-10-20T05:00"))
    assert (s.covered, s.clamped, s.seq) == (step_at("2026-10-20T06:00"), 1, 2)


def test_ac_011_14_stale_runs_are_counted_and_ignored() -> None:
    s, _ = forecast_apply(ForecastStream(), synthetic(0))
    s, _ = forecast_apply(s, synthetic(3))
    for stale in (synthetic(3), synthetic(0)):  # a duplicate, an overtaken run
        s2, run = forecast_apply(s, stale)
        assert run is None and s2 == dataclasses.replace(s, stale=s.stale + 1)
        s = s2
    assert s.stale == 2 and s.seq == 2


def test_ac_011_14_the_watermark_only_rises() -> None:
    s, _ = forecast_apply(ForecastStream(), synthetic(0))
    newer = run_in("2026-10-20T00:30", "2026-10-20T02:30", "2026-10-20T04:00")
    s2, run = forecast_apply(s, newer)
    assert run is not None and s2.covered == s.covered == step_at("2026-10-20T05:00")


# ── prices ────────────────────────────────────────────────────────────────────────────────────────

D20, D21, D22 = date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22)


def test_ac_011_16_curves_on_schedule() -> None:
    first = curve_in("c20", D20, "2026-10-19T11:40:07")
    s, c = price_apply(PriceStream(), first)
    assert c == Curve(1, first.t0, D20, first.rce_pln_per_kwh, first.issued_s, first.issued_step)
    assert first.issued_step == step_at("2026-10-19T11:45")
    assert s == PriceStream(
        covered=step_at("2026-10-20T11:00"), newest_t0=first.t0, newest_ids=("c20",), seq=1
    )
    second = curve_in("c21", D21, "2026-10-20T11:25:03")
    s, c = price_apply(s, second)
    assert c is not None and (c.seq, c.visible_step) == (2, step_at("2026-10-20T11:30"))
    assert (s.covered, s.newest_t0, s.newest_ids, s.clamped) == (
        step_at("2026-10-21T11:00"),
        second.t0,
        ("c21",),
        0,
    )


def test_ac_011_16_a_same_day_publication_is_not_clamped() -> None:
    """PSE published 2026-03-04 at 08:05 local on the day itself: before the window of that day
    opens, after the one of the day before."""
    s, _ = price_apply(PriceStream(), curve_in("c20", D20, "2026-10-19T11:40"))
    s, c = price_apply(s, curve_in("c21", D21, "2026-10-21T06:05"))
    assert c is not None and c.visible_step == step_at("2026-10-21T06:15") and s.clamped == 0
    assert s.covered == step_at("2026-10-21T11:00")


def test_ac_011_16_an_early_publication_is_visible_at_the_watermark() -> None:
    s, _ = price_apply(PriceStream(), curve_in("c20", D20, "2026-10-19T11:40"))
    # the next day published at 12:30 local on 2026-10-20, before its window opens (13:00 local)
    s, c = price_apply(s, curve_in("c21", D21, "2026-10-20T10:30"))
    assert c is not None and c.visible_step == step_at("2026-10-20T11:00")
    assert (s.clamped, s.covered) == (1, step_at("2026-10-21T11:00"))


def test_ac_011_16_a_republished_day_is_revised_and_visible_from_the_watermark() -> None:
    s, _ = price_apply(PriceStream(), curve_in("c21", D21, "2026-10-20T11:30"))
    revised = curve_in("c21-b", D21, "2026-10-20T13:02", tuple(float(-i) for i in range(96)))
    s2, c = price_apply(s, revised)
    assert c == Curve(
        2,
        revised.t0,
        D21,
        revised.rce_pln_per_kwh,
        revised.issued_s,
        step_at("2026-10-21T11:00"),  # max(13:15 on the 20th, the watermark)
    )
    assert s2 == dataclasses.replace(s, newest_ids=("c21", "c21-b"), seq=2, revised=1, clamped=1)
    later = curve_in("c21-c", D21, "2026-10-21T12:01")  # after the watermark: not clamped
    s3, c = price_apply(s2, later)
    assert c is not None and c.visible_step == step_at("2026-10-21T12:15")
    assert (s3.revised, s3.clamped, s3.covered) == (2, 1, s.covered)


def test_ac_011_16_an_older_day_is_counted_and_ignored() -> None:
    s, _ = price_apply(PriceStream(), curve_in("c21", D21, "2026-10-20T11:30"))
    s, _ = price_apply(s, curve_in("c22", D22, "2026-10-21T11:30"))
    s2, c = price_apply(s, curve_in("c21-late", D21, "2026-10-21T12:00"))
    assert c is None and s2 == dataclasses.replace(s, old_day=1)


def test_ac_011_16_a_duplicate_curve_is_counted_and_ignored() -> None:
    s, _ = price_apply(PriceStream(), curve_in("c21", D21, "2026-10-20T11:30"))
    s, _ = price_apply(s, curve_in("c21-b", D21, "2026-10-20T12:30"))
    for event_id in ("c21", "c21-b"):
        s2, c = price_apply(s, curve_in(event_id, D21, "2026-10-20T11:30"))
        assert c is None and s2 == dataclasses.replace(s, duplicate=s.duplicate + 1)
        s = s2
    assert (s.duplicate, s.seq, s.revised) == (2, 2, 1)


def test_ac_011_16_a_later_day_restarts_the_ids() -> None:
    s, _ = price_apply(PriceStream(), curve_in("x", D21, "2026-10-20T11:30"))
    s, c = price_apply(s, curve_in("x", D22, "2026-10-21T11:30"))  # same id, new day: not a dup
    assert c is not None and s.newest_ids == ("x",) and s.duplicate == 0 and s.seq == 2
