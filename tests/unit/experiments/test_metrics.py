"""AC-012-29 … -32 (plan §7, Metrics): the systems metrics of a run, on synthetic timing lines.

``ems.experiments.metrics`` is pure. A timing line is one handler commit: ``ns`` (wall time),
service, handler, key, invocation id, the ``t`` handled and the ``(service, t)`` of each message
sent. The first line per invocation is kept and an invocation with more lines counts as retried
(AC-012-29). A step is released by the `Household` line that sends ``("Ems", t)`` and applied by the
`Battery` line that sends ``("Ems", t)`` (the ``Ack``). Times in ns are whole numbers chosen so that
every expected value is exact in binary floating point.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from ems.experiments import metrics
from ems.experiments.metrics import KillEvent, Line, Recovery, Stats, Sustained

S = 1_000_000_000  # ns per second
MS = 1_000_000  # ns per ms
A, B, C = "r1:PL-LOC-000001", "r1:PL-LOC-000002", "r1:PL-LOC-000004"


def hh(ns: int, key: str, *ts: int, inv: str | None = None, in_t: int | None = None) -> Line:
    """A `Household` commit releasing steps ``ts`` (to SelfConsumption and Ems)."""
    sent = tuple(x for t in ts for x in (("SelfConsumption", t), ("Ems", t)))
    return Line(ns, "Household", "on_measurement", key, inv or f"hh-{key}-{ns}", in_t, sent)


def bat(ns: int, key: str, t: int, inv: str | None = None) -> Line:
    """A `Battery` commit applying step ``t``: the Ack to Ems and the flows to Meter."""
    return Line(
        ns, "Battery", "apply", key, inv or f"bat-{key}-{ns}", t, (("Ems", t), ("Meter", t))
    )


def mtr(ns: int, key: str, t: int | None, inv: str | None = None) -> Line:
    return Line(ns, "Meter", "on_flows", key, inv or f"mtr-{key}-{ns}", t, ())


# ── parse and dedup ──────────────────────────────────────────────────────────────────────────────


def test_ac_012_29_parse_reads_json_lines_and_skips_blank_ones() -> None:
    text = [
        json.dumps(
            {
                "ns": 12 * S,
                "svc": "Household",
                "h": "on_prices",
                "key": A,
                "inv": "inv_1",
                "in_t": 7,
                "sent": [["SelfConsumption", 7], ["Ems", 7]],
            }
        )
        + "\n",
        "\n",
        "",
        json.dumps(
            {
                "ns": 13 * S,
                "svc": "Tariff",
                "h": "subscribe",
                "key": "r1:PL:RCE",
                "inv": "inv_2",
                "in_t": None,
                "sent": [],
            }
        ),
    ]
    assert metrics.parse(text) == [
        Line(12 * S, "Household", "on_prices", A, "inv_1", 7, (("SelfConsumption", 7), ("Ems", 7))),
        Line(13 * S, "Tariff", "subscribe", "r1:PL:RCE", "inv_2", None, ()),
    ]


def test_ac_012_29_dedup_keeps_the_first_line_per_invocation_and_counts_retries() -> None:
    first_a = mtr(100, A, 5, inv="a")
    lines = [
        mtr(200, B, 5, inv="b"),
        first_a,
        mtr(300, A, 5, inv="a"),  # the retry of "a" writes a second line
        mtr(400, C, 5, inv="c"),
        mtr(500, C, 5, inv="c"),
        mtr(600, C, 5, inv="c"),  # three lines: still one retried invocation
    ]
    kept, retries = metrics.dedup(lines)
    assert retries == 2
    assert kept == [
        Line(100, "Meter", "on_flows", A, "a", 5, (), lines=2),
        Line(200, "Meter", "on_flows", B, "b", 5, (), lines=1),
        Line(400, "Meter", "on_flows", C, "c", 5, (), lines=3),
    ]


def test_ac_012_29_dedup_of_nothing() -> None:
    assert metrics.dedup([]) == ([], 0)


# ── stats ────────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_29_stats_uses_nearest_rank_percentiles() -> None:
    assert metrics.stats([float(v) for v in range(100, 0, -1)]) == Stats(100, 50.0, 99.0, 100.0)
    # n = 3: p50 is rank ceil(1.5) = 2, p99 rank ceil(2.97) = 3.
    assert metrics.stats([5.0, 1.0, 3.0], missing=4) == Stats(3, 3.0, 5.0, 5.0, missing=4)
    assert metrics.stats([7.0]) == Stats(1, 7.0, 7.0, 7.0)


def test_ac_012_29_stats_of_nothing_is_none() -> None:
    assert metrics.stats([], missing=2) == Stats(0, None, None, None, missing=2)


# ── releases, applies, decision latency ──────────────────────────────────────────────────────────


def test_ac_012_29_releases_are_the_first_household_line_sending_each_step_to_ems() -> None:
    lines = [
        hh(10, A, 5),
        hh(20, A, 6, 7),  # one batch releases two steps
        bat(25, A, 5),  # a Battery line sends to Ems too, but it is no release
        hh(30, A, 5, inv="retried"),  # a later line for the same step is not the release
        hh(40, B, 5),
        Line(50, "Household", "on_forecast", A, "x", None, (("SelfConsumption", 8),)),
    ]
    assert metrics.releases(lines) == {(A, 5): 10, (A, 6): 20, (A, 7): 20, (B, 5): 40}


def test_ac_012_29_applies_are_the_first_battery_line_sending_each_ack() -> None:
    lines = [bat(15, A, 5), bat(16, A, 5, inv="again"), bat(30, B, 5), hh(40, A, 6), mtr(50, A, 5)]
    assert metrics.applies(lines) == {(A, 5): 15, (B, 5): 30}


def test_ac_012_29_decision_latency_pairs_apply_and_release_and_counts_missing_lines() -> None:
    lines = [
        hh(1 * S, A, 0),
        bat(1 * S + 2 * MS, A, 0),  # 2 ms
        hh(2 * S, A, 1),
        bat(2 * S + 4 * MS, A, 1),  # 4 ms
        hh(1 * S + S // 2, B, 0),  # no apply: missing
        bat(3 * S, C, 0),  # no release (its line was lost at a kill): missing
    ]
    assert metrics.decision_latency_ms(lines) == Stats(2, 2.0, 4.0, 4.0, missing=2)


def test_ac_012_29_a_step_released_inside_a_recovery_window_is_left_out() -> None:
    lines = [
        hh(1 * S, A, 0),
        bat(1 * S + 2 * MS, A, 0),
        hh(5 * S, B, 1),  # inside the window: reported with the recovery
        bat(5 * S + 100 * MS, B, 1),
    ]
    got = metrics.decision_latency_ms(lines, windows=[(4 * S, 6 * S)])
    assert got == Stats(1, 2.0, 2.0, 2.0, missing=0)
    assert metrics.decision_latency_ms(lines) == Stats(2, 2.0, 100.0, 100.0)


# ── release lag (AC-012-29) and the sustained rule (AC-012-31) ───────────────────────────────────

PACED: dict[str, Any] = {
    "paced_from_unix": 1000.0,
    "speed": 96.0,
    "first_step": 100,
    "meter_delay_s": 60.0,
}


@pytest.mark.parametrize(("t", "due"), [(100, 1010.0), (103, 1038.125), (195, 1900.625)])
def test_ac_012_29_due_unix_is_when_the_reading_of_a_slot_is_due_on_the_schedule(
    t: int, due: float
) -> None:
    # paced_from + (900·(t+1) + meter_delay - 900·first_step) / speed
    assert metrics.due_unix(t, **PACED) == due


def test_ac_012_29_release_lag_is_the_release_minus_the_due_time() -> None:
    lines = [
        hh(1_012_500_000_000, A, 100),  # due 1010.0, released 1012.5
        hh(1_038_125_000_000, B, 103),  # released exactly when due
        hh(1_040_000_000_000, A, 103),
        bat(1_041_000_000_000, A, 103),  # no release line: no lag
    ]
    assert metrics.release_lags_s(lines, **PACED) == {
        (A, 100): 2.5,
        (B, 103): 0.0,
        (A, 103): 1.875,
    }


def _lags(within: int, over: int, at_s: float, over_s: float) -> dict[tuple[str, int], float]:
    lags = {(A, t): at_s for t in range(within)}
    lags |= {(B, t): over_s for t in range(over)}
    return lags


def test_ac_012_31_a_speed_is_sustained_at_its_boundary() -> None:
    # S = 96: 900 / S = 9.375 s per step; the drain may take 2 x 900 / S = 18.75 s.
    lags = _lags(99, 1, 9.375, 50.0)
    got = metrics.sustained(lags, speed=96.0, last_publish_unix=1000.0, last_release_unix=1018.75)
    assert got == Sustained(96.0, True, 9.375, 18.75)


def test_ac_012_31_a_drain_just_over_two_quarters_is_not_sustained() -> None:
    lags = _lags(99, 1, 9.375, 50.0)
    got = metrics.sustained(lags, speed=96.0, last_publish_unix=1000.0, last_release_unix=1019.0)
    assert got == Sustained(96.0, False, 9.375, 19.0)


def test_ac_012_31_two_steps_in_a_hundred_over_the_lag_are_not_sustained() -> None:
    lags = _lags(98, 2, 9.375, 50.0)
    got = metrics.sustained(lags, speed=96.0, last_publish_unix=1000.0, last_release_unix=1001.0)
    assert got.sustained is False
    assert got.p99_lag_s == 50.0


# ── recovery phases (AC-012-30) ──────────────────────────────────────────────────────────────────

KILL = KillEvent(target="sdk", kill_unix=2000.0, healthy_unix=2003.0)


def _before_kill() -> tuple[list[Line], dict[tuple[str, int], float]]:
    """Ten releases before the kill; their lags have p99 (nearest rank) 2.0 s."""
    lines = [hh((1000 + t) * S, A, t) for t in range(10)]
    lags = {(A, t): 1.0 for t in range(9)} | {(A, 9): 2.0}
    return lines, lags


def test_ac_012_30_recovery_reports_the_three_phases_of_a_kill() -> None:
    lines, lags = _before_kill()
    lines += [hh(2005 * S, A, 10), hh(2006 * S, A, 11), hh(2008 * S, A, 12), hh(2009 * S, A, 13)]
    lags |= {(A, 10): 30.0, (A, 11): 10.0, (A, 12): 1.5, (A, 13): 1.0}
    rec = metrics.recovery(KILL, lines, lags)
    # kill → healthy 3 s; healthy → first commit 2 s; first commit → back under the p99 3 s.
    assert rec == Recovery("sdk", healthy_s=3.0, first_commit_s=2.0, caught_up_s=3.0)
    assert metrics.recovery_window(KILL, rec) == (2000 * S, 2008 * S)


def test_ac_012_30_a_run_that_never_catches_up_has_an_open_window() -> None:
    lines, lags = _before_kill()
    lines += [hh(2005 * S, A, 10), hh(2006 * S, A, 11)]
    lags |= {(A, 10): 30.0, (A, 11): 10.0}
    rec = metrics.recovery(KILL, lines, lags)
    assert rec == Recovery("sdk", healthy_s=3.0, first_commit_s=2.0, caught_up_s=None)
    start, end = metrics.recovery_window(KILL, rec)
    assert start == 2000 * S
    assert end > max(line.ns for line in lines)
    released_late = metrics.decision_latency_ms(
        [hh(2006 * S, A, 11), bat(2006 * S + MS, A, 11)], windows=[(start, end)]
    )
    assert released_late.count == 0


def test_ac_012_30_no_commit_after_the_kill() -> None:
    lines, lags = _before_kill()
    rec = metrics.recovery(KILL, lines, lags)
    assert rec == Recovery("sdk", healthy_s=3.0, first_commit_s=None, caught_up_s=None)


# ── end and stall detection (AC-012-32) ──────────────────────────────────────────────────────────


def test_ac_012_32_meter_last_is_the_highest_t_per_key_over_meter_lines() -> None:
    lines = [
        mtr(1, A, 5),
        mtr(2, A, 7),
        mtr(3, A, 6),  # a lower t later does not lower it
        mtr(4, B, None),  # configure: no t
        mtr(5, B, 3),
        hh(6, C, 9),
        bat(7, C, 9),
    ]
    assert metrics.meter_last(lines) == {A: 7, B: 3}


def test_ac_012_32_the_run_ends_when_every_meter_handled_the_step_before_its_end() -> None:
    ends = {A: 100, B: 100}
    lines = [mtr(1, A, 99), mtr(2, B, 98)]
    assert metrics.ended(lines, ends) is False
    assert metrics.ended([*lines, mtr(3, B, 99)], ends) is True
    assert metrics.ended([mtr(1, A, 99)], ends) is False  # B has no Meter line yet
    assert metrics.ended([hh(1, A, 99), hh(2, B, 99)], ends) is False


def test_ac_012_32_stalled_when_no_meter_line_advanced_for_stall_s() -> None:
    lines = [
        mtr(10 * S, A, 5),
        mtr(20 * S, A, 6),  # the last advance
        mtr(100 * S, A, 6, inv="retry"),  # the same t again: no advance
        mtr(150 * S, B, None),
        hh(200 * S, A, 7),  # not a Meter line
    ]
    assert metrics.stalled(lines, now_ns=319 * S, stall_s=300.0, since_ns=0) is False
    assert metrics.stalled(lines, now_ns=321 * S, stall_s=300.0, since_ns=0) is True


def test_ac_012_32_without_meter_lines_the_stall_counts_from_since() -> None:
    lines = [hh(5 * S, A, 1)]
    assert metrics.stalled(lines, now_ns=1299 * S, stall_s=300.0, since_ns=1000 * S) is False
    assert metrics.stalled(lines, now_ns=1301 * S, stall_s=300.0, since_ns=1000 * S) is True


def test_ac_012_32_last_t_per_service_for_the_stall_diagnosis() -> None:
    lines = [hh(1, A, 3, in_t=4), hh(2, B, 3, in_t=9), bat(3, A, 8), mtr(4, A, 7), mtr(5, B, None)]
    assert metrics.last_t(lines) == {"Household": 9, "Battery": 8, "Meter": 7}


# ── throughput, forecast age, live release delay ─────────────────────────────────────────────────


def test_ac_012_29_throughput_is_per_wall_second_from_first_publish_to_end() -> None:
    lines = [mtr(i, A, i) for i in range(10)]
    got = metrics.throughput(lines, records=2000, start_unix=1000.0, end_unix=1100.0)
    assert got["records_per_s"] == 20.0
    assert got["invocations_per_s"] == 0.1


def test_ac_012_29_forecast_age_in_hours_with_missing_runs() -> None:
    steps = [(400, 400 * 900 - 7200), (401, 400 * 900 - 3600), (402, None)]
    assert metrics.forecast_age_h(steps) == Stats(2, 1.25, 2.0, 2.0, missing=1)


def test_ac_012_36_live_release_delay_is_the_release_minus_the_quarter_end() -> None:
    lines = [
        hh(901_025_500_000_000, A, 1000),  # quarter 1000 ends at 900 900 s: 125.5 s
        hh(901_921_000_000_000, A, 1001),  # quarter 1001 ends at 901 800 s: 121.0 s
        bat(901_922_000_000_000, A, 1001),
    ]
    assert metrics.live_release_delay_s(lines) == Stats(2, 121.0, 125.5, 125.5)
