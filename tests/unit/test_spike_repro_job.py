"""AC-010-24, AC-010-25: the port check and the verdict of the spike job (spec 010, plan §7).

- AC-010-25: ``make spike`` exits before starting anything when port 8080, 9070, 9080 or 9081 is in
  use; a port is in use when binding it on 127.0.0.1 fails (this tier blocks connects, not binds).
- AC-010-24: the run passes only if A and C report 0 mismatches, 0 meter-against-SelfConsumption
  mismatches and 0 out-of-order events, B reports 0 mismatches, 0 out-of-order events and 96 events
  per household and strategy, and the verb log (``<UTC> <verb> <key=value ...>``) shows that B
  killed the SDK service (``kill-svc1 pids=...``) and the server (``kill-server ... exit=137``).

The canned results have the shape of the spike's ``results.json`` (``experiments.finish`` and
``loadgen.verify``); the tracked file is not read here.
"""

from __future__ import annotations

import functools
import importlib.util
import socket
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
STEPS_PER_DAY = 96
HOUSEHOLDS = 50
STRATEGIES = ["NoBattery", "SelfConsumption", "PriceAware"]
FULL_DAY_EVENTS = HOUSEHOLDS * len(STRATEGIES) * STEPS_PER_DAY  # 14400
CTL_LOG = """\
2026-11-15T18:58:00Z wipe container=ems-spike-restate
2026-11-15T18:58:02Z server container=ems-spike-restate
2026-11-15T18:58:09Z svc1 pid=4101
2026-11-15T18:58:12Z register url=http://host.docker.internal:9080
2026-11-15T19:00:00Z kill-svc1 pids=4101,4102
2026-11-15T19:00:04Z svc1 pid=4188
2026-11-15T19:01:00Z kill-server container=ems-spike-restate exit=137
2026-11-15T19:01:04Z server container=ems-spike-restate
2026-11-15T19:03:00Z svc2 pid=4270
2026-11-15T19:03:02Z register url=http://host.docker.internal:9081
"""


@functools.cache
def _job() -> ModuleType:
    spec = importlib.util.spec_from_file_location("spike_repro", REPO / "jobs" / "spike_repro.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _experiment(strategies: list[str], events_total: int, late_joiners: int) -> dict[str, Any]:
    """One experiment as ``experiments.finish`` writes it, with a clean verification."""
    return {
        "households": HOUSEHOLDS,
        "events": HOUSEHOLDS * STEPS_PER_DAY,
        "strategies": strategies,
        "ingest_s": 18.4,
        "end_to_end_s": 20.5,
        "events_per_s": 234.0,
        "handler_invocations": 38400,
        "invocations_per_s": 1871.9,
        "ingest_retries": 0,
        "http_errors": 0,
        "verification": {
            "checked": HOUSEHOLDS * len(strategies),
            "mismatches": 0,
            "events_total": events_total,
            "out_of_order": 0,
            "late_joiners": late_joiners,
            "meter_vs_selfconsumption_mismatch": 0,
            "examples": [],
            "ems_decisions": HOUSEHOLDS * STEPS_PER_DAY,
            "ems_stale_decisions_pct": 52.0,
            "ems_mean_lag_ms": 276.5,
            "ems_max_lag_ms": 2726.2,
        },
        "mean_daily_cost_pln": {"NoBattery": 6.958, "SelfConsumption": -0.526, "PriceAware": 2.271},
    }


def _results() -> dict[str, Any]:
    crash = _experiment(STRATEGIES, FULL_DAY_EVENTS, 0)
    crash["chaos"] = [
        ["kill -9 SDK service", 1462],
        ["kill -9 restate-server", 2910],
        ["server healthy again after s", 1.0],
    ]
    # C adds a strategy mid-stream, so its late joiners see fewer than 96 events (not judged).
    dynamic = _experiment([*STRATEGIES, "SelfConsumptionReserve"], 16098, HOUSEHOLDS)
    dynamic["steps"] = [["start + register deployment #2", 2406]]
    return {
        "A_baseline": _experiment(STRATEGIES, FULL_DAY_EVENTS, 0),
        "B_crash": crash,
        "C_dynamic_routing": dynamic,
    }


def _without(log: str, verb: str) -> str:
    return "".join(line for line in log.splitlines(keepends=True) if line.split()[1:2] != [verb])


# ── ports (AC-010-25) ─────────────────────────────────────────────────────────────────────────


def test_ac_010_25_required_ports() -> None:
    assert _job().REQUIRED_PORTS == (8080, 9070, 9080, 9081)


def test_ac_010_25_a_port_with_a_listener_is_in_use_and_free_once_closed() -> None:
    job = _job()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        port = held.getsockname()[1]
        assert job.ports_in_use([port]) == [port]
    assert job.ports_in_use([port]) == []


def test_ac_010_25_only_the_busy_ports_are_listed() -> None:
    job = _job()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        busy = held.getsockname()[1]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as released:
            released.bind(("127.0.0.1", 0))
            free = released.getsockname()[1]
        assert job.ports_in_use([free, busy]) == [busy]


# ── verdict (AC-010-24) ───────────────────────────────────────────────────────────────────────


def test_ac_010_24_passing_run_gives_no_reason() -> None:
    assert _job().verdict(_results(), CTL_LOG) == []


@pytest.mark.parametrize("missing", ["A_baseline", "B_crash", "C_dynamic_routing"])
def test_ac_010_24_missing_experiment_fails_with_a_reason_naming_it(missing: str) -> None:
    results = _results()
    del results[missing]
    reasons = _job().verdict(results, CTL_LOG)
    assert any(missing in reason for reason in reasons), reasons


@pytest.mark.parametrize(
    ("experiment", "field", "value"),
    [
        ("A_baseline", "mismatches", 1),
        ("A_baseline", "meter_vs_selfconsumption_mismatch", 1),
        ("A_baseline", "out_of_order", 1),
        ("C_dynamic_routing", "mismatches", 1),
        ("C_dynamic_routing", "meter_vs_selfconsumption_mismatch", 1),
        ("C_dynamic_routing", "out_of_order", 1),
        ("B_crash", "mismatches", 1),
        ("B_crash", "out_of_order", 1),
        ("B_crash", "events_total", FULL_DAY_EVENTS - 1),  # an event lost
        ("B_crash", "events_total", FULL_DAY_EVENTS + 1),  # an event doubled
    ],
)
def test_ac_010_24_a_failed_check_fails_the_run(experiment: str, field: str, value: int) -> None:
    results = _results()
    results[experiment]["verification"][field] = value
    assert _job().verdict(results, CTL_LOG) != []


def test_ac_010_24_b_needs_96_events_per_household_and_strategy() -> None:
    """The expected total follows B's own ``households`` and ``strategies``."""
    results = _results()
    results["B_crash"]["households"] = 20
    assert _job().verdict(results, CTL_LOG) != []  # 14400 events where 20 * 3 * 96 = 5760 belong
    results["B_crash"]["verification"]["events_total"] = 20 * len(STRATEGIES) * STEPS_PER_DAY
    assert _job().verdict(results, CTL_LOG) == []


@pytest.mark.parametrize(
    "ctl_log",
    [
        pytest.param(_without(CTL_LOG, "kill-svc1"), id="no-kill-svc1"),
        pytest.param(_without(CTL_LOG, "kill-server"), id="no-kill-server"),
        pytest.param(CTL_LOG.replace("pids=4101,4102", "pids="), id="empty-pids"),
        pytest.param(CTL_LOG.replace(" exit=137", " exit=0"), id="server-exit-0"),
        pytest.param(
            CTL_LOG.replace("Z kill-svc1 ", "Z svc1 note=kill-svc1 "), id="kill-svc1-not-the-verb"
        ),
        pytest.param("", id="empty-log"),
    ],
)
def test_ac_010_24_verb_log_must_show_that_b_killed_the_service_and_the_server(
    ctl_log: str,
) -> None:
    assert _job().verdict(_results(), ctl_log) != []
