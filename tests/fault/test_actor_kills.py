"""AC-011-43, AC-012-33, -38: kill -9 of the SDK service, and of the Restate server, during a
configured backfill, after the first step and before the last; once it is started again the actors
end equal to the offline side.

30 households over 2 days from 2026-10-20 (about 9 managed sites, 10,900 records; plan, "Fault-tier
run"), configured before anything is published. The backfill is published while the module's
ingress containers are stopped, so the kill lands while records are still flowing. The kill comes
once 30 % of the records are handled. Restate retries the interrupted invocations against their
journals (A11: their bytes must not differ), so the wait is lenient about recorded failed attempts,
never about failed or paused invocations.

SDK service (AC-011-43): from data that survives the kill (read while nothing can change it) the
test proves the kill landed inside the run: fewer records handled than published, and a managed
strategy whose next step lies strictly between `first_step` and its offline end.

Server (AC-012-33): nothing can be read while the server is down, and steps only move forward, so
the proof is split around the kill. Just before it, a managed strategy already stands past its
first step; once the server is healthy again, fewer records are handled than published and a
managed strategy still stands before its offline end. From the kill until the server is healthy a
thread polls `/query` and records every answer (HTTP status, or the transport error) with its time
since the kill, in `logs/tests/server-kill/<run_id>.json`: the harness retries any 5xx for
``KILL_WINDOW_S`` after a kill it caused (AC-012-38), and this is what that window rests on.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from ems.assembler.params import RunParams
from ems.experiments import stack
from tests.stack_fixtures import LOGS, assert_run, configure_run, dump_on_failure, finish_run

HOUSEHOLDS, DAYS = 30, 2
START = "2026-10-20T00:00:00Z"
TRIGGER = 0.3
POLL_S = 0.2
PROBE_SQL = "SELECT service_name FROM state LIMIT 1"


def _publish_paused(env: stack.StackEnv, run_id: str) -> list[stack.Record]:
    env.ingress.pause_consumption()
    try:
        return env.publish(run_id, locations=HOUSEHOLDS, days=DAYS, start=START)
    finally:
        env.ingress.resume_consumption()


def _steps(env: stack.StackEnv, run_id: str, params: Mapping[str, RunParams]) -> list[Any]:
    """(service, key, next step) of every managed strategy object of the run."""
    strategies = sorted({name for p in params.values() for name in p.strategies})
    return [
        (service, key, step)
        for service in strategies
        for key, values in env.restate.states(run_id, service).items()
        if key in params and "state" in values
        for step in [stack.next_step(service, json.loads(values["state"]))]
        if step is not None
    ]


def test_ac_011_43_sdk_service_kill_during_a_configured_run(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    params = configure_run(
        stack_env, run_id, start=START, days=DAYS, locations=HOUSEHOLDS, late_slots=8
    )
    records = _publish_paused(stack_env, run_id)
    published = len(records)

    with dump_on_failure(run_id, params, records):
        stack.wait_for(
            lambda: stack_env.restate.handled(run_id) >= TRIGGER * published,
            600,
            "trigger not reached",
        )
        stack_env.sdk.kill9()  # every process of the group; returns once nothing listens on 9080
        try:
            # The kill landed inside the run (state read while nothing can change it): records still
            # to handle, and a managed strategy mid-way to the end the offline side computes.
            assert stack_env.restate.handled(run_id) < published, (
                "the kill came after the last record"
            )
            ends = stack.run_ends(params, stack.offline_run(params, records))
            midway = [
                (service, key, step)
                for service, key, step in _steps(stack_env, run_id, params)
                if params[key].first_step < step < ends[service][key]
            ]
            assert midway, "no managed strategy stood between its first step and its end"
        finally:
            stack_env.sdk.start()
    run = finish_run(stack_env, run_id, params, records, strict=False, timeout=1800)
    assert_run(stack_env, run, strict=False)


class QueryProbe(threading.Thread):
    """Polls the admin `/query` endpoint until stopped; every answer with its monotonic time (the
    HTTP status, or the transport error's class) and, for an HTTP error, the start of its body."""

    def __init__(self, admin_url: str) -> None:
        super().__init__(daemon=True)
        self.url = f"{admin_url.rstrip('/')}/query"
        self.seen: list[tuple[float, str, str]] = []
        self._stop_event = threading.Event()

    def run(self) -> None:
        with httpx.Client(timeout=2.0) as client:
            while not self._stop_event.is_set():
                at = time.monotonic()
                try:
                    resp = client.post(
                        self.url,
                        json={"query": PROBE_SQL},
                        headers={"accept": "application/json"},
                    )
                    body = "" if resp.is_success else resp.text[:300]
                    self.seen.append((at, str(resp.status_code), body))
                except httpx.HTTPError as exc:
                    self.seen.append((at, type(exc).__name__, ""))
                self._stop_event.wait(POLL_S)

    def answered_after(self, since: float) -> float | None:
        """The time of the first 200 that follows a failed answer after ``since``."""
        failed = False
        for at, what, _ in list(self.seen):
            if at < since:
                continue
            if what != "200":
                failed = True
            elif failed:
                return at
        return None

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=10)


def _record_statuses(
    run_id: str,
    killed_at: float,
    healthy_after_s: float,
    query_after_s: float,
    seen: list[tuple[float, str, str]],
) -> dict[str, Any]:
    answers = [(round(at - killed_at, 3), what) for at, what, _ in seen if at >= killed_at]
    counts: dict[str, int] = {}
    for _, what in answers:
        counts[what] = counts.get(what, 0) + 1
    bodies = sorted({f"{what}: {body}" for at, what, body in seen if at >= killed_at and body})
    record = {
        "run_id": run_id,
        "probe_sql": PROBE_SQL,
        "poll_s": POLL_S,
        "kill_window_s": stack.KILL_WINDOW_S,
        "healthy_after_s": round(healthy_after_s, 3),  # admin /health and ingress health both 200
        "query_after_s": round(query_after_s, 3),  # the first 200 from /query after the kill
        "counts": counts,
        "error_bodies": bodies,
        "answers": answers,
    }
    out = LOGS / "server-kill" / f"{run_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=1) + "\n")
    return record


def _retrying[T](restate: stack.Restate, read: Callable[[], T], timeout: float = 120.0) -> T:
    """A read as the harness's waits make it: transient errors (AC-012-38) retried."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return read()
        except httpx.HTTPError as exc:
            if not restate.transient(exc) or time.monotonic() > deadline:
                raise
        time.sleep(POLL_S)


def test_ac_012_33_restate_server_kill_during_a_configured_run(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    env = stack_env
    params = configure_run(env, run_id, start=START, days=DAYS, locations=HOUSEHOLDS, late_slots=8)
    records = _publish_paused(env, run_id)
    published = len(records)

    with dump_on_failure(run_id, params, records):
        ends = stack.run_ends(params, stack.offline_run(params, records))
        stack.wait_for(
            lambda: env.restate.handled(run_id) >= TRIGGER * published, 600, "trigger not reached"
        )
        started = [(s, k, t) for s, k, t in _steps(env, run_id, params) if params[k].first_step < t]
        assert started, "no managed strategy past its first step before the kill"

        probe = QueryProbe(env.restate.admin)
        probe.start()
        killed_at = time.monotonic()
        healthy_after_s = query_after_s = float("nan")
        try:
            assert stack.kill_restate(restate=env.restate) == 137
        finally:
            try:
                stack.start_restate(env.restate)  # returns once admin and ingress answer healthy
                healthy_after_s = time.monotonic() - killed_at
                # Health comes before /query answers: keep polling until it gives a 200 again.
                stack.wait_for(
                    lambda: probe.answered_after(killed_at) is not None,
                    stack.KILL_WINDOW_S * 3,
                    "/query never answered 200 after the restart",
                    poll=POLL_S,
                )
                query_after_s = (probe.answered_after(killed_at) or killed_at) - killed_at
            finally:
                probe.stop()
                record = _record_statuses(
                    run_id, killed_at, healthy_after_s, query_after_s, probe.seen
                )

        # The kill came before the last step: still records to handle and a strategy short of its
        # end once the server is back (both only move forward, so both held at the kill).
        handled = _retrying(env.restate, lambda: env.restate.handled(run_id))
        assert handled < published, "the kill came after the last record"
        steps = _retrying(env.restate, lambda: _steps(env, run_id, params))
        short = [(s, k, t) for s, k, t in steps if t < ends[s][k]]
        assert short, "every managed strategy had reached its end before the kill"

        # AC-012-38: what /query answered from the kill until it gave a 200 again. A 5xx other than
        # 503 is retried only inside the kill window, so every such answer must fall inside it, and
        # no answer is a 4xx.
        assert record["answers"], record
        outside = [
            (at, what)
            for at, what in record["answers"]
            if what.isdigit() and int(what) >= 500 and what != "503" and at > stack.KILL_WINDOW_S
        ]
        assert outside == [], record["counts"]
        client_errors = [
            (at, w) for at, w in record["answers"] if w.isdigit() and 400 <= int(w) < 500
        ]
        assert client_errors == [], record["counts"]

    run = finish_run(env, run_id, params, records, strict=False, timeout=1800)
    assert_run(env, run, strict=False)
