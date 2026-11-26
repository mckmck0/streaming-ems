"""AC-012-10, -11, -35 (T40): the live deadline chain on the real server.

Each test configures one live household (PL-LOC-000001, with PV) whose window starts in the past.
Its prices are published first: a backfill of the window goes into the module's topics while the
ingress containers are stopped, then only `ingress-prices` is started, so `Tariff` holds the curves
before the household subscribes and the snapshot starts the chain (AC-012-07). The meter and
forecast containers stay stopped: no reading reaches the household, and every step it releases is
released by the clock (closed with its estimate), never by data.

A live run cannot avoid the wall clock: the windows are placed relative to the time the test runs,
and the expected values come from the wall time read just before configuring and just after the
state was observed (the journaled time of a firing lies between them), never from the transition.

- ``first deadline``: ``G`` = 60 s, ``first_step`` the UTC hour two hours back, ``end_step`` four
  quarters after the current one. The first firing releases every step that ended at least ``G``
  ago (``join.next`` = ⌊(now - G)/900⌋, within the clock step of the two bounding wall times);
  ``sys_invocation`` lists exactly one `scheduled` `Household/on_deadline` for the key, with
  ``scheduled_start_at`` within 2 s of ``900·(next) + G + 1`` (AC-012-35); a `Deadline` sent by
  hand for another step is counted as stale and changes nothing else (AC-012-08).
- ``kill -9``: ``G`` is chosen so that the deadline after the first falls about 75 s after
  configuring. The SDK service is killed about 20 s before it is due and started again about 5 s
  after, so Restate delivers it to a service that was down at its due time. It fires, exactly one
  deadline is pending again, and no invocation of the run failed or reported RT0016 (a journal
  mismatch on a retried deadline, AC-012-10, -11).
- ``short window``: ``end_step`` already more than ``G`` in the past. The first firing releases the
  whole window and stops the chain: ``clock.stopped``, nothing scheduled, one firing.
"""

from __future__ import annotations

import math
import time
from datetime import UTC, datetime
from typing import Any

from ems.actors.household import HouseholdState
from ems.actors.messages import Deadline
from ems.actors.serde import serde_for
from ems.assembler.params import RunParams
from ems.experiments import stack
from ems.experiments.runs import managed_params, pse_table
from tests.stack_fixtures import decoded

LOCATION = "PL-LOC-000001"
QUARTER_S, HOUR_STEPS = 900, 4
MARGIN_S = 1.0  # the household fires a deadline this long after its moment
SCHEDULE_TOLERANCE_S = 2.0  # AC-012-35
TIMEOUT_S = 180.0


def _clock_step(unix_s: float, grace_s: int) -> int:
    return math.floor((unix_s - grace_s) / QUARTER_S)


def _live_params(run_id: str, first: int, end: int, grace_s: int) -> RunParams:
    start = datetime.fromtimestamp(first * QUARTER_S, UTC)
    params = managed_params(
        run_id=run_id,
        start=start,
        days=(end - first) / 96,
        locations=1,
        rcem=pse_table(),
        mode="live",
        grace_s=grace_s,
    )
    p = params[f"{run_id}:{LOCATION}"]
    assert (p.first_step, p.end_step, p.mode, p.grace_s) == (first, end, "live", grace_s)
    return p


def _publish_prices(env: stack.StackEnv, run_id: str, p: RunParams) -> None:
    """Backfill the window with every ingress container stopped, then start only the price one and
    wait until `Tariff` has handled every price record of the run."""
    env.ingress.pause_consumption()
    start = datetime.fromtimestamp(p.first_step * QUARTER_S, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    records = env.publish(run_id, locations=1, days=(p.end_step - p.first_step) / 96, start=start)
    prices = sum(stack.service_of(r.meta[0]) == "Tariff" for r in records)
    assert prices > 0, "no price record in the window"
    env.ingress.start("ingress-prices")
    env.restate.wait_handled(run_id, prices, timeout=TIMEOUT_S, services=("Tariff",))


def _household(env: stack.StackEnv, run_id: str, key: str) -> HouseholdState | None:
    return decoded(env, run_id, "Household", HouseholdState).get(key)


def _wait_household(env: stack.StackEnv, run_id: str, key: str, ok: Any, what: str) -> Any:
    found: list[HouseholdState] = []

    def check() -> bool:
        h = _household(env, run_id, key)
        if h is not None and ok(h):
            found[:] = [h]
            return True
        return False

    stack.wait_for(check, TIMEOUT_S, f"{key}: {what}", poll=0.25)
    return found[0]


def _deadlines(env: stack.StackEnv, key: str, status: str | None = None) -> list[dict[str, Any]]:
    where = (
        "target_service_name = 'Household' AND target_handler_name = 'on_deadline' "
        f"AND target_service_key = '{key}'"
    )
    if status is not None:
        where += f" AND status = '{status}'"
    # Restate 1.8.1 fills `scheduled_start_at` only when `scheduled_at` is selected with it (seen
    # on the pinned server: selected alone, or with other columns, it comes back null).
    return env.restate.query(
        "SELECT id, status, scheduled_at, scheduled_start_at, completion_result, last_failure, "
        f"last_failure_error_code, retry_count FROM sys_invocation WHERE {where}"
    )


def _unix(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _pending(env: stack.StackEnv, run_id: str, key: str) -> tuple[HouseholdState, list[Any]]:
    """The household and its scheduled deadlines, read so that no firing came in between."""
    while True:
        before = _household(env, run_id, key)
        rows = _deadlines(env, key, "scheduled")
        after = _household(env, run_id, key)
        if before is not None and before == after:
            return after, rows


def _assert_one_pending(env: stack.StackEnv, run_id: str, key: str, p: RunParams) -> HouseholdState:
    h, rows = _pending(env, run_id, key)
    assert h.clock.expected is not None and not h.clock.stopped, h.clock
    assert len(rows) == 1, rows
    due = QUARTER_S * h.clock.expected + p.grace_s + MARGIN_S
    assert abs(_unix(rows[0]["scheduled_start_at"]) - due) <= SCHEDULE_TOLERANCE_S, (rows, due)
    return h


def test_ac_012_35_first_deadline_releases_due_steps_and_schedules_the_next(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    grace = 60
    now = time.time()
    first = (math.floor(now / QUARTER_S) // HOUR_STEPS) * HOUR_STEPS - 2 * HOUR_STEPS
    end = math.floor(now / QUARTER_S) + 4
    p = _live_params(run_id, first, end, grace)
    key = p.household_key
    _publish_prices(stack_env, run_id, p)

    before = time.time()
    stack_env.restate.configure({key: p})
    h = _wait_household(stack_env, run_id, key, lambda h: h.clock.firings >= 1, "no deadline fired")
    after = time.time()

    # Every step that ended at least G ago is released, the ones after are not.
    assert _clock_step(before, grace) <= h.join.next <= _clock_step(after, grace), (
        h.join.next,
        _clock_step(before, grace),
    )
    assert h.join.next > first + 2 * HOUR_STEPS - 2, h.join.next  # about two hours of steps
    h = _assert_one_pending(stack_env, run_id, key, p)
    assert h.clock.expected == h.join.next + 1, h.clock
    assert h.clock.stale == 0, h.clock

    # A deadline for another step: counted, nothing else changes, still one pending.
    other = h.clock.expected + 5
    body = serde_for(Deadline).serialize(Deadline(other))
    stack_env.restate.send("Household", key, "on_deadline", body)
    stale = _wait_household(
        stack_env, run_id, key, lambda x: x.clock.stale == 1, "the stale deadline was not counted"
    )
    if stale.clock.firings == h.clock.firings:  # no firing in between: nothing else moved
        assert stale.join == h.join, "a stale deadline changed the join"
        assert stale.clock.expected == h.clock.expected
    _assert_one_pending(stack_env, run_id, key, p)
    assert stack_env.restate.failures(run_id) == []


def test_ac_012_10_pending_deadline_fires_after_an_sdk_kill(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    now = time.time()
    first = (math.floor(now / QUARTER_S) // HOUR_STEPS) * HOUR_STEPS - 2 * HOUR_STEPS
    # The prices are published before G is fixed, so the delay is counted from configuring.
    probe = _live_params(run_id, first, math.floor(now / QUARTER_S) + 4, 60)
    _publish_prices(stack_env, run_id, probe)

    due = time.time() + 75.0  # the deadline after the first: 900·m + G + 1 = due
    grace = math.floor(due - MARGIN_S) % QUARTER_S
    m = (math.floor(due - MARGIN_S) - grace) // QUARTER_S
    due = QUARTER_S * m + grace + MARGIN_S
    p = _live_params(run_id, first, m + 2, grace)
    key = p.household_key
    stack_env.restate.configure({key: p})
    h = _wait_household(
        stack_env, run_id, key, lambda h: h.clock.expected == m, "no first firing set for m"
    )
    assert h.join.next == m - 1, (h.join.next, m)
    _assert_one_pending(stack_env, run_id, key, p)
    assert time.time() < due - 10, "the first firing came too late to kill before the next"

    stack.wait_for(lambda: time.time() >= due - 20, 60, "never near the deadline", poll=0.2)
    stack_env.sdk.kill9()
    try:
        assert len(_deadlines(stack_env, key, "scheduled")) == 1  # held by the server meanwhile
        stack.wait_for(lambda: time.time() >= due + 5, 60, "never past the deadline", poll=0.2)
    finally:
        stack_env.sdk.start()

    fired = _wait_household(
        stack_env,
        run_id,
        key,
        lambda h: h.clock.expected == m + 1,
        "the pending deadline did not fire",
    )
    assert fired.join.next == m, (fired.join.next, m)
    _assert_one_pending(stack_env, run_id, key, p)
    rows = _deadlines(stack_env, key)
    assert [r for r in rows if r["status"] == "completed"], rows
    assert stack_env.restate.failures(run_id, strict=False) == []
    everything = stack_env.restate.query(
        "SELECT id, target_service_name, target_handler_name, status, last_failure, "
        "last_failure_error_code FROM sys_invocation "
        f"WHERE starts_with(target_service_key, '{run_id}:')"
    )
    rt0016 = [
        r
        for r in everything
        if "RT0016" in str(r.get("last_failure_error_code")) + str(r.get("last_failure"))
    ]
    assert rt0016 == [], rt0016


def test_ac_012_35_short_window_stops_the_chain_at_end_step(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    grace = 60
    hour = (math.floor(time.time() / QUARTER_S) // HOUR_STEPS) * HOUR_STEPS
    first, end = hour - 3 * HOUR_STEPS, hour - HOUR_STEPS  # ended an hour or more ago
    p = _live_params(run_id, first, end, grace)
    key = p.household_key
    _publish_prices(stack_env, run_id, p)

    stack_env.restate.configure({key: p})
    h = _wait_household(stack_env, run_id, key, lambda h: h.clock.stopped, "the chain did not stop")
    assert h.join.next == end, (h.join.next, end)
    assert (h.clock.expected, h.clock.firings, h.clock.stale) == (None, 1, 0), h.clock
    assert _deadlines(stack_env, key, "scheduled") == []
    assert len(_deadlines(stack_env, key)) == 1, _deadlines(stack_env, key)
    assert stack_env.restate.failures(run_id) == []
