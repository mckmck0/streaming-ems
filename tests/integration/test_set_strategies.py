"""AC-012-12 (T41): `Household.set_strategies` through the HTTP ingress, with the runner's
Idempotency-Key ``<key>:set_strategies:<t>:<sorted names joined by +>``.

One managed household (PL-LOC-000001, sim mode, one day from 2026-10-20) is configured with
``[SelfConsumption]``; nothing is published, so its next unreleased step is ``first_step``. Then:
- the change "from ``first_step + 8``, NoBattery and SelfConsumption" is sent twice with the same
  key: Restate answers the second send with the first invocation, and the household stores one
  change;
- a change with the same ``t`` and another set (so another key) is refused ``not_after_pending``:
  stored as a refusal with its reason, and the schedule's changes stay as they were.
"""

from __future__ import annotations

from urllib.parse import quote

import httpx

from ems.actors.household import HouseholdState
from ems.actors.messages import StrategyChange
from ems.actors.serde import serde_for
from ems.assembler.schedule import Change, Refusal
from ems.experiments import stack
from ems.experiments.runs import managed_params, pse_table
from tests.stack_fixtures import decoded, utc

START, DAYS = "2026-10-20T00:00:00Z", 1.0
LOCATION = "PL-LOC-000001"
TIMEOUT_S = 120.0


def _idempotency_key(key: str, change: StrategyChange) -> str:
    return f"{key}:set_strategies:{change.t}:{'+'.join(sorted(change.strategies))}"


def _send(env: stack.StackEnv, key: str, change: StrategyChange) -> dict[str, str]:
    """A one-way send to ``set_strategies`` with the runner's key; Restate's answer."""
    url = f"{env.settings.restate_ingress_url.rstrip('/')}/Household/{quote(key, safe='')}"
    resp = httpx.post(
        f"{url}/set_strategies/send",
        content=serde_for(StrategyChange).serialize(change),
        headers={
            "content-type": "application/json",
            "idempotency-key": _idempotency_key(key, change),
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    return dict(resp.json())


def test_ac_012_12_one_change_per_key_and_a_refusal_for_the_same_t(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    params = managed_params(
        run_id=run_id,
        start=utc(START),
        days=DAYS,
        locations=1,
        rcem=pse_table(),
        strategies=("SelfConsumption",),
        live="SelfConsumption",
    )
    p = params[f"{run_id}:{LOCATION}"]
    key = p.household_key
    stack_env.restate.configure({key: p})
    stack.wait_for(
        lambda: key in decoded(stack_env, run_id, "Household", HouseholdState),
        TIMEOUT_S,
        "the household was not configured",
        poll=0.25,
    )

    t = p.first_step + 8
    add = StrategyChange(t, ("NoBattery", "SelfConsumption"))
    first = _send(stack_env, key, add)
    again = _send(stack_env, key, add)
    assert again["invocationId"] == first["invocationId"], (first, again)
    late = StrategyChange(t, ("SelfConsumption",))
    _send(stack_env, key, late)

    def refused() -> bool:
        h = decoded(stack_env, run_id, "Household", HouseholdState)[key]
        return len(h.schedule.refused) >= 1

    stack.wait_for(refused, TIMEOUT_S, "the second key was not handled", poll=0.25)
    h = decoded(stack_env, run_id, "Household", HouseholdState)[key]
    assert h.schedule.changes == (Change(t, ("NoBattery", "SelfConsumption")),), h.schedule
    assert h.schedule.refused == (Refusal(t, ("SelfConsumption",), "not_after_pending"),)
    assert h.join.next == p.first_step  # nothing else changed

    rows = stack_env.restate.query(
        "SELECT id, idempotency_key, status FROM sys_invocation "
        "WHERE target_service_name = 'Household' AND target_handler_name = 'set_strategies' "
        f"AND target_service_key = '{key}'"
    )
    assert sorted(r["idempotency_key"] for r in rows) == sorted(
        [_idempotency_key(key, add), _idempotency_key(key, late)]
    ), rows
    assert stack_env.restate.failures(run_id) == []
