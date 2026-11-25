"""AC-012-18: Restate's journal retention comes from `EMS_RESTATE_JOURNAL_RETENTION` (`1d` unset).

The test configures one managed household (PL-LOC-000001) twice, each time under a new run id:
first on the stack as it runs (the variable unset), then after the restate container has been
recreated with the variable at `0`. Configuring causes the keyed `configure` calls the harness sends
(one per object, each with an idempotency key) and key-less internal invocations: `Household`
sends `Tariff.subscribe`, and `Tariff` answers with the price snapshot (`Household.on_prices`).

`sys_invocation` reports a completed invocation's `journal_retention` (and, apart from it,
`completion_retention`) as an ISO 8601 duration (`PT86400S`, `P0D`). With the default, every
completed invocation of the run reports one day; at `0`, every one still shown reports 0. What
each setting shows is written to `logs/tests/retention/<run_id>.json`: which internal key-less
invocations are still listed at `0`, and their retentions, beside the keyed ones.

The SDK service is registered once, before the restart, and not again: a new invocation of a service
registered before the restart must get the new default too. The default is restored in `finally`
(restate recreated with the variable unset, then healthy), so later modules run on the stack as
`make up` leaves it.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from ems.actors.household import HouseholdState
from ems.experiments import stack
from tests.stack_fixtures import LOGS, configure_run, decoded

RETENTION_ENV = "EMS_RESTATE_JOURNAL_RETENTION"
START = "2026-10-20T00:00:00Z"
DAYS = 0.25
LOCATIONS = 1  # PL-LOC-000001, managed, with PV
DAY_S = 86400.0
TIMEOUT_S = 120.0
INTERNAL = (("Tariff", "subscribe"), ("Household", "on_prices"))  # key-less, invoked by a service
COLUMNS = (
    "id, target_service_name, target_service_key, target_handler_name, status, invoked_by, "
    "idempotency_key, completion_result, journal_retention, completion_retention, journal_size"
)
_DURATION = re.compile(
    r"^P(?:(?P<d>\d+)D)?(?:T(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+(?:\.\d+)?)S)?)?$"
)


def seconds(duration: str | None) -> float | None:
    """An ISO 8601 duration as DataFusion prints it (``PT86400S``, ``P0D``, ``PT1.5S``)."""
    if duration is None:
        return None
    m = _DURATION.match(duration)
    if m is None or duration in ("P", "PT"):
        raise ValueError(f"not an ISO 8601 duration: {duration!r}")
    d, h, mi, s = (m.group(g) for g in "dhms")
    return int(d or 0) * 86400.0 + int(h or 0) * 3600.0 + int(mi or 0) * 60.0 + float(s or 0)


def _rows(restate: stack.Restate, run_id: str) -> list[dict[str, Any]]:
    return restate.query(
        f"SELECT {COLUMNS} FROM sys_invocation WHERE starts_with(target_service_key, '{run_id}:')"
    )


def _configure_and_read(env: stack.StackEnv, run_id: str) -> tuple[int, list[dict[str, Any]]]:
    """Configure the household; once every object stores its parameters, the household has
    applied the Tariff's snapshot and nothing of the run is still running, read the run's
    invocations. Returns the number of keyed `configure` calls sent, and the rows."""
    params = configure_run(env, run_id, start=START, days=DAYS, locations=LOCATIONS)
    ((key, p),) = params.items()
    services = (*stack.CONFIGURED, *p.strategies)

    def configured() -> bool:
        return all("params" in env.restate.states(run_id, s).get(key, {}) for s in services)

    def snapshot_applied() -> bool:
        household = decoded(env, run_id, "Household", HouseholdState).get(key)
        return household is not None and household.prices.expected is not None

    stack.wait_for(configured, TIMEOUT_S, f"{run_id}: not every object configured", poll=0.25)
    stack.wait_for(snapshot_applied, TIMEOUT_S, f"{run_id}: no price snapshot applied", poll=0.25)
    stack.wait_for(
        lambda: all(r["status"] == "completed" for r in _rows(env.restate, run_id)),
        TIMEOUT_S,
        f"{run_id}: invocations still running",
        poll=0.25,
    )
    return len(services), _rows(env.restate, run_id)


def _summary(run_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    def short(r: Mapping[str, Any]) -> dict[str, Any]:
        keys = ("target_service_name", "target_handler_name", "status", "invoked_by")
        keys += ("idempotency_key", "journal_retention", "completion_retention", "journal_size")
        return {k: r.get(k) for k in keys}

    internal = {
        f"{service}.{handler}": [
            short(r)
            for r in rows
            if (r["target_service_name"], r["target_handler_name"]) == (service, handler)
            and not r.get("idempotency_key")
        ]
        for service, handler in INTERNAL
    }
    return {
        "run_id": run_id,
        "keyed": [short(r) for r in rows if r.get("idempotency_key")],
        "keyless_internal": {
            name: {"present": bool(found), "rows": found} for name, found in internal.items()
        },
        "other": [
            short(r)
            for r in rows
            if not r.get("idempotency_key")
            and (r["target_service_name"], r["target_handler_name"]) not in INTERNAL
        ],
    }


def _recreate_restate(restate: stack.Restate, env: Mapping[str, str] | None) -> None:
    """`docker compose up -d --wait restate` on the stack's project: compose recreates the container
    when its configuration changed (here, the retention variable), keeping the data volume."""
    stack.compose(
        "up", "-d", "--wait", "--no-deps", "restate", project=stack.STACK_PROJECT, env=env
    )
    restate.wait_healthy()


def _completed(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in rows if r["status"] == "completed"]


def test_ac_012_18_journal_retention_one_day_by_default_and_zero_when_set(
    stack_env: stack.StackEnv, run_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Unset in this process, so compose sees it unset unless a call passes it.
    monkeypatch.delenv(RETENTION_ENV, raising=False)
    restate = stack_env.restate
    record: dict[str, Any] = {}
    out = LOGS / "retention" / f"{run_id}.json"
    try:
        # The stack as `make up` starts it: the variable unset, so `1d`.
        sent, rows = _configure_and_read(stack_env, run_id)
        record["default"] = _summary(run_id, rows)
        keyed = [r for r in _completed(rows) if r.get("idempotency_key")]
        assert len(keyed) == sent, rows
        retentions = {seconds(r["journal_retention"]) for r in _completed(rows)}
        assert retentions == {DAY_S}, rows

        # Restate recreated with `0`; the deployment registered before stays as it is.
        _recreate_restate(restate, {RETENTION_ENV: "0"})
        zero_run = f"{run_id}z"
        sent, rows = _configure_and_read(stack_env, zero_run)
        record["zero"] = _summary(zero_run, rows)
        keyed = [r for r in _completed(rows) if r.get("idempotency_key")]
        assert len(keyed) == sent, rows  # a keyed call keeps its completion for its idempotency
        retentions = {seconds(r["journal_retention"]) for r in _completed(rows)}
        assert retentions == {0.0}, rows
    finally:
        _write(out, record)
        _recreate_restate(restate, None)  # the variable unset again: back to `1d`


def _write(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=1, sort_keys=True) + "\n")
