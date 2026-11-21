"""Fixtures shared by the tiers that run against the compose stack (integration, e2e, fault; spec
010).

Each tier's conftest imports what it needs from here. The Restate fixtures wrap the harness in
`ems.experiments.stack`: every module gets its own topic prefix, SDK service and ingress containers,
and every test its own run_id, so no test reads data an earlier run left behind and none writes to
the stack's own topics.

Spec 011 (plan §7) adds the configured runs of the e2e and fault tiers: ``actor_run`` builds the
parameters of the managed households (``managed_params``), configures them, publishes, folds the
records offline, waits until the actors stand where the offline side ends (``wait_run``) and
compares (``assert_run``, AC-011-38 … -42). A failed wait or comparison leaves the run's records
(key, topic, partition, offset, value) and parameters as JSON under ``logs/tests/<run_id>/``.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from ems.actors.battery import BatteryState
from ems.actors.household import HouseholdState
from ems.actors.meter import MeterState
from ems.actors.serde import serde_for
from ems.actors.strategy import StrategyState
from ems.assembler.offline import Offline
from ems.assembler.params import RunParams
from ems.experiments import stack
from ems.experiments.runs import CONFIG, managed_params
from ems.ingest.kafka.config import base_config
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[1]
LOGS = REPO / "logs" / "tests"
FAULTS = REPO / "tests" / "fixtures" / "generator.faults.yaml"
UNMANAGED_STATE_KEYS = {"log", "n"}
UNMANAGED_MAX_BYTES = 1000  # AC-011-28: under 1 kB in total, whatever the number of records


@pytest.fixture(scope="session")
def bootstrap() -> str:
    s = EmsSettings()
    try:
        AdminClient(base_config(s.kafka_bootstrap)).list_topics(timeout=10)
    except Exception as exc:  # broker missing: these tests need real services
        pytest.fail(f"Kafka not reachable at {s.kafka_bootstrap} ({exc}); run `make up` first")
    return s.kafka_bootstrap


@pytest.fixture(scope="session", autouse=True)
def restate_session() -> Iterator[None]:
    """AC-010-26: stop before the first test if the SDK port is taken; clear stale test projects."""
    if stack.port_in_use(stack.SDK_PORT):
        pytest.exit(
            f"port {stack.SDK_PORT} is in use (is `make dev` running?): the Restate tiers start "
            "their own SDK service there. Stop it and run again.",
            returncode=3,
        )
    stack.remove_test_projects()
    yield


def new_prefix(tag: str = "it") -> str:
    return f"{tag}{uuid.uuid4().hex[:10]}."


@pytest.fixture(scope="module")
def stack_env(bootstrap: str) -> Iterator[stack.StackEnv]:
    """A module's environment: own topics, SDK service and ingress containers; ingestion of the
    stack closed.
    """
    env = stack.StackEnv(new_prefix(), log_dir=LOGS)
    try:
        env.open()
        yield env
    finally:
        env.close()


@pytest.fixture
def run_id() -> str:
    return f"r{uuid.uuid4().hex[:10]}"


# ── configured runs (spec 011) ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ActorRun:
    """A configured run: its parameters, its records as read back from Kafka, the offline side."""

    run_id: str
    params: dict[str, RunParams]
    records: list[stack.Record]
    offline: dict[str, Offline]


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def configure_run(
    env: stack.StackEnv,
    run_id: str,
    *,
    start: str,
    days: float,
    locations: int,
    late_slots: int = 8,
    generator_config: Path | None = None,
) -> dict[str, RunParams]:
    """The managed households' parameters, sent to their objects before anything is published."""
    params = managed_params(
        run_id=run_id,
        start=utc(start),
        days=days,
        locations=locations,
        late_slots=late_slots,
        generator_config=generator_config or CONFIG / "generator.yaml",
    )
    assert params, "the catalog has no EMS-managed site in this window"
    env.restate.configure(params)
    return params


def publish_run(
    env: stack.StackEnv,
    run_id: str,
    *,
    start: str,
    days: float,
    locations: int,
    faults: Path | None = None,
) -> list[stack.Record]:
    """A backfill of the run, with the fault configuration ``faults`` when given."""
    if faults is None:
        return env.publish(run_id, locations=locations, days=days, start=start)
    return env.publish(
        run_id, locations=locations, days=days, start=start, faults=True, config=faults
    )


def dump_run(run_id: str, params: Mapping[str, RunParams], records: list[stack.Record]) -> Path:
    """The run's records and parameters as JSON under logs/tests/<run_id>/, to replay a failure."""
    folder = LOGS / run_id
    folder.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "key": r.key,
            "topic": r.meta[0],
            "partition": r.meta[1],
            "offset": r.meta[2],
            "value": r.value.decode("utf-8", errors="backslashreplace"),
        }
        for r in records
    ]
    (folder / "records.json").write_text(json.dumps(rows, indent=1) + "\n")
    serde = serde_for(RunParams)
    by_key = {key: json.loads(serde.serialize(p)) for key, p in params.items()}
    (folder / "params.json").write_text(json.dumps(by_key, indent=1) + "\n")
    return folder


@contextlib.contextmanager
def dump_on_failure(
    run_id: str, params: Mapping[str, RunParams], records: list[stack.Record]
) -> Iterator[None]:
    try:
        yield
    except Exception:
        dump_run(run_id, params, records)
        raise


def finish_run(
    env: stack.StackEnv,
    run_id: str,
    params: dict[str, RunParams],
    records: list[stack.Record],
    *,
    check_offline: Callable[[dict[str, Offline]], None] | None = None,
    strict: bool = True,
    timeout: float = 900.0,
) -> ActorRun:
    """Fold the records offline, check the offline side first, then wait for the actors."""
    with dump_on_failure(run_id, params, records):
        offline = stack.offline_run(params, records)
        if check_offline is not None:
            check_offline(offline)
        ends = stack.run_ends(params, offline)
        env.restate.wait_run(run_id, len(records), ends, strict=strict, timeout=timeout)
    return ActorRun(run_id, params, records, offline)


def actor_run(
    env: stack.StackEnv,
    run_id: str,
    *,
    start: str,
    days: float,
    locations: int,
    late_slots: int = 8,
    faults: Path | None = None,
    publish: Callable[[], list[stack.Record]] | None = None,
    check_offline: Callable[[dict[str, Offline]], None] | None = None,
    timeout: float = 900.0,
) -> ActorRun:
    """Configure, publish (``publish`` replaces the plain backfill), wait, then compare."""
    params = configure_run(
        env,
        run_id,
        start=start,
        days=days,
        locations=locations,
        late_slots=late_slots,
        generator_config=faults,
    )
    if publish is None:
        records = publish_run(
            env, run_id, start=start, days=days, locations=locations, faults=faults
        )
    else:
        records = publish()
    run = finish_run(env, run_id, params, records, check_offline=check_offline, timeout=timeout)
    assert_run(env, run)
    return run


def decoded(env: stack.StackEnv, run_id: str, service: str, tp: type[Any]) -> dict[str, Any]:
    """``{key: state}`` of every object of ``service`` in the run that holds a ``state``."""
    serde = serde_for(tp)
    return {
        key: serde.deserialize(values["state"].encode())
        for key, values in env.restate.states(run_id, service).items()
        if "state" in values
    }


def assert_run(env: stack.StackEnv, run: ActorRun, *, strict: bool = True) -> None:
    """AC-011-38 … -42 after a handled run; leaves the records and parameters on a failure."""
    with dump_on_failure(run.run_id, run.params, run.records):
        _assert_states(env, run)
        _assert_logs_and_unmanaged(env, run)
        assert env.restate.failures(run.run_id, strict=strict) == []


def _assert_states(env: stack.StackEnv, run: ActorRun) -> None:
    households = decoded(env, run.run_id, "Household", HouseholdState)
    batteries = decoded(env, run.run_id, "Battery", BatteryState)
    meters = decoded(env, run.run_id, "Meter", MeterState)
    names = {name for p in run.params.values() for name in p.strategies}
    strategies = {name: decoded(env, run.run_id, name, StrategyState) for name in names}
    for key, p in run.params.items():
        offline = run.offline[key]
        # The household's join: every step released, what each step saw, the counters.
        assert households[key].join == offline.join, key
        for name in p.strategies:
            assert strategies[name][key].result == offline.strategies[name], (key, name)
        # AC-011-36: the real battery and meter equal the live strategy's virtual ones.
        live = offline.strategies[p.live]
        assert batteries[key].soc_kwh == live.state.soc_kwh, key
        assert batteries[key].soc_kwh == strategies[p.live][key].result.state.soc_kwh, key
        m = meters[key]
        assert (m.acc, m.recent, m.months) == (live.state.acc, live.recent, live.months), key
        # The stored parameters are those sent; nothing arrived before `configure` (AC-011-42).
        params_serde = serde_for(RunParams)
        for service in (*stack.CONFIGURED, *p.strategies):
            values = env.restate.states(run.run_id, service)[key]
            assert params_serde.deserialize(values["params"].encode()) == p, (service, key)
            if service in stack.KAFKA_FED:
                assert "n_at_configure" in values, (service, key)
            assert json.loads(values.get("n_at_configure", "0")) == 0, (service, key)


def _assert_logs_and_unmanaged(env: stack.StackEnv, run: ActorRun) -> None:
    # AC-011-42: every Kafka-fed object's log equals the offline fold of the records read back.
    assert env.restate.ingress_logs(run.run_id) == stack.offline_logs(run.records)
    # AC-011-28: an unmanaged location keeps only its log and counter, under 1 kB in total.
    rows = env.restate.query(
        "SELECT service_name, service_key, key, value_length FROM state "
        "WHERE service_name IN ('Household', 'Weather') "
        f"AND starts_with(service_key, '{run.run_id}:')"
    )
    held: dict[tuple[str, str], dict[str, int]] = {}
    for row in rows:
        obj = (row["service_name"], row["service_key"])
        held.setdefault(obj, {})[row["key"]] = int(row["value_length"])
    expected = {
        (stack.service_of(r.meta[0]), r.key)
        for r in run.records
        if stack.service_of(r.meta[0]) in ("Household", "Weather") and r.key not in run.params
    }
    unmanaged = {obj: keys for obj, keys in held.items() if obj[1] not in run.params}
    assert set(unmanaged) == expected
    for obj, sizes in unmanaged.items():
        assert set(sizes) == UNMANAGED_STATE_KEYS, (obj, sorted(sizes))
        assert sum(sizes.values()) < UNMANAGED_MAX_BYTES, (obj, sizes)


def offline_counts(offline: Mapping[str, Offline]) -> dict[str, int]:
    """The meter counters and corrections of the managed households, summed (AC-011-40, -41)."""
    counts = dict.fromkeys(("estimated", "duplicate", "too_old", "late", "corrections"), 0)
    for o in offline.values():
        sides = [o.join.consumption] + ([o.join.production] if o.join.production else [])
        for side in sides:
            counts["estimated"] += side.stream.estimated
            counts["duplicate"] += side.stream.duplicate
            counts["too_old"] += side.stream.too_old
            counts["late"] += side.stream.late
        counts["corrections"] += sum(len(s.corrections) for s in o.steps)
    return counts
