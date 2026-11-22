"""The shared harness of the Restate tiers (`ems.experiments.stack`), without a stack.

AC-010-26 (spec 010, plan §6): the harness fails fast when the SDK service's port is busy. A port
counts as in use when binding it on 127.0.0.1 fails (this tier blocks connects, not binds).
``SdkService.start()`` then raises ``PortInUse``, naming the port, before it starts any process.

Spec 011 (plan §7): the ingress targets and the topic → service map (AC-011-25); the registration
guard on canned service lists (AC-011-44); the wait for a configured run, strict and lenient, on
canned ``sys_invocation`` rows and canned state (AC-011-45); the offline side grouped by service
(AC-011-47); ``configure`` with an idempotency key per object (AC-011-37). The Restate client is
subclassed so that no request leaves the process.
"""

from __future__ import annotations

import dataclasses
import json
import socket
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NoReturn

import pytest

from ems.actors import battery, ems, ingress_check, meter, strategy
from ems.actors.app import SERVICES
from ems.actors.reorder import Inbox
from ems.actors.serde import serde_for
from ems.assembler.params import RunParams
from ems.experiments import stack
from ems.experiments.runs import managed_params
from ems.settings import EmsSettings

RUN_ID = "r1"
PREFIX = "it0123456789."
LOCATION = f"{RUN_ID}:PL-LOC-000001"
MARKET = f"{RUN_ID}:PL:RCE"
EIGHT_SERVICES = {
    "Household",
    "Tariff",
    "Weather",
    "NoBattery",
    "SelfConsumption",
    "Ems",
    "Battery",
    "Meter",
}
CONFIGURED_SERVICES = {"Household", "Weather", "Ems", "Battery", "Meter"}


@pytest.fixture
def busy_port() -> Iterator[int]:
    """A port on 127.0.0.1 held by a listening socket while the test runs."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        yield held.getsockname()[1]


def test_ac_010_26_port_in_use_is_a_runtime_error() -> None:
    assert issubclass(stack.PortInUse, RuntimeError)


def test_ac_010_26_port_in_use_is_decided_by_binding() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        port = held.getsockname()[1]
        assert stack.port_in_use(port) is True
    assert stack.port_in_use(port) is False


def test_ac_010_26_sdk_service_refuses_a_busy_port_before_starting_a_process(
    busy_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[tuple[object, ...]] = []

    def no_process(*args: object, **_kwargs: object) -> NoReturn:
        started.append(args)
        pytest.fail(f"a process was started although port {busy_port} is in use: {args!r}")

    # ``stack.subprocess.Popen`` is ``subprocess.Popen``; a ``Popen`` imported by name goes too.
    monkeypatch.setattr(subprocess, "Popen", no_process)
    if hasattr(stack, "Popen"):
        monkeypatch.setattr(stack, "Popen", no_process)
    service = stack.SdkService(port=busy_port)
    with pytest.raises(stack.PortInUse) as raised:
        service.start()
    message = str(raised.value)
    assert str(busy_port) in message
    assert "in use" in message
    assert started == []


# ── spec 011: targets, the offline side by service ──────────────────────────────────────────────


def _inbox(expected: int) -> dict[str, Any]:
    return {"expected": expected, "held": [], "reordered": 0, "stale": 0}


def test_ac_011_25_ingress_targets_are_household_weather_and_tariff() -> None:
    assert stack.INGRESS_TARGETS == {
        "ingress-meters": ("Household", "on_measurement"),
        "ingress-forecasts": ("Weather", "on_forecast"),
        "ingress-prices": ("Tariff", "on_price"),
    }
    assert set(stack.KAFKA_FED) == {"Household", "Weather", "Tariff"}


def test_ac_011_25_every_target_handler_is_served_by_the_app() -> None:
    handlers = stack.registered_handlers()
    for service, handler in stack.INGRESS_TARGETS.values():
        assert handler in handlers[service], (service, handler)


@pytest.mark.parametrize(
    ("topic", "service"),
    [
        ("ems.production.v1", "Household"),
        ("ems.consumption.v1", "Household"),
        ("ems.weather.forecast.v1", "Weather"),
        ("ems.price.v1", "Tariff"),
        (PREFIX + "ems.production.v1", "Household"),
        (PREFIX + "ems.consumption.v1", "Household"),
        (PREFIX + "ems.weather.forecast.v1", "Weather"),
        (PREFIX + "ems.price.v1", "Tariff"),
    ],
)
def test_ac_011_25_service_of_a_topic_with_and_without_prefix(topic: str, service: str) -> None:
    assert stack.service_of(topic) == service


@pytest.mark.parametrize("topic", ["ems.weather.actual.v1", PREFIX + "ems.gen.dlq.v1", "other"])
def test_ac_011_25_service_of_an_unread_topic_raises(topic: str) -> None:
    with pytest.raises(ValueError, match="no service reads"):
        stack.service_of(topic)


def _records() -> list[stack.Record]:
    """A location's meter and forecast records and the market's price records, in partition
    order; the values need not be valid events (the log counts them as rejected).
    """
    return [
        stack.Record(LOCATION, (PREFIX + "ems.production.v1", 0, 0), b'{"p": 0}'),
        stack.Record(LOCATION, (PREFIX + "ems.consumption.v1", 1, 0), b'{"c": 0}'),
        stack.Record(LOCATION, (PREFIX + "ems.weather.forecast.v1", 2, 0), b'{"f": 0}'),
        stack.Record(LOCATION, (PREFIX + "ems.consumption.v1", 1, 1), b"not json {"),
        stack.Record(f"{RUN_ID}:PL-LOC-000002", (PREFIX + "ems.consumption.v1", 0, 0), b"{}"),
        stack.Record(MARKET, (PREFIX + "ems.price.v1", 0, 0), b'{"r": 0}'),
        stack.Record(MARKET, (PREFIX + "ems.price.v1", 0, 1), b'{"r": 1}'),
    ]


def test_ac_011_47_offline_logs_group_a_location_by_the_service_of_each_topic() -> None:
    records = _records()
    logs = stack.offline_logs(records)

    def fold(*indices: int) -> dict[str, Any]:
        return ingress_check.fold_log((records[i].meta, records[i].value) for i in indices)

    assert logs == {
        ("Household", LOCATION): fold(0, 1, 3),
        ("Weather", LOCATION): fold(2),
        ("Household", f"{RUN_ID}:PL-LOC-000002"): fold(4),
        ("Tariff", MARKET): fold(5, 6),
    }
    assert set(logs[("Household", LOCATION)]["topics"]) == {
        PREFIX + "ems.production.v1",
        PREFIX + "ems.consumption.v1",
    }
    assert set(logs[("Weather", LOCATION)]["topics"]) == {PREFIX + "ems.weather.forecast.v1"}


def test_ac_011_38_records_by_stream_gives_one_key_per_stream_in_partition_order() -> None:
    records = _records()
    assert dict(stack.records_by_stream(records, LOCATION)) == {
        "production": [b'{"p": 0}'],
        "consumption": [b'{"c": 0}', b"not json {"],
        "weather_forecast": [b'{"f": 0}'],
    }
    assert dict(stack.records_by_stream(records, MARKET)) == {"price": [b'{"r": 0}', b'{"r": 1}']}
    assert dict(stack.records_by_stream(records, f"{RUN_ID}:PL-LOC-999999")) == {}


def test_ac_011_45_run_ends_cover_each_strategy_ems_battery_and_meter() -> None:
    params = {
        "r1:A": SimpleNamespace(strategies=("NoBattery", "SelfConsumption")),
        "r1:B": SimpleNamespace(strategies=("NoBattery", "SelfConsumption")),
    }
    offline = {
        "r1:A": SimpleNamespace(join=SimpleNamespace(next=1000)),
        "r1:B": SimpleNamespace(join=SimpleNamespace(next=990)),
    }
    ends = {"r1:A": 1000, "r1:B": 990}
    assert stack.run_ends(params, offline) == {
        "NoBattery": ends,
        "SelfConsumption": ends,
        "Ems": ends,
        "Battery": ends,
        "Meter": ends,
    }


# ── spec 011: the next step each actor expects, from its stored state ───────────────────────────


@pytest.fixture(scope="module")
def params() -> dict[str, RunParams]:
    return managed_params(
        run_id=RUN_ID, start=datetime(2026, 10, 20, tzinfo=UTC), days=1, locations=10
    )


@pytest.fixture(scope="module")
def site(params: dict[str, RunParams]) -> RunParams:
    return params[LOCATION]


def _stored(state: Any) -> dict[str, Any]:
    """A state as Restate stores it: the serde's JSON, read back as the harness reads it."""
    return json.loads(serde_for(type(state)).serialize(state))


@pytest.mark.parametrize(
    ("service", "module"),
    [
        ("NoBattery", strategy),
        ("SelfConsumption", strategy),
        ("Ems", ems),
        ("Battery", battery),
        ("Meter", meter),
    ],
)
def test_ac_011_45_next_step_of_a_start_state_is_the_first_step(
    site: RunParams, service: str, module: Any
) -> None:
    state, _ = module.start(site)
    assert stack.next_step(service, _stored(state)) == site.first_step


def test_ac_011_45_next_step_of_a_strategy_is_its_step_inbox(site: RunParams) -> None:
    state, _ = strategy.start(site)
    later = dataclasses.replace(state, steps=Inbox(expected=site.first_step + 40))
    assert stack.next_step("SelfConsumption", _stored(later)) == site.first_step + 40


def test_ac_011_45_next_step_of_ems_is_its_acknowledgement_inbox(site: RunParams) -> None:
    """Ems may hold steps it has not commanded yet: it stands where its acknowledgements stand."""
    state, _ = ems.start(site)
    later = dataclasses.replace(
        state,
        steps=Inbox(expected=site.first_step + 40),
        acks=Inbox(expected=site.first_step + 37),
    )
    assert stack.next_step("Ems", _stored(later)) == site.first_step + 37


def test_ac_011_45_next_step_of_battery_is_its_command_inbox(site: RunParams) -> None:
    state, _ = battery.start(site)
    later = dataclasses.replace(state, commands=Inbox(expected=site.first_step + 12))
    assert stack.next_step("Battery", _stored(later)) == site.first_step + 12


def test_ac_011_45_next_step_of_meter_is_its_flow_inbox(site: RunParams) -> None:
    state, _ = meter.start(site)
    later = dataclasses.replace(state, flows=Inbox(expected=site.first_step + 9))
    assert stack.next_step("Meter", _stored(later)) == site.first_step + 9


# ── spec 011: failed invocations on canned sys_invocation rows ──────────────────────────────────


def _row(**fields: Any) -> dict[str, Any]:
    base = {
        "target_service_name": "Household",
        "target_service_key": LOCATION,
        "target_handler_name": "on_measurement",
        "status": "completed",
        "invoked_by": "ingress",
        "completion_result": "success",
        "last_failure": None,
        "retry_count": 1,
    }
    return base | fields


# A transport error between Restate and the SDK service (an HTTP/2 stream closed) fails one attempt
# and the retry succeeds; a handler that raises fails every attempt. Strict mode fails on the second
# kind only: an invocation still failing after STRICT_ATTEMPTS attempts (AC-011-45, revised).
PAUSED = _row(status="paused", completion_result=None, last_failure="RT0016 journal mismatch")
FAILED = _row(completion_result="failure", target_handler_name="on_step", target_service_name="Ems")
H2 = "[500 Internal] h2 pool connection error: stream closed because of a broken pipe"
RETRYING = _row(status="backing-off", completion_result=None, last_failure=H2, retry_count=2)
KEEPS_FAILING = _row(
    status="backing-off",
    completion_result=None,
    last_failure="[500 Internal] ValueError: quarter-hour 9 out of order",
    retry_count=stack.STRICT_ATTEMPTS,
)
RETRIED_THEN_DONE = _row(last_failure=H2, retry_count=2)
CLEAN = _row()
RUNNING = _row(status="running", completion_result=None)


@pytest.mark.parametrize(
    ("row", "strict", "expected"),
    [
        (PAUSED, True, True),
        (PAUSED, False, True),
        (FAILED, True, True),
        (FAILED, False, True),
        (RETRYING, True, False),
        (RETRYING, False, False),
        (KEEPS_FAILING, True, True),
        (KEEPS_FAILING, False, False),
        (RETRIED_THEN_DONE, True, False),
        (RETRIED_THEN_DONE, False, False),
        (CLEAN, True, False),
        (CLEAN, False, False),
        (RUNNING, True, False),
        (RUNNING, False, False),
    ],
)
def test_ac_011_45_failed_on_canned_rows(row: dict[str, Any], strict: bool, expected: bool) -> None:
    assert stack.failed(row, strict=strict) is expected


# ── spec 011: wait_run on canned rows and state (no request leaves the process) ──────────────────


class CannedRestate(stack.Restate):
    """Answers the harness's queries from canned rows and state."""

    def __init__(
        self,
        *,
        rows: Sequence[Mapping[str, Any]] = (),
        handled: int = 0,
        states: Mapping[str, Mapping[str, Mapping[str, str]]] | None = None,
    ) -> None:
        super().__init__(EmsSettings())
        self.rows = [dict(r) for r in rows]
        self.n = handled
        self.canned = {
            svc: {k: dict(v) for k, v in by.items()} for svc, by in (states or {}).items()
        }

    def invocations(self, run_id: str) -> list[dict[str, Any]]:
        return self.rows

    def handled(self, run_id: str, services: Sequence[str] = stack.KAFKA_FED) -> int:
        return self.n

    def states(self, run_id: str, service: str) -> dict[str, dict[str, str]]:
        return {key: dict(values) for key, values in self.canned.get(service, {}).items()}


ENDS = {
    "NoBattery": {LOCATION: 1200},
    "SelfConsumption": {LOCATION: 1200},
    "Ems": {LOCATION: 1200},
    "Battery": {LOCATION: 1200},
    "Meter": {LOCATION: 1200},
}
INBOX_OF = {"NoBattery": "steps", "SelfConsumption": "steps", "Ems": "acks"}
INBOX_OF |= {"Battery": "commands", "Meter": "flows"}
RECORDS = 500


def _at(next_steps: Mapping[str, int]) -> dict[str, dict[str, dict[str, str]]]:
    """Canned state of every stepping object of LOCATION, each expecting the given step."""
    return {
        service: {LOCATION: {"state": json.dumps({INBOX_OF[service]: _inbox(step)})}}
        for service, step in next_steps.items()
    }


AT_END = _at(dict.fromkeys(ENDS, 1200))


@pytest.fixture
def canned() -> Iterator[list[CannedRestate]]:
    made: list[CannedRestate] = []
    yield made
    for client in made:
        client.close()


def _restate(made: list[CannedRestate], **kwargs: Any) -> CannedRestate:
    client = CannedRestate(**kwargs)
    made.append(client)
    return client


def test_ac_011_45_wait_run_returns_when_records_are_handled_and_every_object_is_at_its_end(
    canned: list[CannedRestate],
) -> None:
    r = _restate(canned, rows=[CLEAN, RUNNING], handled=RECORDS, states=AT_END)
    assert r.wait_run(RUN_ID, RECORDS, ENDS, timeout=-1.0) >= 0.0


@pytest.mark.parametrize("row", [PAUSED, FAILED, KEEPS_FAILING])
def test_ac_011_45_wait_run_strict_fails_at_once_on_an_invocation_that_keeps_failing(
    canned: list[CannedRestate], row: dict[str, Any]
) -> None:
    r = _restate(canned, rows=[CLEAN, row], handled=RECORDS, states=AT_END)
    with pytest.raises(RuntimeError, match="failed invocations"):
        r.wait_run(RUN_ID, RECORDS, ENDS, timeout=-1.0)


@pytest.mark.parametrize("row", [PAUSED, FAILED])
def test_ac_011_45_wait_run_lenient_fails_at_once_on_a_failed_or_paused_invocation(
    canned: list[CannedRestate], row: dict[str, Any]
) -> None:
    r = _restate(canned, rows=[CLEAN, row], handled=RECORDS, states=AT_END)
    with pytest.raises(RuntimeError, match="failed invocations"):
        r.wait_run(RUN_ID, RECORDS, ENDS, strict=False, timeout=-1.0)


@pytest.mark.parametrize("row", [RETRYING, RETRIED_THEN_DONE, KEEPS_FAILING])
def test_ac_011_45_wait_run_lenient_accepts_retried_attempts(
    canned: list[CannedRestate], row: dict[str, Any]
) -> None:
    """The fault tier: the attempts a kill interrupts are retried and recorded as failures."""
    r = _restate(canned, rows=[CLEAN, row], handled=RECORDS, states=AT_END)
    assert r.wait_run(RUN_ID, RECORDS, ENDS, strict=False, timeout=-1.0) >= 0.0


@pytest.mark.parametrize("row", [RETRYING, RETRIED_THEN_DONE])
def test_ac_011_45_wait_run_strict_accepts_a_transient_failed_attempt(
    canned: list[CannedRestate], row: dict[str, Any]
) -> None:
    """A transport error between Restate and the SDK service is retried and succeeds."""
    r = _restate(canned, rows=[CLEAN, row], handled=RECORDS, states=AT_END)
    assert r.wait_run(RUN_ID, RECORDS, ENDS, timeout=-1.0) >= 0.0


def test_ac_011_45_wait_run_waits_for_every_published_record(
    canned: list[CannedRestate],
) -> None:
    r = _restate(canned, rows=[CLEAN], handled=RECORDS - 1, states=AT_END)
    with pytest.raises(TimeoutError):
        r.wait_run(RUN_ID, RECORDS, ENDS, timeout=-1.0)


@pytest.mark.parametrize("behind", sorted(ENDS))
def test_ac_011_45_wait_run_waits_for_every_object_to_reach_its_end(
    canned: list[CannedRestate], behind: str
) -> None:
    states = _at({svc: 1199 if svc == behind else 1200 for svc in ENDS})
    r = _restate(canned, rows=[CLEAN], handled=RECORDS, states=states)
    with pytest.raises(TimeoutError):
        r.wait_run(RUN_ID, RECORDS, ENDS, timeout=-1.0)


@pytest.mark.parametrize("missing", sorted(ENDS))
def test_ac_011_45_wait_run_waits_for_an_object_without_state(
    canned: list[CannedRestate], missing: str
) -> None:
    states = {svc: by for svc, by in AT_END.items() if svc != missing}
    r = _restate(canned, rows=[CLEAN], handled=RECORDS, states=states)
    with pytest.raises(TimeoutError):
        r.wait_run(RUN_ID, RECORDS, ENDS, timeout=-1.0)


def test_ac_011_45_failures_lists_the_failed_rows(canned: list[CannedRestate]) -> None:
    r = _restate(canned, rows=[CLEAN, PAUSED, RETRYING, KEEPS_FAILING, FAILED, RUNNING])
    assert r.failures(RUN_ID) == [PAUSED, KEEPS_FAILING, FAILED]
    assert r.failures(RUN_ID, strict=False) == [PAUSED, FAILED]


def test_ac_011_47_ingress_logs_by_service_and_key(canned: list[CannedRestate]) -> None:
    log = {"topics": {}, "rejected": 1}
    states = {
        "Household": {LOCATION: {"log": json.dumps(log), "n": "1"}},
        "Weather": {LOCATION: {"log": json.dumps(log | {"rejected": 2}), "n": "2"}},
        "Tariff": {MARKET: {"log": json.dumps(log | {"rejected": 3}), "n": "3", "state": "{}"}},
        "Ems": {LOCATION: {"state": "{}"}},
    }
    r = _restate(canned, states=states)
    assert r.ingress_logs(RUN_ID) == {
        ("Household", LOCATION): log,
        ("Weather", LOCATION): log | {"rejected": 2},
        ("Tariff", MARKET): log | {"rejected": 3},
    }


class QueryRecorder(stack.Restate):
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        super().__init__(EmsSettings())
        self.rows = rows
        self.sql: list[str] = []

    def query(self, sql: str) -> list[dict[str, Any]]:
        self.sql.append(sql)
        return self.rows


def test_ac_011_47_handled_sums_n_over_the_kafka_fed_services() -> None:
    r = QueryRecorder([{"value_utf8": "3"}, {"value_utf8": "4"}])
    try:
        assert r.handled(RUN_ID) == 7
        assert "IN ('Household', 'Weather', 'Tariff')" in r.sql[-1]
        assert r.handled(RUN_ID, services=("Household",)) == 7
        assert "IN ('Household')" in r.sql[-1]
        assert f"starts_with(service_key, '{RUN_ID}:')" in r.sql[-1]
        with pytest.raises(ValueError):
            r.handled("r1' OR '1'='1")
    finally:
        r.close()


# ── spec 011: configure (AC-011-37) ──────────────────────────────────────────────────────────────


class SendRecorder(stack.Restate):
    def __init__(self) -> None:
        super().__init__(EmsSettings())
        self.sent: list[tuple[str, str, str, Any, str | None]] = []

    def send(
        self,
        service: str,
        key: str,
        handler: str,
        body: Any = None,
        *,
        idempotency_key: str | None = None,
    ) -> None:
        self.sent.append((service, key, handler, body, idempotency_key))


def test_ac_011_37_configure_sends_each_object_its_parameters_with_an_idempotency_key(
    params: dict[str, RunParams],
) -> None:
    r = SendRecorder()
    try:
        r.configure(params)
    finally:
        r.close()
    expected = {
        (service, key, "configure", f"{key}:{service}:configure")
        for key, p in params.items()
        for service in (*CONFIGURED_SERVICES, *p.strategies)
    }
    assert {(s, k, h, idem) for s, k, h, _, idem in r.sent} == expected
    assert len(r.sent) == len(expected)  # one call per object
    serde = serde_for(RunParams)
    for service, key, _, body, _ in r.sent:
        assert isinstance(body, bytes), service
        assert serde.deserialize(body) == params[key], (service, key)


def test_ac_011_37_tariff_is_never_configured(params: dict[str, RunParams]) -> None:
    r = SendRecorder()
    try:
        r.configure(params)
    finally:
        r.close()
    assert "Tariff" not in {service for service, *_ in r.sent}


# ── spec 011: the registration guard (AC-011-44) ─────────────────────────────────────────────────


def test_ac_011_44_registered_handlers_are_those_of_the_app() -> None:
    handlers = stack.registered_handlers()
    assert handlers == {svc.name: set(svc.handlers) for svc in SERVICES}
    assert set(handlers) == EIGHT_SERVICES
    assert not {"Probe", "StateCostEager", "StateCostLazy"} & set(handlers)


class FakeAdmin(stack.Restate):
    """Restate's admin API as a list of service listings, one after each registration."""

    def __init__(self, listings: Sequence[Mapping[str, set[str]]]) -> None:
        super().__init__(EmsSettings())
        self.listings = [dict(listing) for listing in listings]
        self.forces: list[bool] = []

    def register(self, uri: str | None = None, *, force: bool = False) -> dict[str, Any]:
        self.forces.append(force)
        return {"status": 200, "id": f"dp_{len(self.forces)}"}

    def services(self) -> dict[str, set[str]]:
        assert self.forces, "services listed before any registration"
        listing = self.listings[min(len(self.forces), len(self.listings)) - 1]
        return {name: set(handlers) for name, handlers in listing.items()}


@pytest.fixture
def admins() -> Iterator[list[FakeAdmin]]:
    made: list[FakeAdmin] = []
    yield made
    for client in made:
        client.close()


def _admin(made: list[FakeAdmin], *listings: Mapping[str, set[str]]) -> FakeAdmin:
    client = FakeAdmin(listings)
    made.append(client)
    return client


EXPECTED = {
    "Household": {"configure", "on_measurement", "on_prices", "on_forecast", "status"},
    "Tariff": {"on_price", "subscribe", "status"},
}
STALE = {"Household": {"on_measurement", "on_forecast", "on_price", "status"}}  # a stage 2 shape


def test_ac_011_44_equal_handlers_register_once_without_force(admins: list[FakeAdmin]) -> None:
    admin = _admin(admins, EXPECTED)
    assert admin.ensure_registered(EXPECTED) == {"status": 200, "id": "dp_1"}
    assert admin.forces == [False]


def test_ac_011_44_services_outside_the_app_are_not_compared(admins: list[FakeAdmin]) -> None:
    admin = _admin(admins, EXPECTED | {"Probe": {"on_measurement", "status"}})
    admin.ensure_registered(EXPECTED)
    assert admin.forces == [False]


@pytest.mark.parametrize(
    "stale",
    [
        STALE | {"Tariff": EXPECTED["Tariff"]},  # other handlers
        {"Tariff": EXPECTED["Tariff"]},  # a service missing
        EXPECTED | {"Tariff": EXPECTED["Tariff"] | {"count"}},  # one handler too many
    ],
)
def test_ac_011_44_a_stale_listing_registers_again_with_force(
    admins: list[FakeAdmin], stale: Mapping[str, set[str]]
) -> None:
    admin = _admin(admins, stale, EXPECTED)
    assert admin.ensure_registered(EXPECTED) == {"status": 200, "id": "dp_2"}
    assert admin.forces == [False, True]


def test_ac_011_44_still_stale_after_force_raises(admins: list[FakeAdmin]) -> None:
    admin = _admin(admins, STALE, STALE)
    with pytest.raises(RuntimeError, match="still differ"):
        admin.ensure_registered(EXPECTED)
    assert admin.forces == [False, True]


def test_ac_011_44_stack_env_registers_the_app_after_closing_ingestion_and_before_its_ingress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env = stack.StackEnv(PREFIX, log_dir=tmp_path)
    calls: list[Any] = []

    def record(name: str, result: Any = None) -> Any:
        def fn(*args: Any, **kwargs: Any) -> Any:
            calls.append((name, args))
            return result

        return fn

    monkeypatch.setattr(env.restate, "wait_healthy", record("healthy", 0.0))
    monkeypatch.setattr(stack, "close_stack_ingestion", record("close", []))
    monkeypatch.setattr(stack, "ensure_topics", record("topics"))
    monkeypatch.setattr(env.sdk, "start", record("sdk"))
    monkeypatch.setattr(env.restate, "ensure_registered", record("register", {"id": "dp_1"}))
    monkeypatch.setattr(env.restate, "register", record("register-unchecked"))
    monkeypatch.setattr(env.ingress, "up", record("ingress"))
    try:
        env.open()
    finally:
        env.restate.close()
    names = [name for name, _ in calls]
    assert "register-unchecked" not in names
    assert names.index("close") < names.index("sdk") < names.index("register")
    assert names.index("register") < names.index("ingress")
    assert dict(calls)["register"] == (stack.registered_handlers(),)
    assert env.deployment == {"id": "dp_1"}
