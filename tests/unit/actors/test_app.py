"""AC-011-24 (with AC-011-25, -26 for the handlers' input): the services of ``ems.actors.app``.

- ``SERVICES`` holds exactly the eight Virtual Objects of the plan, with the handler names and
  kinds of its Handlers table; the read handlers (``status``, ``summary``, ``state``, ``bill``) are
  shared, write nothing and return the stored state through its ``DataclassSerde``.
- Every ``(service, handler)`` a transition sends to exists, is exclusive (reached by a one-way
  message only) and decodes the bytes it is sent to an equal message. The targets are collected from
  a run of the in-memory network, where every edge is used.
- The Kafka-fed handlers take raw bytes of any content type.
- No file of ``src/ems/actors/`` sleeps, schedules a delayed send, reads the clock, journals a side
  effect or makes a request-response call (P4, P5, P7; R1, R2, R5). ``probe.py`` and
  ``statecost.py`` are excluded: phase 4 deletes them (the stage 2 objects stay in ``app`` until
  then, so ``app`` itself is not checked here).
"""

from __future__ import annotations

import inspect
import re
from typing import Any

import pytest
from restate import VirtualObject
from restate.serde import BytesSerde

from ems.actors.app import SERVICES
from ems.actors.serde import DataclassSerde, serde_for
from tests.network import (
    HOURS,
    REPO,
    START,
    STARTS,
    STATE_TYPES,
    Network,
    Order,
    generate,
    managed_params,
)

EXCLUSIVE, SHARED = "exclusive", "shared"
HANDLERS: dict[str, dict[str, str]] = {
    "Household": {
        "configure": EXCLUSIVE,
        "on_measurement": EXCLUSIVE,
        "on_prices": EXCLUSIVE,
        "on_forecast": EXCLUSIVE,
        "status": SHARED,
    },
    "Tariff": {"on_price": EXCLUSIVE, "subscribe": EXCLUSIVE, "status": SHARED},
    "Weather": {"configure": EXCLUSIVE, "on_forecast": EXCLUSIVE, "status": SHARED},
    "NoBattery": {"configure": EXCLUSIVE, "on_step": EXCLUSIVE, "summary": SHARED},
    "SelfConsumption": {"configure": EXCLUSIVE, "on_step": EXCLUSIVE, "summary": SHARED},
    "Ems": {"configure": EXCLUSIVE, "on_step": EXCLUSIVE, "on_ack": EXCLUSIVE, "status": SHARED},
    "Battery": {"configure": EXCLUSIVE, "apply": EXCLUSIVE, "state": SHARED},
    "Meter": {"configure": EXCLUSIVE, "on_flows": EXCLUSIVE, "bill": SHARED},
}
# The one-way edges of the plan's Handlers table (column Sends).
EDGES = {
    ("Tariff", "subscribe"),
    ("Household", "on_prices"),
    ("Household", "on_forecast"),
    ("NoBattery", "on_step"),
    ("SelfConsumption", "on_step"),
    ("Ems", "on_step"),
    ("Battery", "apply"),
    ("Ems", "on_ack"),
    ("Meter", "on_flows"),
}
KAFKA_FED = {("Household", "on_measurement"), ("Weather", "on_forecast"), ("Tariff", "on_price")}
ACTORS = REPO / "src" / "ems" / "actors"
STAGE_2 = {"probe.py", "statecost.py"}  # deleted in phase 4 (T33)
FORBIDDEN = (
    "sleep",
    "send_delay",
    "ctx.time",
    "ctx.run",
    "object_call",
    "service_call",
    "generic_call",
)


def _services() -> dict[str, VirtualObject]:
    return {s.name: s for s in SERVICES}


def _handler(service: str, handler: str) -> Any:
    return _services()[service].handlers[handler]


def test_ac_011_24_services_are_the_eight_actors() -> None:
    names = [s.name for s in SERVICES]
    assert sorted(names) == sorted(HANDLERS)
    assert len(names) == len(set(names)) == 8
    assert all(isinstance(s, VirtualObject) for s in SERVICES)
    assert not {"Probe", "StateCostEager", "StateCostLazy"} & set(names)


@pytest.mark.parametrize("service", sorted(HANDLERS))
def test_ac_011_24_handler_names_and_kinds(service: str) -> None:
    handlers = _services()[service].handlers
    assert {name: h.kind for name, h in handlers.items()} == HANDLERS[service]


SHARED_HANDLERS = [
    (s, h) for s, hs in sorted(HANDLERS.items()) for h, k in hs.items() if k == SHARED
]


@pytest.mark.parametrize(("service", "handler"), SHARED_HANDLERS)
def test_ac_011_24_read_handlers_write_nothing_and_return_the_state(
    service: str, handler: str
) -> None:
    h = _handler(service, handler)
    source = inspect.getsource(h.fn).replace("shell.STATE", "")  # the state key's name
    for write in (".set(", ".clear(", "send(", "shell."):
        assert write not in source, (service, handler, write)

    serde = h.handler_io.output_serde
    assert isinstance(serde, DataclassSerde)
    params = next(iter(managed_params(START, HOURS, 8).values()))
    start = STARTS.get(service)
    value = start(params)[0] if start is not None else STATE_TYPES[service]()
    assert serde.serialize(value) == serde_for(STATE_TYPES[service]).serialize(value)
    assert serde.deserialize(serde.serialize(value)) == value


@pytest.mark.parametrize(("service", "handler"), sorted(KAFKA_FED))
def test_ac_011_24_kafka_fed_handlers_take_raw_bytes(service: str, handler: str) -> None:
    io = _handler(service, handler).handler_io
    assert isinstance(io.input_serde, BytesSerde)
    assert io.accept == "*/*"


@pytest.fixture(scope="module")
def network() -> Network:
    net = Network(generate(START, HOURS, 8, False))
    net.run(Order(seed=21, weights=(1, 1, 1, 1, 1), pick="any"))
    return net


def test_ac_011_24_every_edge_is_used_by_the_network(network: Network) -> None:
    assert {(s.service, s.handler) for s in network.sent} == EDGES


def test_ac_011_24_every_send_target_exists_and_is_exclusive(network: Network) -> None:
    for service, handler in sorted({(s.service, s.handler) for s in network.sent}):
        assert service in _services(), service
        assert handler in _services()[service].handlers, (service, handler)
        assert _handler(service, handler).kind == EXCLUSIVE, (service, handler)


def test_ac_011_24_every_target_decodes_what_it_is_sent(network: Network) -> None:
    """The input serde a target handler declares decodes the bytes its sender encodes."""
    checked: set[tuple[str, str]] = set()
    for s in network.sent:
        raw = serde_for(type(s.message)).serialize(s.message)
        decoded = _handler(s.service, s.handler).handler_io.input_serde.deserialize(raw)
        assert decoded == s.message, (s.service, s.handler)
        assert type(decoded) is type(s.message)
        checked.add((s.service, s.handler))
    assert checked == EDGES


def _actor_files() -> list[str]:
    return sorted(p.name for p in ACTORS.glob("*.py") if p.name not in STAGE_2)


def test_ac_011_24_the_scan_covers_every_actor_file() -> None:
    expected = {
        "app.py",
        "battery.py",
        "ems.py",
        "household.py",
        "messages.py",
        "meter.py",
        "reorder.py",
        "serde.py",
        "shell.py",
        "strategy.py",
        "tariff.py",
        "weather.py",
    }
    assert expected <= set(_actor_files())


@pytest.mark.parametrize("name", _actor_files())
def test_ac_011_24_no_actor_file_waits_reads_the_clock_or_calls(name: str) -> None:
    text = (ACTORS / name).read_text()
    found = [word for word in FORBIDDEN if re.search(re.escape(word), text)]
    assert found == [], (name, found)
