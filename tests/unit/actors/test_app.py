"""AC-011-24 (with AC-011-25, -26 for the handlers' input): the services of ``ems.actors.app``.

- ``SERVICES`` holds exactly the eight Virtual Objects of the plan, with the handler names and
  kinds of its Handlers table; the read handlers (``status``, ``summary``, ``state``, ``bill``) are
  shared, write nothing and return the stored state through its ``DataclassSerde``.
- Every ``(service, handler)`` a transition sends to exists, is exclusive (reached by a one-way
  message only) and decodes the bytes it is sent to an equal message. The targets are collected from
  a run of the in-memory network, where every edge is used.
- The Kafka-fed handlers take raw bytes of any content type.
- No file of ``src/ems/actors/`` sleeps, journals a side effect or makes a request-response call
  (P4, P5, P7; R1, R2, R5). ``probe.py`` and ``statecost.py`` are excluded: phase 4 deletes them
  (the stage 2 objects stay in ``app`` until then, so ``app`` itself is not checked here).

Spec 012 (T20; AC-012-09, -19, -20):
- ``Household`` gains ``on_deadline`` (a delayed self-send, through ``shell.handle_timed``) and
  ``set_strategies`` (an operator call through the HTTP ingress, through ``shell.handle``); the
  edges gain ``Household → Household``, used by a live household in the network fixture.
- The journaled clock and the delayed send stay on the deadline path: ``ctx.time`` only in
  ``shell.handle_timed``, ``send_delay`` only in ``shell._commit``; the wall clock (``time.time``,
  ``time.time_ns``) only in ``timing.py``, which makes no context call and is imported only by
  ``shell`` and ``app`` (P2: no transition logs).
"""

from __future__ import annotations

import ast
import inspect
import re
from typing import Any

import pytest
from restate import VirtualObject
from restate.serde import BytesSerde

from ems.actors import messages
from ems.actors.app import SERVICES
from ems.actors.serde import DataclassSerde, serde_for
from tests.network import (
    HOURS,
    REPO,
    RUN_ID,
    START,
    STARTS,
    STATE_TYPES,
    Network,
    Order,
    generate,
    live,
    managed_params,
)

EXCLUSIVE, SHARED = "exclusive", "shared"
HANDLERS: dict[str, dict[str, str]] = {
    "Household": {
        "configure": EXCLUSIVE,
        "on_measurement": EXCLUSIVE,
        "on_prices": EXCLUSIVE,
        "on_forecast": EXCLUSIVE,
        "on_deadline": EXCLUSIVE,
        "set_strategies": EXCLUSIVE,
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
    ("Household", "on_deadline"),  # spec 012: the live deadline, a delayed self-send
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
    "ctx.run",
    "object_call",
    "service_call",
    "generic_call",
    # clock reads other than the timing log's (CONFINED)
    "datetime.now",
    "date.today",
    "monotonic",
    "perf_counter",
    "from time import",
)
# Allowed only inside one function of one file (None: anywhere in that file), AC-012-09.
CONFINED: dict[str, set[tuple[str, str | None]]] = {
    r"ctx\.time\b": {("shell.py", "handle_timed")},
    r"send_delay": {("shell.py", "_commit")},
    r"\btime\.time(_ns)?\b": {("timing.py", None)},
}
LIVE_SITE = f"{RUN_ID}:PL-LOC-000001"


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
    """The three managed households, one of them live (its deadline chain runs on the fake
    clock), so every edge is used."""
    gen = generate(START, HOURS, 8, False)
    params = {**gen.params, **live({LIVE_SITE: gen.params[LIVE_SITE]})}
    net = Network(gen, params)
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


# ── spec 012: the deadline and strategy-change handlers, the confined clock (T20) ───────────────


def test_ac_012_09_the_scan_covers_the_timing_log() -> None:
    assert "timing.py" in _actor_files()


def _enclosing(tree: ast.Module, line: int) -> str | None:
    """The name of the innermost function around ``line``, or None at module level."""
    best: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            end = node.end_lineno or node.lineno
            if node.lineno <= line <= end and (best is None or node.lineno > best.lineno):
                best = node
    return None if best is None else best.name


def _uses(pattern: str) -> set[tuple[str, str | None]]:
    """Every (file, enclosing function) of ``src/ems/actors/`` where ``pattern`` occurs."""
    found: set[tuple[str, str | None]] = set()
    for name in _actor_files():
        text = (ACTORS / name).read_text()
        tree = ast.parse(text)
        for match in re.finditer(pattern, text):
            line = text.count("\n", 0, match.start()) + 1
            found.add((name, _enclosing(tree, line)))
    return found


@pytest.mark.parametrize("pattern", sorted(CONFINED))
def test_ac_012_09_the_clock_and_the_delay_stay_on_the_deadline_path(pattern: str) -> None:
    """Each confined pattern occurs, and only where it is allowed."""
    allowed = CONFINED[pattern]
    uses = _uses(pattern)
    assert uses, pattern  # not vacuous: the deadline path uses it
    outside = {
        (name, fn) for name, fn in uses if (name, fn) not in allowed and (name, None) not in allowed
    }
    assert outside == set(), (pattern, sorted(outside, key=str))


def test_ac_012_19_the_timing_log_makes_no_context_call_and_imports_no_restate() -> None:
    text = (ACTORS / "timing.py").read_text()
    assert not re.search(r"\bctx\b", text)
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".")[0]
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.ImportFrom)
    }
    assert "restate" not in imported


def _imports_timing(text: str) -> bool:
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.ImportFrom):
            if node.module == "ems.actors.timing":
                return True
            if node.module == "ems.actors" and any(a.name == "timing" for a in node.names):
                return True
        if isinstance(node, ast.Import) and any(a.name == "ems.actors.timing" for a in node.names):
            return True
    return False


def test_ac_012_19_only_shell_and_app_use_the_timing_log() -> None:
    users = {name for name in _actor_files() if _imports_timing((ACTORS / name).read_text())}
    assert users == {"shell.py", "app.py"}


@pytest.mark.parametrize(
    ("handler", "entry"),
    [("on_deadline", "shell.handle_timed("), ("set_strategies", "shell.handle(")],
)
def test_ac_012_09_the_new_household_handlers_use_their_shell_entry_points(
    handler: str, entry: str
) -> None:
    assert entry in inspect.getsource(_handler("Household", handler).fn)


def test_ac_012_12_set_strategies_decodes_the_operator_body() -> None:
    change = messages.StrategyChange(1_992_768, ("NoBattery", "SelfConsumption"))
    raw = serde_for(messages.StrategyChange).serialize(change)
    decoded = _handler("Household", "set_strategies").handler_io.input_serde.deserialize(raw)
    assert decoded == change and type(decoded) is messages.StrategyChange


def test_ac_012_07_the_live_household_sends_itself_deadlines(network: Network) -> None:
    """Not vacuous: the live household's chain ran to its end on the fake clock, with delayed
    sends; the sim households sent no deadline."""
    deadlines = [s for s in network.sent if (s.service, s.handler) == ("Household", "on_deadline")]
    assert {s.key for s in deadlines} == {LIVE_SITE}
    assert deadlines[0].delay_s is None  # the first one, on the snapshot
    assert all(s.delay_s is not None and s.delay_s >= 1.0 for s in deadlines[1:])
    clock = network.state("Household", LIVE_SITE).clock
    assert clock.stopped and clock.firings == len(deadlines)
    sim = [k for k in network.households if k != LIVE_SITE]
    assert sim and all(network.state("Household", k).clock == type(clock)() for k in sim)
