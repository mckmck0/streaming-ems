"""AC-011-33: state, parameters and messages round-trip through the one serde (``serde_for``).

Real values of every stored and sent type are taken from the in-memory network
(``tests/network.py``) over a generated 6 h window of the 10-household catalog: the run parameters
of the three managed
sites, every object's start state, the states half-way through a run (with held messages) and at
its end (October closed), and every message sent. Each decodes to a value equal to the original,
with every float bit-identical (compared as IEEE-754 bytes, so -0.0 and 0.0 differ), with the
counters outside ``==`` kept, and encodes again to the same bytes. Empty bytes decode to None.

No type reachable from these, through dataclass fields and type arguments, is a ``set`` or a
``frozenset``: their order depends on the hash seed, and a retried handler that wrote other bytes
would fail its journal check (A11).
"""

from __future__ import annotations

import collections.abc
import dataclasses
import struct
import types
import typing
from datetime import date
from typing import Any

import pytest

from ems.actors.messages import Ack, Apply, Flows, ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.serde import DataclassSerde, serde_for
from ems.actors.tariff import TariffState
from ems.assembler.join import Step
from ems.assembler.params import RunParams
from ems.domain.tariff import National
from tests.network import HOURS, START, STARTS, STATE_TYPES, Network, Order, generate

MESSAGE_TYPES = (Step, PriceUpdate, ForecastUpdate, Subscribe, Apply, Ack, Flows)
STORED_AND_SENT = (*dict.fromkeys(STATE_TYPES.values()), RunParams, *MESSAGE_TYPES)
LATE_SLOTS = 8


def _leaves(value: Any, path: str = "") -> list[tuple[str, str, Any]]:
    """Every leaf of a value with its path and type; floats as IEEE-754 bytes. Fields outside
    ``==`` (``compare=False``) are included."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        out = [(path, "class", type(value).__qualname__)]
        for f in dataclasses.fields(value):
            out += _leaves(getattr(value, f.name), f"{path}.{f.name}")
        return out
    if isinstance(value, (tuple, list)):
        out = [(path, type(value).__name__, len(value))]
        for i, item in enumerate(value):
            out += _leaves(item, f"{path}[{i}]")
        return out
    if isinstance(value, dict):
        out = [(path, "dict", tuple(value))]
        for k, item in value.items():
            out += _leaves(item, f"{path}[{k!r}]")
        return out
    if isinstance(value, float):
        return [(path, "float", struct.pack(">d", value))]
    if isinstance(value, (set, frozenset)):
        raise AssertionError(f"{path}: a {type(value).__name__} in a stored value")
    return [(path, type(value).__name__, value)]


@pytest.fixture(scope="module")
def values() -> dict[type[Any], list[Any]]:
    return stored_and_sent_values()


def stored_and_sent_values() -> dict[type[Any], list[Any]]:
    """The values of the ``values`` fixture; the 1.0.0 state fixtures of spec 012 start from them
    (``tests/fixtures/state/generate.py``)."""
    gen = generate(START, HOURS, LATE_SLOTS, False)
    out: dict[type[Any], list[Any]] = {tp: [] for tp in STORED_AND_SENT}
    for params in gen.params.values():
        out[RunParams].append(params)
        for service, start in STARTS.items():
            out[STATE_TYPES[service]].append(start(params)[0])
    out[TariffState].append(TariffState())

    half = Network(gen)
    half.run(Order(seed=3, weights=(1, 1, 1, 1, 1), pick="newest"), limit=450)
    full = Network(gen)
    full.run(Order(seed=4, weights=(1, 1, 1, 1, 1), pick="any"))
    for net in (half, full):
        for service, key in net.stored:
            out[STATE_TYPES[service]].append(net.state(service, key))
    for s in full.sent:
        out[type(s.message)].append(s.message)
    return out


def _held(value: Any) -> int:
    return sum(
        len(getattr(value, f.name).held)
        for f in dataclasses.fields(value)
        if hasattr(getattr(value, f.name), "held")
    )


def test_ac_011_33_the_values_cover_every_type_and_held_messages(
    values: dict[type[Any], list[Any]],
) -> None:
    assert all(values[tp] for tp in STORED_AND_SENT), [tp for tp in values if not values[tp]]
    assert any(_held(v) for tp in STATE_TYPES.values() for v in values[tp])
    assert any(len(v.result.months) for v in values[STATE_TYPES["SelfConsumption"]])


@pytest.mark.parametrize("tp", STORED_AND_SENT, ids=lambda tp: tp.__name__)
def test_ac_011_33_values_round_trip_equal_with_bit_identical_floats(
    tp: type[Any], values: dict[type[Any], list[Any]]
) -> None:
    serde = serde_for(tp)
    assert isinstance(serde, DataclassSerde)
    for value in values[tp]:
        raw = serde.serialize(value)
        back = serde.deserialize(raw)
        assert type(back) is tp
        assert back == value
        assert _leaves(back) == _leaves(value)
        assert serde.serialize(back) == raw


@pytest.mark.parametrize("tp", STORED_AND_SENT, ids=lambda tp: tp.__name__)
def test_ac_011_33_empty_bytes_decode_to_none(tp: type[Any]) -> None:
    serde = serde_for(tp)
    assert serde.deserialize(b"") is None
    assert serde.serialize(None) == b""


def test_ac_011_33_serde_for_builds_one_serde_per_type() -> None:
    assert serde_for(Step) is serde_for(Step)
    assert serde_for(Step) is not serde_for(Ack)


SET_TYPES = (set, frozenset, collections.abc.Set, collections.abc.MutableSet, typing.AbstractSet)


def _set_types(tp: Any, seen: set[Any], path: str) -> list[str]:
    """Paths to every set-typed field reachable from ``tp`` (fields and type arguments)."""
    origin = typing.get_origin(tp)
    if origin is typing.Literal:
        return []
    if origin in SET_TYPES or (isinstance(tp, type) and issubclass(tp, (set, frozenset))):
        return [path]
    found: list[str] = []
    for i, arg in enumerate(typing.get_args(tp)):
        found += _set_types(arg, seen, f"{path}[{i}]")
    if origin is not None and origin not in (tuple, list, dict, types.UnionType, typing.Union):
        found += _set_types(origin, seen, path)
    if isinstance(tp, type) and dataclasses.is_dataclass(tp) and tp not in seen:
        seen.add(tp)
        for name, hint in typing.get_type_hints(tp).items():
            found += _set_types(hint, seen, f"{tp.__name__}.{name}")
    return found


@pytest.mark.parametrize("tp", STORED_AND_SENT, ids=lambda tp: tp.__name__)
def test_ac_011_33_no_stored_or_sent_type_contains_a_set(tp: type[Any]) -> None:
    assert _set_types(tp, set(), tp.__name__) == []


@dataclasses.dataclass(frozen=True)
class _Inner:
    days: frozenset[date]


@dataclasses.dataclass(frozen=True)
class _Outer:
    a: tuple[_Inner, ...] | None
    b: dict[str, set[int]]


def test_ac_011_33_the_type_walk_finds_a_set() -> None:
    """The walk is not vacuous: a set nested in a dataclass, a tuple and a union is found."""
    assert sorted(_set_types(_Outer, set(), "_Outer")) == ["_Inner.days", "_Outer.b[1]"]


def test_ac_011_33_the_walk_reaches_the_holidays(values: dict[type[Any], list[Any]]) -> None:
    """``National.holidays`` (once a set) is reached from ``RunParams`` and is a sorted tuple."""
    seen: set[Any] = set()
    _set_types(RunParams, seen, "RunParams")
    assert National in seen
    holidays = values[RunParams][0].model.tariff.national.holidays
    assert isinstance(holidays, tuple) and list(holidays) == sorted(set(holidays))
