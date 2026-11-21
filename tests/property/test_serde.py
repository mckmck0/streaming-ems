"""AC-011-33: any value of a stored or sent type round-trips through ``serde_for`` bit for bit.

Values are drawn from the types themselves: every dataclass field gets a strategy for its type
(arbitrary finite floats, -0.0 and subnormals included; 64-bit ints; text; dates; enums; tuples of
up to three items; ``X | None``; ``Inbox[M]`` with ``M`` bound). Types whose ``__post_init__``
checks their values (``Action``, ``Tranche``) get strategies that keep to those checks, as a value
built by the code does. ``RunParams`` is drawn around a real record (its ``SiteModel`` is
configuration).

The decoded value equals the original, every leaf is identical (floats as IEEE-754 bytes, counters
outside ``==`` included) and it encodes to the same bytes again.
"""

from __future__ import annotations

import dataclasses
import struct
import types
import typing
from datetime import date
from enum import Enum
from typing import Any

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from ems.actors.messages import Ack, Apply, Flows, ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.serde import serde_for
from ems.assembler.join import Step
from ems.assembler.params import MAX_LATE_SLOTS, RunParams
from ems.domain.actions import Action, ActionKind
from ems.domain.calendar import month_add
from ems.domain.netbilling import PriceBasis
from ems.domain.settlement import Correction, Tranche
from tests.network import HOURS, START, STATE_TYPES, managed_params

MESSAGE_TYPES = (Step, PriceUpdate, ForecastUpdate, Subscribe, Apply, Ack, Flows)
STATE_AND_MESSAGE_TYPES = (*dict.fromkeys(STATE_TYPES.values()), *MESSAGE_TYPES)

finite_floats = st.floats(allow_nan=False, allow_infinity=False)  # ±0.0, subnormals, extremes
int64s = st.integers(min_value=-(2**63), max_value=2**63 - 1)
short_texts = st.text(max_size=8)
months = st.builds(
    lambda y, m: f"{y:04d}-{m:02d}",
    st.integers(min_value=2000, max_value=2100),
    st.integers(min_value=1, max_value=12),
)
EDGE_FLOATS = (
    -0.0,
    0.0,
    5e-324,  # the smallest subnormal
    -5e-324,
    2.2250738585072009e-308,  # the largest subnormal
    2.2250738585072014e-308,  # the smallest normal
    1.7976931348623157e308,
    -1.7976931348623157e308,
    0.1,
    1 / 3,
)

_PLAIN_KINDS = (
    ActionKind.HOLD,
    ActionKind.SELF_CONSUME,
    ActionKind.CHARGE_FROM_PV,
    ActionKind.DISCHARGE_TO_LOAD,
)
actions = st.one_of(
    st.sampled_from(_PLAIN_KINDS).map(Action),
    st.builds(
        lambda kind, level: Action(kind, level=level),
        st.sampled_from((ActionKind.CHARGE_FROM_GRID, ActionKind.EXPORT)),
        st.floats(min_value=0.0, max_value=1.0, exclude_min=True),  # (0, 1], subnormals included
    ),
    st.builds(lambda p: Action(ActionKind.SETPOINT, p_kw=p), finite_floats),
)


@st.composite
def tranches(draw: st.DrawFn) -> Tranche:
    created = draw(months)
    amount = draw(st.floats(min_value=0.0, allow_infinity=False))
    remaining = draw(st.floats(min_value=0.0, max_value=amount))
    cap = draw(st.floats(min_value=0.0, allow_infinity=False))
    basis = draw(st.sampled_from(PriceBasis))
    return Tranche(created, month_add(created, 1), amount, remaining, cap, basis)


OVERRIDES: dict[Any, st.SearchStrategy[Any]] = {Action: actions, Tranche: tranches()}


def values_of(tp: Any, env: dict[Any, Any] | None = None) -> st.SearchStrategy[Any]:
    """A strategy for values of the type ``tp`` (``env`` binds the type parameters of a generic)."""
    env = env or {}
    if isinstance(tp, typing.TypeVar):
        return values_of(env[tp])
    if tp in OVERRIDES:
        return OVERRIDES[tp]
    origin, args = typing.get_origin(tp), typing.get_args(tp)
    if origin in (types.UnionType, typing.Union):
        return st.one_of(*(values_of(a, env) for a in args))
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return st.lists(values_of(args[0], env), max_size=3).map(tuple)
        return st.tuples(*(values_of(a, env) for a in args))
    if origin is dict:
        return st.dictionaries(values_of(args[0], env), values_of(args[1], env), max_size=3)
    if origin is not None and dataclasses.is_dataclass(origin):  # e.g. Inbox[Step]
        return _dataclass(origin, dict(zip(origin.__type_params__, args, strict=True)))
    if tp is Any:  # the stream leads (outside ``==``): JSON numbers
        return st.integers(min_value=-(10**6), max_value=10**6)
    if tp is type(None):
        return st.none()
    if tp is bool:
        return st.booleans()
    if tp is int:
        return int64s
    if tp is float:
        return finite_floats
    if tp is str:
        return short_texts
    if tp is date:
        return st.dates()
    if isinstance(tp, type) and issubclass(tp, Enum):
        return st.sampled_from(tp)
    if isinstance(tp, type) and dataclasses.is_dataclass(tp):
        return _dataclass(tp, {})
    raise TypeError(f"no strategy for {tp!r}")


def _dataclass(cls: type[Any], env: dict[Any, Any]) -> st.SearchStrategy[Any]:
    hints = typing.get_type_hints(cls)
    fields = {f.name: values_of(hints[f.name], env) for f in dataclasses.fields(cls) if f.init}
    return st.builds(cls, **fields)


def _leaves(value: Any, path: str = "") -> list[tuple[str, str, Any]]:
    """Every leaf with its path and type; floats as IEEE-754 bytes; all fields included."""
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
    return [(path, type(value).__name__, value)]


def _round_trip(tp: type[Any], value: Any) -> None:
    serde = serde_for(tp)
    raw = serde.serialize(value)
    back = serde.deserialize(raw)
    assert type(back) is tp
    assert back == value
    assert _leaves(back) == _leaves(value)
    assert serde.serialize(back) == raw


@pytest.mark.parametrize("tp", STATE_AND_MESSAGE_TYPES, ids=lambda tp: tp.__name__)
@given(data=st.data())
def test_ac_011_33_any_value_round_trips_bit_for_bit(tp: type[Any], data: st.DataObject) -> None:
    _round_trip(tp, data.draw(values_of(tp), label=tp.__name__))


@given(x=finite_floats, t=int64s)
@example(x=-0.0, t=0)
@example(x=5e-324, t=-1)
@example(x=-5e-324, t=2**63 - 1)
@example(x=2.2250738585072009e-308, t=-(2**63))
@example(x=1.7976931348623157e308, t=1)
def test_ac_011_33_every_float_field_of_a_message_keeps_its_bits(x: float, t: int) -> None:
    corrections = (Correction("production", t, x), Correction("consumption", t, -x))
    _round_trip(Ack, Ack(t, x))
    _round_trip(Flows, Flows(t, x, -x, x, x, corrections))
    _round_trip(Apply, Apply(t, Action(ActionKind.SETPOINT, p_kw=x), x, x, -x, corrections))


@pytest.mark.parametrize("x", EDGE_FLOATS)
def test_ac_011_33_edge_floats_keep_their_bits(x: float) -> None:
    _round_trip(Ack, Ack(1, x))
    _round_trip(Step, Step(1, x, x, x, False, True, False, None, (Correction("x", 0, x),)))


@given(data=st.data())
def test_ac_011_33_run_params_round_trip_bit_for_bit(data: st.DataObject) -> None:
    real = data.draw(st.sampled_from(sorted(managed_params(START, HOURS, 8).items())))[1]
    b = real.model.battery
    first_hour = data.draw(st.integers(min_value=0, max_value=2**40), label="first_hour")
    params = dataclasses.replace(
        real,
        run_id=data.draw(st.none() | st.just(real.run_id), label="run_id"),
        first_step=4 * first_hour,
        end_step=4 * first_hour + data.draw(st.integers(min_value=1, max_value=10**6)),
        late_slots=data.draw(st.integers(min_value=1, max_value=MAX_LATE_SLOTS)),
        initial_soc_kwh=data.draw(st.floats(min_value=b.soc_min_kwh, max_value=b.soc_max_kwh)),
    )
    _round_trip(RunParams, params)
