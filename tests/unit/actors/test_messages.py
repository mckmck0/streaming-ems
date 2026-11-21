"""AC-011-32: the internal messages and their envelope (plan, Actor contract, Messages).

Every message carries ``schema_version`` ("1.0.0") and ``t`` (the sim step); price and forecast
updates also carry ``seq``; the control record ``RunParams`` carries ``schema_version`` and no
``t``. The field names of every message and of every type nested in it are pinned here, in order:
a rename, a removal or a reordering fails this test (within version 1 a new field must be
optional, with a default, and added here deliberately).
"""

from __future__ import annotations

import dataclasses
import types
import typing
from enum import Enum
from typing import Any

import pytest

from ems.actors.messages import Ack, Apply, Flows, ForecastUpdate, PriceUpdate, Subscribe
from ems.assembler.join import Step
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import Curve, Run
from ems.domain.actions import Action, ActionKind
from ems.domain.settlement import Correction

MESSAGES: dict[type[Any], tuple[str, ...]] = {
    Step: (
        "t",
        "pv_kwh",
        "load_kwh",
        "rce_pln_per_kwh",
        "pv_estimated",
        "load_estimated",
        "price_replaced",
        "forecast_issue_s",
        "corrections",
        "schema_version",
    ),
    PriceUpdate: ("t", "seq", "snapshot", "curves", "schema_version"),
    ForecastUpdate: ("t", "seq", "run", "schema_version"),
    Subscribe: ("t", "household_key", "schema_version"),
    Apply: (
        "t",
        "action",
        "pv_kwh",
        "load_kwh",
        "rce_pln_per_kwh",
        "corrections",
        "schema_version",
    ),
    Ack: ("t", "soc_kwh", "schema_version"),
    Flows: ("t", "pv_kwh", "load_kwh", "p_kw", "rce_pln_per_kwh", "corrections", "schema_version"),
}
NESTED: dict[type[Any], tuple[str, ...]] = {
    Curve: ("seq", "t0", "day", "rce_pln_per_kwh", "issued_s", "visible_step"),
    Run: ("seq", "issue_s", "visible_step"),
    Correction: ("stream", "t", "kwh"),
    Action: ("kind", "level", "p_kw"),
}
RUN_PARAMS = (
    "run_id",
    "location_id",
    "household_key",
    "market_key",
    "first_step",
    "end_step",
    "late_slots",
    "strategies",
    "live",
    "has_pv",
    "initial_soc_kwh",
    "model",
    "rcem",
    "schema_version",
)
UPDATES = (PriceUpdate, ForecastUpdate)


def _names(tp: type[Any]) -> tuple[str, ...]:
    return tuple(f.name for f in dataclasses.fields(tp))


def _default(tp: type[Any], name: str) -> Any:
    return next(f.default for f in dataclasses.fields(tp) if f.name == name)


@pytest.mark.parametrize("tp", [*MESSAGES, *NESTED], ids=lambda tp: tp.__name__)
def test_ac_011_32_field_names_are_pinned_in_order(tp: type[Any]) -> None:
    assert dataclasses.is_dataclass(tp)
    assert _names(tp) == {**MESSAGES, **NESTED}[tp]


def test_ac_011_32_run_params_fields_are_pinned_in_order() -> None:
    assert _names(RunParams) == RUN_PARAMS


@pytest.mark.parametrize("tp", [*MESSAGES, *NESTED, RunParams], ids=lambda tp: tp.__name__)
def test_ac_011_32_messages_and_nested_types_are_frozen(tp: type[Any]) -> None:
    assert tp.__dataclass_params__.frozen


@pytest.mark.parametrize("tp", MESSAGES, ids=lambda tp: tp.__name__)
def test_ac_011_32_every_message_carries_schema_version_and_an_int_t(tp: type[Any]) -> None:
    hints = typing.get_type_hints(tp)
    assert hints["t"] is int
    assert hints["schema_version"] is str
    assert _default(tp, "schema_version") == "1.0.0"
    assert _names(tp)[0] == "t" and _names(tp)[-1] == "schema_version"


@pytest.mark.parametrize("tp", UPDATES, ids=lambda tp: tp.__name__)
def test_ac_011_32_price_and_forecast_updates_carry_an_int_seq(tp: type[Any]) -> None:
    assert typing.get_type_hints(tp)["seq"] is int


def test_ac_011_32_run_params_carries_schema_version_and_no_t() -> None:
    hints = typing.get_type_hints(RunParams)
    assert "t" not in hints
    assert hints["schema_version"] is str
    assert _default(RunParams, "schema_version") == "1.0.0"


def test_ac_011_32_action_kinds_are_pinned() -> None:
    """``Apply`` carries the action kind by value: renaming one changes the message."""
    assert [k.value for k in ActionKind] == [
        "hold",
        "self_consume",
        "charge_from_pv",
        "charge_from_grid",
        "discharge_to_load",
        "export",
        "setpoint",
    ]


def _reachable(tp: Any, seen: set[type[Any]]) -> None:
    """Every dataclass and enum reachable from ``tp`` through field types and type arguments."""
    for arg in typing.get_args(tp):
        _reachable(arg, seen)
    origin = typing.get_origin(tp)
    if origin is not None and origin not in (tuple, list, dict, types.UnionType, typing.Union):
        _reachable(origin, seen)
    if isinstance(tp, type) and tp not in seen:
        if dataclasses.is_dataclass(tp):
            seen.add(tp)
            for hint in typing.get_type_hints(tp).values():
                _reachable(hint, seen)
        elif issubclass(tp, Enum):
            seen.add(tp)


def test_ac_011_32_no_type_nested_in_a_message_escapes_the_pins() -> None:
    seen: set[type[Any]] = set()
    for tp in MESSAGES:
        _reachable(tp, seen)
    assert seen == {*MESSAGES, *NESTED, ActionKind}
