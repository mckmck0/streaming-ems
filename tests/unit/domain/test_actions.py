"""AC-009-15, -16: actions resolve to one battery command; policies are pure."""

from __future__ import annotations

import dataclasses
import math

import pytest

from ems.domain.actions import (
    Action,
    ActionKind,
    BatteryState,
    NoBattery,
    Observation,
    SelfConsumption,
    resolve,
)

OBS = Observation(
    t=100, buy_pln_per_kwh=1.1, sell_pln_per_kwh=0.4, last_pv_kwh=0.0, last_load_kwh=0.3
)


@pytest.mark.parametrize(
    ("action", "pv", "load", "expected"),
    [
        (Action(ActionKind.HOLD), 1.0, 0.2, 0.0),
        (Action(ActionKind.SELF_CONSUME), 1.0, 0.2, 3.2),
        (Action(ActionKind.SELF_CONSUME), 0.2, 1.0, -3.2),
        (Action(ActionKind.CHARGE_FROM_PV), 1.0, 0.2, 3.2),
        (Action(ActionKind.CHARGE_FROM_PV), 0.2, 1.0, 0.0),
        (Action(ActionKind.DISCHARGE_TO_LOAD), 0.2, 1.0, -3.2),
        (Action(ActionKind.DISCHARGE_TO_LOAD), 1.0, 0.2, 0.0),
        (Action(ActionKind.CHARGE_FROM_GRID, level=0.5), 0.0, 0.0, 2.5),
        (Action(ActionKind.EXPORT, level=1.0), 0.0, 0.0, -5.0),
        (Action(ActionKind.SETPOINT, p_kw=-1.7), 0.0, 0.0, -1.7),
        (Action(ActionKind.SETPOINT, p_kw=99.0), 0.0, 0.0, 99.0),  # the battery model clamps it
    ],
)
def test_ac_009_15_each_kind_resolves(
    action: Action, pv: float, load: float, expected: float
) -> None:
    assert resolve(action, pv, load, p_max_kw=5.0) == pytest.approx(expected, rel=1e-12)


def test_ac_009_15_mode_without_surplus_or_deficit_is_zero() -> None:
    for kind in (ActionKind.SELF_CONSUME, ActionKind.CHARGE_FROM_PV, ActionKind.DISCHARGE_TO_LOAD):
        assert resolve(Action(kind), 0.4, 0.4, p_max_kw=5.0) == 0.0


@pytest.mark.parametrize("level", [0.0, -0.1, 1.0001, math.nan, None])
@pytest.mark.parametrize("kind", [ActionKind.CHARGE_FROM_GRID, ActionKind.EXPORT])
def test_ac_009_15_levels_outside_unit_interval_are_rejected(
    kind: ActionKind, level: float | None
) -> None:
    with pytest.raises(ValueError, match="level"):
        Action(kind, level=level)


def test_ac_009_15_bad_parameters_are_rejected() -> None:
    with pytest.raises(ValueError, match="level"):
        Action(ActionKind.HOLD, level=0.5)
    with pytest.raises(ValueError, match="p_kw"):
        Action(ActionKind.SETPOINT)
    with pytest.raises(ValueError, match="p_kw"):
        Action(ActionKind.SETPOINT, p_kw=math.inf)
    with pytest.raises(ValueError, match="p_kw"):
        Action(ActionKind.EXPORT, level=0.5, p_kw=1.0)


def test_ac_009_16_baselines() -> None:
    state = BatteryState(soc_kwh=5.0)
    assert NoBattery().decide(state, OBS) == Action(ActionKind.HOLD)
    assert SelfConsumption().decide(state, OBS) == Action(ActionKind.SELF_CONSUME)


def test_ac_009_16_observation_fields() -> None:
    names = [f.name for f in dataclasses.fields(Observation)]
    assert names == ["t", "buy_pln_per_kwh", "sell_pln_per_kwh", "last_pv_kwh", "last_load_kwh"]
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(OBS, "t", 1)  # noqa: B010 - frozen: assignment must fail at runtime
