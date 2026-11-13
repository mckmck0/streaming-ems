"""The action set and ``Policy``: what a strategy decides, and the one command it resolves to.

Battery actions plus a setpoint (spec 009). Mode actions (SELF_CONSUME, CHARGE_FROM_PV,
DISCHARGE_TO_LOAD) resolve with the quarter's realised PV and load: they model the inverter's local
control, while the decision itself sees only the observation. ``battery.step`` clamps every command.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from ems.domain.units import DT_H


class ActionKind(StrEnum):
    HOLD = "hold"
    SELF_CONSUME = "self_consume"
    CHARGE_FROM_PV = "charge_from_pv"
    CHARGE_FROM_GRID = "charge_from_grid"
    DISCHARGE_TO_LOAD = "discharge_to_load"
    EXPORT = "export"
    SETPOINT = "setpoint"


_LEVELLED = (ActionKind.CHARGE_FROM_GRID, ActionKind.EXPORT)


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    level: float | None = None  # (0, 1] of p_max, for CHARGE_FROM_GRID and EXPORT
    p_kw: float | None = None  # AC power for SETPOINT, > 0 charges

    def __post_init__(self) -> None:
        if self.kind in _LEVELLED:
            if self.level is None or not (0.0 < self.level <= 1.0):
                raise ValueError(f"{self.kind} needs a level in (0, 1], got {self.level}")
        elif self.level is not None:
            raise ValueError(f"{self.kind} takes no level")
        if self.kind is ActionKind.SETPOINT:
            if self.p_kw is None or not math.isfinite(self.p_kw):
                raise ValueError(f"setpoint needs a finite p_kw, got {self.p_kw}")
        elif self.p_kw is not None:
            raise ValueError(f"{self.kind} takes no p_kw")


def resolve(
    action: Action, pv_kwh: float, load_kwh: float, p_max_kw: float, dt_h: float = DT_H
) -> float:
    """The battery command (kW, > 0 charges) for one quarter-hour, before clamping."""
    k = action.kind
    surplus = pv_kwh - load_kwh
    if k is ActionKind.HOLD:
        return 0.0
    if k is ActionKind.SELF_CONSUME:
        return surplus / dt_h
    if k is ActionKind.CHARGE_FROM_PV:
        return surplus / dt_h if surplus > 0.0 else 0.0
    if k is ActionKind.DISCHARGE_TO_LOAD:
        return surplus / dt_h if surplus < 0.0 else 0.0
    if k is ActionKind.CHARGE_FROM_GRID:
        return (action.level or 0.0) * p_max_kw
    if k is ActionKind.EXPORT:
        return -(action.level or 0.0) * p_max_kw
    return action.p_kw or 0.0


@dataclass(frozen=True)
class BatteryState:
    soc_kwh: float


@dataclass(frozen=True)
class Observation:
    """Only what is known at the start of q. Forecasts are added later, additively."""

    t: int
    buy_pln_per_kwh: float  # signal for q
    sell_pln_per_kwh: float  # signal for q
    last_pv_kwh: float  # last completed quarter-hour (0.0 before the first)
    last_load_kwh: float


class Policy(Protocol):
    def decide(self, state: BatteryState, obs: Observation) -> Action: ...


@dataclass(frozen=True)
class NoBattery:
    def decide(self, state: BatteryState, obs: Observation) -> Action:
        return Action(ActionKind.HOLD)


@dataclass(frozen=True)
class SelfConsumption:
    def decide(self, state: BatteryState, obs: Observation) -> Action:
        return Action(ActionKind.SELF_CONSUME)
