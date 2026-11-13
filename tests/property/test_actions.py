"""AC-009-15, -16 properties: grid bounds of mode actions after clamping; policy determinism."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ems.domain.actions import (
    Action,
    ActionKind,
    BatteryState,
    NoBattery,
    Observation,
    SelfConsumption,
    resolve,
)
from ems.domain.battery import BatteryParams, step_params
from ems.domain.units import DT_H

TOL = 1e-12
energy = st.floats(min_value=0.0, max_value=5.0, allow_nan=False)
PARAMS = BatteryParams(capacity_kwh=10.0, p_max_kw=5.0, eta=0.95, soc_min_kwh=1.0, soc_max_kwh=9.5)


def _grid(kind: ActionKind, pv: float, load: float, soc: float) -> float:
    cmd = resolve(Action(kind), pv, load, PARAMS.p_max_kw)
    _soc, p = step_params(soc, cmd, DT_H, PARAMS)
    return load - pv + p * DT_H


@given(pv=energy, load=energy, soc=st.floats(min_value=1.0, max_value=9.5))
def test_charge_from_pv_never_imports_for_the_battery(pv: float, load: float, soc: float) -> None:
    assert _grid(ActionKind.CHARGE_FROM_PV, pv, load, soc) <= max(load - pv, 0.0) + TOL


@given(pv=energy, load=energy, soc=st.floats(min_value=1.0, max_value=9.5))
def test_discharge_to_load_never_exports_battery_energy(pv: float, load: float, soc: float) -> None:
    assert _grid(ActionKind.DISCHARGE_TO_LOAD, pv, load, soc) >= min(load - pv, 0.0) - TOL


@given(pv=energy, load=energy, soc=st.floats(min_value=1.0, max_value=9.5))
def test_self_consume_satisfies_both_bounds(pv: float, load: float, soc: float) -> None:
    g = _grid(ActionKind.SELF_CONSUME, pv, load, soc)
    assert min(load - pv, 0.0) - TOL <= g <= max(load - pv, 0.0) + TOL


@given(
    t=st.integers(min_value=0, max_value=10**7),
    buy=st.floats(min_value=0.0, max_value=3.0),
    sell=st.floats(min_value=0.0, max_value=3.0),
    pv=energy,
    load=energy,
    soc=st.floats(min_value=0.0, max_value=10.0),
)
def test_policies_are_deterministic(
    t: int, buy: float, sell: float, pv: float, load: float, soc: float
) -> None:
    obs = Observation(t, buy, sell, pv, load)
    for policy in (NoBattery(), SelfConsumption()):
        a = policy.decide(BatteryState(soc), obs)
        b = policy.decide(BatteryState(soc), Observation(t, buy, sell, pv, load))
        assert a == b
