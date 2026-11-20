"""AC-011-01: ``reference.quarter`` is the composition of ``prices``, ``decide``, ``apply`` and
``grid_kwh`` and returns the bits of its spec 009 body, kept verbatim below (spec 011, plan §2).

The window is the spec 009 one (``ref_window``): 964 quarter-hours of PL-LOC-000001 with an RCE and
an RCEm cohort. ``Cycle`` returns every action kind, with levels and setpoints, in three-hour blocks
long enough to clamp the battery at both SoC bounds; ``Echo`` turns every field of the state and the
observation into its setpoint, so a wrong argument to ``decide`` changes the result.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from ems.domain.actions import (
    POLICIES,
    Action,
    ActionKind,
    BatteryState,
    NoBattery,
    Observation,
    Policy,
    SelfConsumption,
    resolve,
)
from ems.domain.battery import step_params
from ems.domain.calendar import civil_month
from ems.domain.netbilling import PriceBasis, RcemTable, basis_for
from ems.domain.reference import (
    Prices,
    QuarterIn,
    QuarterOut,
    RefState,
    SiteModel,
    apply,
    decide,
    initial_state,
    prices,
    quarter,
    rcem_used,
)
from ems.domain.settlement import grid_kwh, price_signal
from ems.domain.tariff import buy_price
from ems.domain.units import DT_H
from tests.unit.domain.ref_window import SOC0_KWH, Window, bits, spec009_window


def _quarter_spec009(
    model: SiteModel, policy: Policy, rcem: RcemTable, state: RefState, q: QuarterIn
) -> QuarterOut:
    """The physics of one quarter-hour, shared by ``run`` and ``stepper.step``."""
    buy = buy_price(model.tariff, q.t)
    basis = basis_for(model.cohort, civil_month(q.t))
    if basis is PriceBasis.RCE:
        signal_p = export_p = q.rce_pln_per_kwh
    else:
        _entry, pub, own = rcem_used(rcem, q.t)
        signal_p = pub.value_pln_per_kwh
        export_p = own.billing_value
    signal = price_signal(buy, signal_p, model.rules)
    obs = Observation(
        q.t, signal.buy_pln_per_kwh, signal.sell_pln_per_kwh, state.last_pv_kwh, state.last_load_kwh
    )
    action = policy.decide(BatteryState(state.soc_kwh), obs)
    cmd = resolve(action, q.pv_kwh, q.load_kwh, model.battery.p_max_kw)
    soc, p = step_params(state.soc_kwh, cmd, DT_H, model.battery)
    grid = (q.load_kwh - q.pv_kwh) + p * DT_H
    return QuarterOut(q.t, action, cmd, p, soc, grid, buy, basis, signal.sell_pln_per_kwh, export_p)


def _composed(
    model: SiteModel, policy: Policy, rcem: RcemTable, state: RefState, q: QuarterIn
) -> QuarterOut:
    """The split as ``Ems``, ``Battery`` and ``Meter`` call it (plan §2), composed by hand."""
    pr = prices(model, rcem, q.t, q.rce_pln_per_kwh)
    action = decide(policy, pr, q.t, state.soc_kwh, state.last_pv_kwh, state.last_load_kwh)
    cmd, p, soc = apply(model.battery, state.soc_kwh, action, q.pv_kwh, q.load_kwh)
    grid = grid_kwh(q.pv_kwh, q.load_kwh, p)
    return QuarterOut(
        q.t,
        action,
        cmd,
        p,
        soc,
        grid,
        pr.buy,
        pr.basis,
        pr.signal.sell_pln_per_kwh,
        pr.export_price_pln_per_kwh,
    )


@dataclass(frozen=True)
class Cycle:
    """Each action for ``block_q`` quarter-hours in turn, keyed by ``t``."""

    actions: tuple[Action, ...]
    block_q: int = 12

    def decide(self, state: BatteryState, obs: Observation) -> Action:
        return self.actions[(obs.t // self.block_q) % len(self.actions)]


CYCLE = Cycle(
    (
        Action(ActionKind.CHARGE_FROM_GRID, level=1.0),  # to soc_max, then clamped
        Action(ActionKind.HOLD),
        Action(ActionKind.EXPORT, level=1.0),  # to soc_min, then clamped
        Action(ActionKind.SELF_CONSUME),
        Action(ActionKind.SETPOINT, p_kw=7.5),  # beyond p_max (5 kW)
        Action(ActionKind.DISCHARGE_TO_LOAD),
        Action(ActionKind.SETPOINT, p_kw=-7.5),
        Action(ActionKind.CHARGE_FROM_PV),
        Action(ActionKind.CHARGE_FROM_GRID, level=0.4),
        Action(ActionKind.EXPORT, level=0.6),
        Action(ActionKind.SETPOINT, p_kw=1.25),
        Action(ActionKind.SETPOINT, p_kw=-0.8),
    )
)


@dataclass(frozen=True)
class Echo:
    """A setpoint built from every input of ``decide``."""

    def decide(self, state: BatteryState, obs: Observation) -> Action:
        p_kw = (
            (state.soc_kwh - 5.0)
            + 2.0 * obs.buy_pln_per_kwh
            - 3.0 * obs.sell_pln_per_kwh
            + 4.0 * obs.last_pv_kwh
            - 5.0 * obs.last_load_kwh
            + 0.1 * (obs.t % 7)
        )
        return Action(ActionKind.SETPOINT, p_kw=p_kw)


POLICY_CASES: dict[str, Policy] = {
    "no-battery": NoBattery(),
    "self-consumption": SelfConsumption(),
    "cycle": CYCLE,
    "echo": Echo(),
}


@pytest.fixture(scope="module")
def window() -> Window:
    return spec009_window()


def _walk(
    window: Window,
    basis: PriceBasis,
    policy: Policy,
    check: Callable[[SiteModel, RefState, QuarterIn, QuarterOut], None],
) -> list[QuarterOut]:
    """Run the spec 009 body over the window and call ``check`` on every quarter-hour."""
    model = window.model(basis)
    state = initial_state(model, window.inputs[0].t, SOC0_KWH)
    outs: list[QuarterOut] = []
    for q in window.inputs:
        ref = _quarter_spec009(model, policy, window.proxies, state, q)
        check(model, state, q, ref)
        outs.append(ref)
        state = RefState(ref.soc_kwh, q.pv_kwh, q.load_kwh, state.acc)
    return outs


@pytest.mark.parametrize("basis", [PriceBasis.RCE, PriceBasis.RCEM])
@pytest.mark.parametrize("policy", POLICY_CASES.values(), ids=POLICY_CASES.keys())
def test_ac_011_01_quarter_returns_the_bits_of_the_spec_009_body(
    window: Window, basis: PriceBasis, policy: Policy
) -> None:
    def check(model: SiteModel, state: RefState, q: QuarterIn, ref: QuarterOut) -> None:
        out = quarter(model, policy, window.proxies, state, q)
        assert out == ref, q.t
        assert bits(out) == bits(ref), q.t

    outs = _walk(window, basis, policy, check)
    assert len(outs) == 964
    assert {o.basis for o in outs} == {basis}


@pytest.mark.parametrize("basis", [PriceBasis.RCE, PriceBasis.RCEM])
@pytest.mark.parametrize("policy", POLICY_CASES.values(), ids=POLICY_CASES.keys())
def test_ac_011_01_prices_decide_apply_grid_compose_to_quarter(
    window: Window, basis: PriceBasis, policy: Policy
) -> None:
    def check(model: SiteModel, state: RefState, q: QuarterIn, ref: QuarterOut) -> None:
        by_hand = _composed(model, policy, window.proxies, state, q)
        assert bits(by_hand) == bits(ref), q.t
        assert bits(quarter(model, policy, window.proxies, state, q)) == bits(by_hand), q.t
        pr = prices(model, window.proxies, q.t, q.rce_pln_per_kwh)
        assert isinstance(pr, Prices)
        assert pr.signal.buy_pln_per_kwh == pr.buy.energy_pln_per_kwh + pr.buy.dist_pln_per_kwh
        assert pr.signal.sell_pln_per_kwh == ref.sell_signal_pln_per_kwh

    _walk(window, basis, policy, check)


@pytest.mark.parametrize("basis", [PriceBasis.RCE, PriceBasis.RCEM])
def test_ac_011_01_the_cycle_covers_every_kind_and_clamps_at_both_bounds(
    window: Window, basis: PriceBasis
) -> None:
    """The comparison above is not vacuous: every kind is decided, and the battery is clamped at
    soc_max while charging and at soc_min while discharging."""
    outs = _walk(window, basis, CYCLE, lambda *_: None)
    b = window.model(basis).battery
    assert {o.action.kind for o in outs} == set(ActionKind)
    assert any(o.soc_kwh == b.soc_max_kwh and 0.0 <= o.p_kw < o.p_cmd_kw for o in outs)
    assert any(o.soc_kwh == b.soc_min_kwh and o.p_cmd_kw < o.p_kw <= 0.0 for o in outs)
    assert any(o.p_cmd_kw > b.p_max_kw and o.p_kw == b.p_max_kw for o in outs)


def test_ac_011_01_grid_kwh_is_load_minus_pv_plus_battery_energy() -> None:
    for pv_kwh, load_kwh, p_kw in [(0.0, 0.3, 0.0), (1.7, 0.4, 5.0), (0.2, 0.9, -3.3)]:
        expected = (load_kwh - pv_kwh) + p_kw * DT_H
        assert bits(grid_kwh(pv_kwh, load_kwh, p_kw)) == bits(expected)


def test_ac_011_01_policies_by_name() -> None:
    assert set(POLICIES) == {"NoBattery", "SelfConsumption"}
    assert isinstance(POLICIES["NoBattery"], NoBattery)
    assert isinstance(POLICIES["SelfConsumption"], SelfConsumption)
    obs = Observation(0, 1.0, 0.5, 0.4, 0.2)
    assert POLICIES["NoBattery"].decide(BatteryState(5.0), obs) == Action(ActionKind.HOLD)
    assert POLICIES["SelfConsumption"].decide(BatteryState(5.0), obs) == Action(
        ActionKind.SELF_CONSUME
    )
