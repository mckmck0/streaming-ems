"""AC-011-02 … -05: late readings correct the ledger, and a civil month closes a lag after it ends
(spec 011, plan §2; ADR 0018).

- Lag 0 without corrections settles as spec 009 at every step of the spec 009 window (local days
  2026-10-24 … 2026-11-02, the October close with opening tranches). The spec 009 bodies of
  ``settlement.settle_quarter`` and ``stepper.step`` are kept below; the step returns a tuple and
  calls the kept ``settle_quarter`` (``quarter()`` is pinned by ``test_quarter_split.py``).
- ``correct``: a quarter of the open hour, of a closed hour, a flow not kept, an unknown stream, a
  future slot, an hour whose month has closed. A correction changes no decision, power or SoC.
- The lag: 16 quarter-hours (``L`` = 8) delay October's close by exactly 16 steps and change nothing
  else; corrections up to ``t + 2L`` reach the close; a September close made from an October step
  uses September's fixed charges and basis (``ref_window.september_window``).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from functools import cache

import pytest

from ems.domain.actions import (
    Action,
    ActionKind,
    BatteryState,
    Observation,
    Policy,
    SelfConsumption,
)
from ems.domain.calendar import civil_month, hour_month
from ems.domain.netbilling import NetBillingRules, PriceBasis, RcemTable, basis_for
from ems.domain.reference import (
    QuarterIn,
    QuarterOut,
    RefState,
    SiteModel,
    initial_state,
    quarter,
    settle_in,
)
from ems.domain.settlement import (
    Correction,
    Flow,
    HourRow,
    MonthSettlement,
    SettleAcc,
    SettleIn,
    Tranche,
    add_quarter,
    close_month,
    correct,
    due_month,
    grid_kwh,
    hour_rows,
    settle_hour,
    settle_month,
    settle_quarter,
)
from ems.domain.stepper import StepOut, settle_step, step
from ems.domain.tariff import FixedCharges, fixed_charges
from ems.domain.units import QUARTERS_PER_HOUR
from tests.unit.domain.ref_window import (
    OPENING,
    SEPTEMBER_OPENING,
    SOC0_KWH,
    Window,
    bits,
    local_t,
    september_window,
    spec009_window,
)

BASES = [PriceBasis.RCE, PriceBasis.RCEM]
OCT_LAST = local_t(date(2026, 11, 1)) - 1  # 2026-10-31T22:45Z, October's last quarter-hour
SEP_LAST = local_t(date(2026, 10, 1)) - 1  # 2026-09-30T21:45Z
L_SLOTS = 8
LAG = 2 * L_SLOTS
SC = SelfConsumption()


# ── spec 009 bodies, kept verbatim ──────────────────────────────────────────────────────────────


def _settle_quarter_spec009(
    acc: SettleAcc,
    s: SettleIn,
    fixed: FixedCharges,
    basis: PriceBasis,
    rules: NetBillingRules,
) -> tuple[SettleAcc, HourRow | None, MonthSettlement | None]:
    """Add one quarter-hour. Its fourth quarter closes the hour; the last hour of a civil month
    closes the month with ``fixed`` and ``basis`` of that month.
    """
    if s.t != acc.t:
        raise ValueError(f"quarter-hour {s.t} out of order: expected {acc.t}")
    hour = (*acc.hour, s)
    rows = acc.month_rows
    tranches = acc.tranches
    row = None
    month = None
    if s.t % QUARTERS_PER_HOUR == QUARTERS_PER_HOUR - 1:
        row = settle_hour(hour)
        hour = ()
        rows = (*rows, row)
        if hour_month(row.h + 1) != row.month:
            month = settle_month(row.month, rows, fixed, basis, rules, tranches)
            rows = ()
            tranches = month.closing
    return SettleAcc(s.t + 1, hour, rows, tranches), row, month


def _step_spec009(
    model: SiteModel, policy: Policy, rcem: RcemTable, state: RefState, q: QuarterIn
) -> tuple[RefState, QuarterOut, HourRow | None, MonthSettlement | None]:
    if q.t != state.acc.t:
        raise ValueError(f"quarter-hour {q.t} out of order: expected {state.acc.t}")
    out = quarter(model, policy, rcem, state, q)
    month = civil_month(q.t)
    acc, row, settled = _settle_quarter_spec009(
        state.acc,
        settle_in(out),
        fixed_charges(model.tariff, month, model.fixed_inputs),
        basis_for(model.cohort, month),
        model.rules,
    )
    return RefState(out.soc_kwh, q.pv_kwh, q.load_kwh, acc), out, row, settled


# ── helpers ─────────────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def w9() -> Window:
    return spec009_window()


def _run(
    model: SiteModel,
    rcem: RcemTable,
    opening: tuple[Tranche, ...],
    inputs: Sequence[QuarterIn],
    lag: int,
    attached: Mapping[int, Sequence[Correction]] | None = None,
    policy: Policy = SC,
) -> list[StepOut]:
    """Step the window as a strategy object does: carried state and kept flows, the corrections
    attached to each step, one lag."""
    state = initial_state(model, inputs[0].t, SOC0_KWH, opening)
    recent: tuple[Flow, ...] = ()
    outs: list[StepOut] = []
    for q in inputs:
        out = step(model, policy, rcem, state, q, recent, (attached or {}).get(q.t, ()), lag)
        outs.append(out)
        state, recent = out.state, out.recent
    return outs


@cache
def _runs(basis: PriceBasis, lag: int) -> tuple[StepOut, ...]:
    w = spec009_window()
    return tuple(_run(w.model(basis), w.proxies, OPENING[basis], w.inputs, lag))


def _final_settle_ins(
    outs: Sequence[StepOut],
    inputs: Sequence[QuarterIn],
    attached: Mapping[int, Sequence[Correction]],
) -> list[SettleIn]:
    """Each quarter-hour as the ledger must end: corrected PV and load, the executed power."""
    pv = {q.t: q.pv_kwh for q in inputs}
    load = {q.t: q.load_kwh for q in inputs}
    for cs in attached.values():
        for c in cs:
            (pv if c.stream == "production" else load)[c.t] = c.kwh
    return [
        SettleIn(
            o.quarter.t,
            grid_kwh(pv[o.quarter.t], load[o.quarter.t], o.quarter.p_kw),
            o.quarter.buy,
            o.quarter.export_price_pln_per_kwh,
        )
        for o in outs
    ]


def _months(outs: Sequence[StepOut]) -> list[tuple[int, MonthSettlement]]:
    return [(o.quarter.t, o.month) for o in outs if o.month is not None]


def _flow(q: QuarterIn, out: QuarterOut) -> Flow:
    return Flow(q.pv_kwh, q.load_kwh, out.p_kw, settle_in(out))


def _corrected(f: Flow, c: Correction) -> Flow:
    pv_kwh = c.kwh if c.stream == "production" else f.pv_kwh
    load_kwh = c.kwh if c.stream == "consumption" else f.load_kwh
    settle = dataclasses.replace(f.settle, grid_kwh=grid_kwh(pv_kwh, load_kwh, f.p_kw))
    return Flow(pv_kwh, load_kwh, f.p_kw, settle)


# ── AC-011-02: lag 0 is spec 009 ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("basis", BASES)
def test_ac_011_02_lag_0_settles_as_spec_009_at_every_step(w9: Window, basis: PriceBasis) -> None:
    model, opening = w9.model(basis), OPENING[basis]
    old = new = initial_state(model, w9.inputs[0].t, SOC0_KWH, opening)
    acc, kept = old.acc, ()
    recent: tuple[Flow, ...] = ()
    months = 0
    for q in w9.inputs:
        o_state, o_out, o_row, o_month = _step_spec009(model, SC, w9.proxies, old, q)
        expected = (o_state.acc, o_row, o_month)

        month = civil_month(q.t)
        fixed = fixed_charges(model.tariff, month, model.fixed_inputs)
        got = settle_quarter(
            old.acc, settle_in(o_out), fixed, basis_for(model.cohort, month), model.rules
        )
        assert bits(got) == bits(expected), q.t

        flow = _flow(q, o_out)
        acc, kept, row, settled = settle_step(model, acc, kept, flow)
        assert bits((acc, row, settled)) == bits(expected), q.t

        out = step(model, SC, w9.proxies, new, q, recent)
        assert bits((out.state, out.quarter, out.row, out.month)) == bits(
            (o_state, o_out, o_row, o_month)
        ), q.t
        assert out.recent == kept
        # Only the open hour's flows are kept: none on an hour border.
        assert tuple(f.settle for f in out.recent) == out.state.acc.hour
        if out.state.acc.t % QUARTERS_PER_HOUR == 0:
            assert out.recent == ()
        else:
            assert out.recent[-1] == flow

        old, new, recent = o_state, out.state, out.recent
        months += o_month is not None
    assert months == 1


# ── AC-011-03: the three cases of ``correct`` ───────────────────────────────────────────────────


def test_ac_011_03_a_correction_in_the_open_hour_replaces_its_entry(w9: Window) -> None:
    model, inputs = w9.rce, w9.inputs
    t0 = inputs[0].t
    assert t0 % QUARTERS_PER_HOUR == 0
    outs = _run(model, w9.proxies, OPENING[PriceBasis.RCE], inputs[:2], lag=0)
    state, recent = outs[-1].state, outs[-1].recent
    acc = state.acc
    assert (acc.t, len(acc.hour), len(recent)) == (t0 + 2, 2, 2)

    c = Correction("consumption", t0 + 1, inputs[1].load_kwh + 0.5)
    acc2, recent2, row = correct(acc, recent, c)
    fixed = _corrected(recent[1], c)
    assert row is None
    assert recent2 == (recent[0], fixed)
    assert fixed.p_kw == outs[1].quarter.p_kw
    assert acc2 == SettleAcc(acc.t, (acc.hour[0], fixed.settle), acc.month_rows, acc.tranches)

    # Attached to the next step, the hour then closes over the corrected flow.
    a = step(model, SC, w9.proxies, state, inputs[2], recent, (c,))
    b = step(model, SC, w9.proxies, a.state, inputs[3], a.recent)
    assert a.row is None
    assert b.row == settle_hour(
        (recent[0].settle, fixed.settle, settle_in(a.quarter), settle_in(b.quarter))
    )
    plain = _runs(PriceBasis.RCE, 0)
    assert b.row != plain[3].row


def test_ac_011_03_a_correction_of_a_closed_hour_replaces_its_row(w9: Window) -> None:
    model, inputs = w9.rce, w9.inputs
    t0 = inputs[0].t
    outs = _run(model, w9.proxies, OPENING[PriceBasis.RCE], inputs[:10], lag=LAG)
    acc, recent = outs[-1].state.acc, outs[-1].recent
    assert [f.settle.t for f in recent] == list(range(t0, t0 + 10))
    assert len(acc.month_rows) == 2 and len(acc.hour) == 2

    c = Correction("production", t0 + 1, inputs[1].pv_kwh + 0.75)
    acc2, recent2, row = correct(acc, recent, c)
    fixed = _corrected(recent[1], c)
    assert recent2 == (recent[0], fixed, *recent[2:])
    assert row == settle_hour((recent[0].settle, fixed.settle, recent[2].settle, recent[3].settle))
    assert row != acc.month_rows[0]
    assert acc2 == SettleAcc(acc.t, acc.hour, (row, acc.month_rows[1]), acc.tranches)

    # Through a step: the row is replaced, the step's own row is not the recomputed one.
    out = step(model, SC, w9.proxies, outs[-1].state, inputs[10], recent, (c,), LAG)
    assert out.state.acc.month_rows == (row, acc.month_rows[1])
    assert out.row is None
    assert out.recent[1] == fixed


def test_ac_011_03_a_correction_for_a_flow_not_kept_is_refused(w9: Window) -> None:
    model, inputs = w9.rce, w9.inputs
    t0 = inputs[0].t
    outs = _run(model, w9.proxies, OPENING[PriceBasis.RCE], inputs[:6], lag=0)
    state, recent = outs[-1].state, outs[-1].recent
    assert [f.settle.t for f in recent] == [t0 + 4, t0 + 5]  # hour 0 closed, its flows dropped

    late = Correction("consumption", t0 + 1, 0.3)
    with pytest.raises(ValueError):
        correct(state.acc, recent, late)
    with pytest.raises(ValueError):
        step(model, SC, w9.proxies, state, inputs[6], recent, (late,))
    with pytest.raises(ValueError):  # an open-hour slot whose flow is missing
        correct(state.acc, recent[1:], Correction("production", t0 + 4, 0.3))


@pytest.mark.parametrize("stream", ["grid", "battery", "Production", "pv", ""])
def test_ac_011_03_an_unknown_stream_is_refused(w9: Window, stream: str) -> None:
    outs = _run(w9.rce, w9.proxies, OPENING[PriceBasis.RCE], w9.inputs[:6], lag=LAG)
    acc, recent = outs[-1].state.acc, outs[-1].recent
    for t in (w9.inputs[1].t, w9.inputs[5].t):  # a closed hour and the open hour
        with pytest.raises(ValueError):
            correct(acc, recent, Correction(stream, t, 0.3))


def test_ac_011_03_a_correction_for_a_slot_not_yet_stepped_is_refused(w9: Window) -> None:
    outs = _run(w9.rce, w9.proxies, OPENING[PriceBasis.RCE], w9.inputs[:6], lag=LAG)
    acc, recent = outs[-1].state.acc, outs[-1].recent
    for t in (acc.t, acc.t + 1, acc.t + 40):
        with pytest.raises(ValueError):
            correct(acc, recent, Correction("consumption", t, 0.3))


def test_ac_011_03_a_correction_after_its_month_closed_is_refused(w9: Window) -> None:
    """At lag 0 October closes on its last quarter-hour: its last hour's flows, even if kept, can no
    longer reach a row."""
    i = OCT_LAST - w9.inputs[0].t
    outs = _runs(PriceBasis.RCE, 0)
    closed = outs[i]
    assert closed.month is not None and closed.state.acc.month_rows == ()
    last_hour = tuple(
        _flow(q, o.quarter)
        for q, o in zip(w9.inputs[i - 3 : i + 1], outs[i - 3 : i + 1], strict=True)
    )
    with pytest.raises(ValueError):
        correct(closed.state.acc, last_hour, Correction("consumption", OCT_LAST - 1, 0.3))


@dataclass(frozen=True)
class FollowLast:
    """Charges from PV after a quarter with surplus, else discharges to the load: the decision reads
    the last PV and load, so a correction leaking into them would change it."""

    def decide(self, state: BatteryState, obs: Observation) -> Action:
        if obs.last_pv_kwh > obs.last_load_kwh:
            return Action(ActionKind.CHARGE_FROM_PV)
        return Action(ActionKind.DISCHARGE_TO_LOAD)


def test_ac_011_03_a_correction_changes_no_decision_power_or_soc(w9: Window) -> None:
    inputs = w9.inputs[40:88]  # 2026-10-24 10:00 … 22:00 local: PV and load both move
    attached: dict[int, list[Correction]] = {}
    for k, q in enumerate(inputs[:24]):
        delay = L_SLOTS + 1 + k % L_SLOTS  # within [t + L + 1, t + 2L]
        stream = "production" if k % 2 else "consumption"
        kwh = q.load_kwh if stream == "production" else 2.0 * q.pv_kwh + 0.1
        attached.setdefault(q.t + delay, []).append(Correction(stream, q.t, kwh))
    opening = OPENING[PriceBasis.RCE]
    plain = _run(w9.rce, w9.proxies, opening, inputs, LAG, policy=FollowLast())
    fixed = _run(w9.rce, w9.proxies, opening, inputs, LAG, attached, policy=FollowLast())
    assert {o.quarter.action.kind for o in plain} == {
        ActionKind.CHARGE_FROM_PV,
        ActionKind.DISCHARGE_TO_LOAD,
    }
    for a, b in zip(plain, fixed, strict=True):
        assert bits(b.quarter) == bits(a.quarter)
        assert bits((b.state.soc_kwh, b.state.last_pv_kwh, b.state.last_load_kwh)) == bits(
            (a.state.soc_kwh, a.state.last_pv_kwh, a.state.last_load_kwh)
        )
        assert [f.p_kw for f in b.recent] == [f.p_kw for f in a.recent]
    assert fixed[-1].state.acc != plain[-1].state.acc  # the ledger did change


# ── AC-011-04, -05: the month closes a lag after it ends ────────────────────────────────────────


@pytest.mark.parametrize("basis", BASES)
def test_ac_011_04_a_lag_only_delays_the_october_close(w9: Window, basis: PriceBasis) -> None:
    lag0, lagged = _runs(basis, 0), _runs(basis, LAG)
    october_rows = tuple(r for o in lag0 if (r := o.row) is not None and r.month == "2026-10")
    for a, b in zip(lag0, lagged, strict=True):
        t = a.quarter.t
        assert b.quarter == a.quarter and b.row == a.row
        in_lag = OCT_LAST <= t < OCT_LAST + LAG
        assert (b.state == a.state) is (not in_lag), t
        if in_lag:  # October's rows and tranches wait for the close; nothing else differs
            assert b.state.acc == SettleAcc(
                a.state.acc.t,
                a.state.acc.hour,
                october_rows + a.state.acc.month_rows,
                OPENING[basis],
            )
        assert len(b.recent) <= LAG + 3  # AC-011-05
        assert len({r.month for r in b.state.acc.month_rows}) <= 2
    ((t_a, oct_a),) = _months(lag0)
    ((t_b, oct_b),) = _months(lagged)
    assert (t_a, t_b) == (OCT_LAST, OCT_LAST + LAG)
    assert bits(oct_b) == bits(oct_a)
    assert oct_b.month == "2026-10" and oct_b.closing != OPENING[basis]


def test_ac_011_04_add_quarter_due_month_and_close_month(w9: Window) -> None:
    model = w9.rce
    i = OCT_LAST - w9.inputs[0].t
    lag0, lagged = _runs(PriceBasis.RCE, 0), _runs(PriceBasis.RCE, LAG)

    before = lagged[i - 1].state.acc  # expects October's last quarter-hour
    assert before.t == OCT_LAST and due_month(before, 0) is None
    acc, row = add_quarter(before, settle_in(lagged[i].quarter))
    assert row is not None and row.month == "2026-10" and hour_month(row.h + 1) == "2026-11"
    # add_quarter writes the row and never closes the month.
    assert acc == SettleAcc(OCT_LAST + 1, (), (*before.month_rows, row), before.tranches)
    assert due_month(acc, 0) == "2026-10"
    assert due_month(acc, 1) is None
    with pytest.raises(ValueError, match="expected"):
        add_quarter(before, settle_in(lagged[i + 1].quarter))

    waiting = lagged[i + LAG - 1].state.acc  # acc.t == OCT_LAST + LAG
    assert due_month(waiting, LAG) is None
    assert due_month(waiting, LAG - 1) == "2026-10"
    fixed = fixed_charges(model.tariff, "2026-10", model.fixed_inputs)
    closed, october = close_month(waiting, fixed, basis_for(model.cohort, "2026-10"), model.rules)
    assert october == lag0[i].month
    assert closed == SettleAcc(
        waiting.t,
        waiting.hour,
        tuple(r for r in waiting.month_rows if r.month == "2026-11"),
        october.closing,
    )
    assert closed == lag0[i + LAG - 1].state.acc
    assert due_month(closed, 0) is None


def test_ac_011_04_corrections_up_to_the_lag_reach_the_october_close(w9: Window) -> None:
    model, inputs = w9.rce, w9.inputs
    q = {x.t: x for x in inputs}
    attached = {
        OCT_LAST - 20 + L_SLOTS + 1: [
            Correction("consumption", OCT_LAST - 20, q[OCT_LAST - 20].load_kwh + 0.25)
        ],
        OCT_LAST - 3 + LAG: [Correction("production", OCT_LAST - 3, q[OCT_LAST - 3].pv_kwh + 0.5)],
        # The month's last quarter-hour, at the last step its lateness allows: the closing step.
        OCT_LAST + LAG: [Correction("consumption", OCT_LAST, q[OCT_LAST].load_kwh + 1.0)],
        # November's first quarter-hour, after October closed.
        OCT_LAST + 1 + LAG: [Correction("production", OCT_LAST + 1, q[OCT_LAST + 1].pv_kwh + 2.0)],
    }
    plain = _runs(PriceBasis.RCE, 0)
    outs = _run(model, w9.proxies, OPENING[PriceBasis.RCE], inputs, LAG, attached)
    for a, b in zip(plain, outs, strict=True):
        assert bits(b.quarter) == bits(a.quarter)

    rows = hour_rows(_final_settle_ins(plain, inputs, attached))
    ((t_close, october),) = _months(outs)
    assert t_close == OCT_LAST + LAG
    expected = settle_month(
        "2026-10",
        [r for r in rows if r.month == "2026-10"],
        fixed_charges(model.tariff, "2026-10", model.fixed_inputs),
        basis_for(model.cohort, "2026-10"),
        model.rules,
        OPENING[PriceBasis.RCE],
    )
    assert bits(october) == bits(expected)
    assert october != _months(plain)[0][1]
    assert outs[-1].state.acc.month_rows == tuple(r for r in rows if r.month == "2026-11")
    assert outs[-1].state.acc.hour == ()


def test_ac_011_04_a_lagged_close_uses_the_closed_months_charges_and_basis() -> None:
    w = september_window()
    model = w.rcem
    september = fixed_charges(model.tariff, "2026-09", model.fixed_inputs)
    # Preconditions: the closing step lies in October, where the DSO version and the basis differ.
    assert civil_month(SEP_LAST) == "2026-09" and civil_month(SEP_LAST + LAG) == "2026-10"
    assert september != fixed_charges(model.tariff, "2026-10", model.fixed_inputs)
    assert basis_for(model.cohort, "2026-09") is PriceBasis.RCEM
    assert basis_for(model.cohort, "2026-10") is PriceBasis.RCE

    lag0 = _run(model, w.proxies, SEPTEMBER_OPENING, w.inputs, 0)
    lagged = _run(model, w.proxies, SEPTEMBER_OPENING, w.inputs, LAG)
    ((t_a, sep_a),) = _months(lag0)
    ((t_b, sep_b),) = _months(lagged)
    assert (t_a, t_b) == (SEP_LAST, SEP_LAST + LAG)
    assert sep_b.month == "2026-09"
    assert sep_b.fixed == september and sep_b.fixed.operator_version == "enea-op-2026-01"
    assert sep_b.basis is PriceBasis.RCEM
    assert sep_b.expired_pln > 0.0  # the 2025-09 tranche expires at September's close
    assert bits(sep_b) == bits(sep_a)
    for a, b in zip(lag0, lagged, strict=True):
        in_lag = SEP_LAST <= a.quarter.t < SEP_LAST + LAG
        assert (b.state == a.state) is (not in_lag), a.quarter.t
