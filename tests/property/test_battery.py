"""AC-003-04 / principle 8: the domain battery keeps SoC and power within limits; scalar
= vector.
"""

from __future__ import annotations

import numpy as np
from hypothesis import given
from hypothesis import strategies as st

from ems.domain.battery import BatteryParams, step, step_params

B = BatteryParams(capacity_kwh=10.0, p_max_kw=5.0, eta=0.95, soc_min_kwh=1.0, soc_max_kwh=9.5)
soc_st = st.floats(1.0, 9.5, allow_nan=False)
cmd_st = st.floats(-50, 50, allow_nan=False)


@given(soc=soc_st, cmd=cmd_st)
def test_soc_and_power_stay_within_limits(soc: float, cmd: float) -> None:
    new, p = step_params(soc, cmd, 0.25, B)
    assert B.soc_min_kwh <= new <= B.soc_max_kwh
    assert -B.p_max_kw <= p <= B.p_max_kw
    assert (p >= 0) == (cmd >= 0) or p == 0
    assert abs(p) <= abs(cmd) + 1e-12


@given(soc=soc_st, cmd=cmd_st)
def test_efficiency_on_charge_and_discharge(soc: float, cmd: float) -> None:
    new, p = step_params(soc, cmd, 0.25, B)
    expected = soc + (p * B.eta if p >= 0 else p / B.eta) * 0.25
    assert abs(new - min(max(expected, B.soc_min_kwh), B.soc_max_kwh)) < 1e-9


def test_matches_the_spike_reference() -> None:
    # spike: soc 5.0, charge command 10 kW for 15 min -> p = 5 kW, soc = 5 + 5*0.95*0.25
    assert step_params(5.0, 10.0, 0.25, B) == (5.0 + 5.0 * 0.95 * 0.25, 5.0)
    soc, p = step_params(1.2, -5.0, 0.25, B)  # limited by soc_min
    assert abs(soc - 1.0) < 1e-12 and abs(p - (-(0.2 * 0.95 / 0.25))) < 1e-12


@given(st.lists(st.tuples(soc_st, cmd_st), min_size=1, max_size=30))
def test_vectorised_equals_scalar(pairs: list[tuple[float, float]]) -> None:
    socs = np.array([s for s, _ in pairs])
    cmds = np.array([c for _, c in pairs])
    vs, vp = step(socs, cmds, 0.25, B.p_max_kw, B.eta, B.soc_min_kwh, B.soc_max_kwh)
    for i, (s, c) in enumerate(pairs):
        ss, sp = step_params(s, c, 0.25, B)
        assert vs[i] == ss and vp[i] == sp
