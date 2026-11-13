"""AC-009-17, -20: the offline reference and the incremental stepper agree bit for bit.

The test site (Enea G12) runs over local days 2026-10-24 … 2026-11-02: the 100-slot civil day, the
October close with an expiring opening tranche, and an RCEm variant whose signal price (lagged,
published) differs from its ledger price (the month's own proxy). RCE and the proxy weights are
built per local day, as the generator emits prices (``day_prices``).
"""

from __future__ import annotations

import dataclasses
import math
from datetime import date, timedelta
from pathlib import Path

import pytest

from ems.contracts.time import local_day_bounds, quarter_index
from ems.domain.actions import NoBattery, Policy, SelfConsumption
from ems.domain.calendar import civil_date, civil_month
from ems.domain.netbilling import Cohort, PriceBasis, RcemTable, floored
from ems.domain.reference import QuarterIn, RefResult, SiteModel, initial_state, run
from ems.domain.settlement import SettleAcc, SettleIn, Tranche, settle_quarter
from ems.domain.stepper import step
from ems.domain.tariff import FixedCharges
from ems.domain.units import pln_per_mwh_to_pln_per_kwh
from ems.ingest.catalog.build import load_overrides
from ems.ingest.generator import SyntheticModel
from ems.ingest.synthetic.prices import day_prices, national_pv_index
from ems.tariffs import load_book, proxy_rcem, site_model

CONFIG = Path(__file__).resolve().parents[3] / "config"
SEED = 4242
FIRST, LAST = date(2026, 10, 24), date(2026, 11, 2)


def _t(day: date, end: bool = False) -> int:
    start, stop = local_day_bounds(day)
    return quarter_index(stop if end else start)


@dataclasses.dataclass(frozen=True)
class Window:
    rce: SiteModel
    rcem: SiteModel
    proxies: RcemTable
    inputs: tuple[QuarterIn, ...]


def _daily(synth: SyntheticModel, first: date, last: date) -> tuple[list[float], list[float]]:
    """RCE (PLN/kWh) and PV-index weights per local day, as the generator emits them."""
    rce: list[float] = []
    weights: list[float] = []
    day = first
    while day <= last:
        rce += [
            pln_per_mwh_to_pln_per_kwh(float(p))
            for p in day_prices(day, SEED, synth.price_params, synth.weather_params)
        ]
        t0, t1 = _t(day), _t(day, end=True)
        weights += [
            float(w)
            for w in national_pv_index(t0, t1 - t0, SEED, synth.price_params, synth.weather_params)
        ]
        day += timedelta(days=1)
    return rce, weights


@pytest.fixture(scope="module")
def window() -> Window:
    site = load_overrides(CONFIG / "sites.yaml", SEED)["PL-LOC-000001"]
    t0, t1 = _t(FIRST), _t(LAST, end=True)
    model = site_model(site, load_book(CONFIG / "tariffs"), (t0, t1))
    synth = SyntheticModel([site], SEED, CONFIG)
    chunk = synth.chunk(t0, t1 - t0, [])
    # Whole months for the proxies: September (signal), October and November (ledger).
    m0 = _t(date(2026, 9, 1))
    rce, weights = _daily(synth, date(2026, 9, 1), date(2026, 11, 30))
    inputs = tuple(
        QuarterIn(t0 + i, chunk.generation(0, i), chunk.consumption(0, i), rce[t0 + i - m0])
        for i in range(t1 - t0)
    )
    rcem_model = dataclasses.replace(model, cohort=Cohort(date(2023, 5, 1)))
    return Window(model, rcem_model, proxy_rcem(m0, rce, weights), inputs)


def _stepped(
    model: SiteModel,
    policy: Policy,
    rcem: RcemTable,
    opening: tuple[Tranche, ...],
    inputs: tuple[QuarterIn, ...],
) -> RefResult:
    state = initial_state(model, inputs[0].t, 5.0, opening)
    quarters, rows, months = [], [], []
    for q in inputs:
        out = step(model, policy, rcem, state, q)
        state = out.state
        quarters.append(out.quarter)
        if out.row is not None:
            rows.append(out.row)
        if out.month is not None:
            months.append(out.month)
    return RefResult(tuple(quarters), tuple(rows), tuple(months), state)


# RCE: the old tranche covers October and expires with a capped refund. RCEm: it is used up and the
# newer one follows it.
OPENING = {
    PriceBasis.RCE: (
        Tranche("2025-10", "2025-11", 615.0, 500.0, 20.0, PriceBasis.RCE),
        Tranche("2026-09", "2026-10", 49.2, 40.0, 12.0, PriceBasis.RCE),
    ),
    PriceBasis.RCEM: (
        Tranche("2025-10", "2025-11", 36.9, 10.0, 6.0, PriceBasis.RCEM),
        Tranche("2026-09", "2026-10", 492.0, 400.0, 80.0, PriceBasis.RCEM),
    ),
}


@pytest.mark.parametrize("basis", [PriceBasis.RCE, PriceBasis.RCEM])
@pytest.mark.parametrize(
    "policy", [NoBattery(), SelfConsumption()], ids=["no-battery", "self-consumption"]
)
def test_ac_009_20_reference_equals_stepping(
    window: Window, basis: PriceBasis, policy: Policy
) -> None:
    model = window.rce if basis is PriceBasis.RCE else window.rcem
    opening = OPENING[basis]
    start = initial_state(model, window.inputs[0].t, 5.0, opening)
    batch = run(model, policy, window.proxies, start, window.inputs)
    stepped = _stepped(model, policy, window.proxies, opening, window.inputs)
    assert stepped.quarters == batch.quarters
    assert stepped.rows == batch.rows
    assert stepped.months == batch.months
    assert stepped.state == batch.state

    assert len(batch.quarters) == 964 and len(batch.rows) == 241
    (october,) = batch.months
    assert october.month == "2026-10" and october.basis is basis
    if basis is PriceBasis.RCE:
        assert october.refund_pln == 20.0 and october.extinguished_pln > 0.0
        assert october.closing[0].assigned == "2026-10"
        for q_in, q in zip(window.inputs, batch.quarters, strict=True):
            assert q.export_price_pln_per_kwh == q_in.rce_pln_per_kwh
            assert q.sell_signal_pln_per_kwh == 1.23 * floored(q_in.rce_pln_per_kwh)
    else:
        assert [a for a, _ in october.offsets] == ["2025-11", "2026-10"]
        # The ledger uses the month's own RCEm; the signal the newest one known at q (September's).
        for q in batch.quarters:
            assert q.export_price_pln_per_kwh == window.proxies.billing(civil_month(q.t))
            known = window.proxies.signal(civil_date(q.t))[1].value_pln_per_kwh
            assert q.sell_signal_pln_per_kwh == 1.23 * floored(known)
        assert {window.proxies.signal(civil_date(q.t))[0].month for q in batch.quarters} == {
            "2026-09"
        }
        oct_price = window.proxies.billing("2026-10")
        assert october.export_value_pln == pytest.approx(october.export_kwh * oct_price, rel=1e-12)
        assert october.export_kwh > 0.0


def test_ac_009_17_outputs_and_p14_metadata(window: Window) -> None:
    start = initial_state(window.rcem, window.inputs[0].t, 5.0, ())
    res = run(window.rcem, SelfConsumption(), window.proxies, start, window.inputs)
    q = res.quarters[0]
    assert (q.t, q.basis) == (window.inputs[0].t, PriceBasis.RCEM)
    fields = {f.name for f in dataclasses.fields(q)}
    assert fields >= {"action", "p_cmd_kw", "p_kw", "soc_kwh", "grid_kwh"}
    assert res.state.acc.t == window.inputs[-1].t + 1
    assert len(res.state.acc.month_rows) == 48  # Nov 1-2 stay open
    meta = res.meta
    assert meta is not None
    assert meta.location_id == "PL-LOC-000001" and meta.cohort == Cohort(date(2023, 5, 1))
    assert meta.versions == ("enea-op-2026-10", "enea-sa-2026", "quality-2026-02")
    assert {(m, s) for m, s, _ in meta.rcem} == {
        ("2026-09", "proxy"),
        ("2026-10", "proxy"),
        ("2026-11", "proxy"),
    }
    assert all(not math.isnan(x.grid_kwh) for x in res.quarters)


def test_settle_quarter_rejects_an_out_of_order_quarter(window: Window) -> None:
    state = initial_state(window.rce, window.inputs[0].t, 5.0, ())
    with pytest.raises(ValueError, match="expected"):
        step(window.rce, NoBattery(), window.proxies, state, window.inputs[1])
    acc = SettleAcc(window.inputs[0].t, (), (), ())
    buy = run(window.rce, NoBattery(), window.proxies, state, window.inputs[:4]).quarters[0].buy
    fixed = FixedCharges(10.0, 0.0, 20.0, "op")
    with pytest.raises(ValueError, match="expected"):
        settle_quarter(
            acc, SettleIn(acc.t + 2, 0.0, buy, 0.0), fixed, PriceBasis.RCE, window.rce.rules
        )


def test_initial_state_checks_start_soc_and_opening(window: Window) -> None:
    t0 = _t(FIRST)
    with pytest.raises(ValueError, match="hour"):
        initial_state(window.rce, t0 + 1, 5.0, ())
    with pytest.raises(ValueError, match="SoC"):
        initial_state(window.rce, t0, 0.5, ())  # below soc_min 1.0
    expired = Tranche("2025-08", "2025-09", 10.0, 10.0, 3.0, PriceBasis.RCE)  # last usable 2026-08
    with pytest.raises(ValueError, match=r"PL-LOC-000001: opening .* expired"):
        initial_state(window.rce, t0, 5.0, (expired,))
    early = Tranche("2026-10", "2026-11", 10.0, 10.0, 3.0, PriceBasis.RCE)
    with pytest.raises(ValueError, match="not usable yet"):
        initial_state(window.rce, t0, 5.0, (early,))


@pytest.mark.parametrize(
    ("args", "match"),
    [
        (("2026-01", "2026-03", 10.0, 10.0, 3.0), "next month"),
        (("2026-01", "2026-02", 10.0, 11.0, 3.0), "remaining"),
        (("2026-01", "2026-02", 10.0, -1.0, 3.0), "remaining"),
        (("2026-01", "2026-02", 10.0, 5.0, -3.0), "cap"),
    ],
)
def test_tranche_invariants(args: tuple[str, str, float, float, float], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Tranche(*args, PriceBasis.RCE)
