"""AC-009-04 … -06: purchase prices against a hand-computed golden table, fixed charges, VAT."""

from __future__ import annotations

import csv
import dataclasses
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ems.contracts.time import quarter_index
from ems.domain.actions import SelfConsumption
from ems.domain.calendar import civil_month, zone_at
from ems.domain.reference import QuarterIn, SiteModel, initial_state, run
from ems.domain.tariff import FixedInputs, SiteTariff, buy_price, capacity_band, fixed_charges
from ems.ingest.catalog.build import load_overrides
from ems.tariffs import load_book, load_rcem, real_rcem, site_model

ROOT = Path(__file__).resolve().parents[3]
CONFIG = ROOT / "config"
GOLDEN = ROOT / "tests" / "fixtures" / "tariffs" / "golden_2026.csv"
BOOK = load_book(CONFIG / "tariffs")


def _golden() -> list[dict[str, str]]:
    with GOLDEN.open() as f:
        return list(csv.DictReader(line for line in f if not line.startswith("#")))


def _first_t_in_zone(tariff: SiteTariff, month: str, zone: str) -> int:
    y, m = (int(x) for x in month.split("-"))
    t = quarter_index(datetime(y, m, 5, 0, tzinfo=UTC))  # a few days into the month, on an hour
    for i in range(0, 7 * 96, 4):
        if zone_at(t + i, tariff.scheme, tariff.national.holidays) == zone:
            assert civil_month(t + i) == month
            return t + i
    raise AssertionError(f"zone {zone} not found in {month}")


@pytest.mark.parametrize(
    "row", _golden(), ids=lambda r: f"{r['dso']}-{r['group']}-{r['zone']}-{r['month']}"
)
def test_ac_009_04_golden_prices(row: dict[str, str]) -> None:
    tariff = BOOK.site_tariff(row["dso"], row["group"])
    price = buy_price(tariff, _first_t_in_zone(tariff, row["month"], row["zone"]))
    assert price.zone == row["zone"]
    assert price.energy_pln_per_kwh == pytest.approx(float(row["energy_gross"]), abs=1e-9)
    assert price.dist_pln_per_kwh == pytest.approx(float(row["dist_gross"]), abs=1e-9)


def test_golden_table_covers_every_dso_group_zone_and_version() -> None:
    rows = _golden()
    expected = {
        (d.dso, g, z, m)
        for d in BOOK.dsos
        for g, scheme in d.schemes
        for z in scheme.zones
        for m in ("2026-01", "2026-02", "2026-10")
    }
    assert {(r["dso"], r["group"], r["zone"], r["month"]) for r in rows} == expected
    assert {d.dso for d in BOOK.dsos} == {"Enea", "Energa", "PGE", "Tauron"}


def test_version_ids_follow_the_civil_month() -> None:
    tariff = BOOK.site_tariff("Tauron", "G12")
    sep_end = quarter_index(datetime(2026, 9, 30, 21, 45, tzinfo=UTC))  # 23:45 CEST, September
    p_sep, p_oct = buy_price(tariff, sep_end), buy_price(tariff, sep_end + 1)
    assert (p_sep.operator_version, p_oct.operator_version) == (
        "tauron-dystr-2026-01",
        "tauron-dystr-2026-10",
    )
    jan = buy_price(tariff, quarter_index(datetime(2026, 1, 20, 12, tzinfo=UTC)))
    assert (jan.quality_period, p_oct.quality_period) == ("quality-2026-01", "quality-2026-02")
    with pytest.raises(ValueError, match="2027-01"):
        buy_price(tariff, quarter_index(datetime(2026, 12, 31, 23, tzinfo=UTC)))


@pytest.mark.parametrize(
    ("annual", "band", "net"),
    [
        (499.0, 0, 4.29),
        (500.0, 1, 10.31),
        (1200.0, 1, 10.31),
        (1200.5, 2, 17.18),
        (2800.0, 2, 17.18),
        (2801.0, 3, 24.05),
    ],
)
def test_fixed_charges_by_band(annual: float, band: int, net: float) -> None:
    tariff = BOOK.site_tariff("Enea", "G12")
    assert capacity_band(annual, tariff.national) == band
    jan = fixed_charges(tariff, "2026-01", FixedInputs(annual))
    assert jan.capacity_pln == pytest.approx(1.23 * net, abs=1e-12)
    assert jan.network_pln == pytest.approx(1.23 * 14.56, abs=1e-12)
    assert jan.subscription_pln == pytest.approx(1.23 * 3.84, abs=1e-12)
    assert fixed_charges(tariff, "2026-10", FixedInputs(annual)).subscription_pln == 0.0


# ── AC-009-05 and -06 on the reference, over the September/October edge ─────────────────────────

SITE = load_overrides(CONFIG / "sites.yaml", 4242)["PL-LOC-000001"]
RCEM = real_rcem(load_rcem(CONFIG / "tariffs" / "rcem.yaml"), ["2026-08", "2026-09", "2026-10"])
T0 = quarter_index(datetime(2026, 9, 30, 12, tzinfo=UTC))
INPUTS = tuple(
    QuarterIn(
        T0 + i,
        [0.0, 0.9, 1.6, 0.3][i % 4],
        [0.4, 0.5, 0.2, 0.7][i % 4],
        [0.31, -0.02, 0.45, 0.12][i % 4],
    )
    for i in range(48)
)


def _run(model: SiteModel):
    return run(model, SelfConsumption(), RCEM, initial_state(model, T0, 5.0, ()), INPUTS)


def test_ac_009_05_fixed_charges_only_reach_the_bill() -> None:
    base = site_model(SITE, BOOK, (T0, T0 + len(INPUTS)))
    ops = tuple(
        dataclasses.replace(v, fixed_fee_pln_per_month=v.fixed_fee_pln_per_month + 7.5)
        for v in base.tariff.operator_versions
    )
    other = dataclasses.replace(
        base, tariff=dataclasses.replace(base.tariff, operator_versions=ops)
    )
    a, b = _run(base), _run(other)
    assert a.quarters == b.quarters and a.rows == b.rows
    (ma,) = a.months
    (mb,) = b.months
    for f in ("energy_cost_pln", "dist_cost_pln", "export_value_pln", "offset_pln", "created"):
        assert getattr(ma, f) == getattr(mb, f)
    diff = mb.fixed.total_pln - ma.fixed.total_pln
    assert diff == pytest.approx(1.23 * 7.5, abs=1e-12)
    assert mb.payable_pln - ma.payable_pln == pytest.approx(diff, abs=1e-9)


def test_ac_009_06_vat_scales_purchases_only() -> None:
    base = site_model(SITE, BOOK, (T0, T0 + len(INPUTS)))
    low = dataclasses.replace(
        base.tariff, national=dataclasses.replace(base.tariff.national, vat=0.08)
    )
    other = dataclasses.replace(base, tariff=low)
    for t in (T0, T0 + 20, T0 + 44):
        p, q = buy_price(base.tariff, t), buy_price(low, t)
        assert q.energy_pln_per_kwh == pytest.approx(p.energy_pln_per_kwh * 1.08 / 1.23, rel=1e-12)
        assert q.dist_pln_per_kwh == pytest.approx(p.dist_pln_per_kwh * 1.08 / 1.23, rel=1e-12)
    a, b = _run(base), _run(other)
    assert [r.export_value_pln for r in a.rows] == [r.export_value_pln for r in b.rows]
    assert a.months[0].created is not None
    assert a.months[0].created == b.months[0].created
    assert a.months[0].export_value_pln == b.months[0].export_value_pln
