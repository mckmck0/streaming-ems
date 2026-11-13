"""AC-009-07 … -09: hourly netting, export valuation, RCEm table and proxy, cohorts."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ems.contracts.time import quarter_index
from ems.domain.netbilling import (
    Cohort,
    PriceBasis,
    Publication,
    RcemEntry,
    RcemTable,
    basis_for,
    rcem_proxy,
)
from ems.domain.settlement import SettleIn, settle_hour
from ems.domain.tariff import BuyPrice
from ems.tariffs import load_rcem, proxy_rcem, real_rcem

CONFIG = Path(__file__).resolve().parents[3] / "config"
BUY = BuyPrice(1.0, 0.5, "all", "op", "sel", "q")
T0 = quarter_index(datetime(2026, 5, 3, 10, tzinfo=UTC))  # an hour start


def _hour(grid: list[float], prices: list[float]) -> list[SettleIn]:
    return [SettleIn(T0 + i, g, BUY, p) for i, (g, p) in enumerate(zip(grid, prices, strict=True))]


def test_ac_009_07_negative_slot_adds_exactly_zero() -> None:
    grid = [-1.0, -2.0, -0.5, -0.5]
    with_negative = settle_hour(_hour(grid, [0.40, -0.10, 0.30, 0.20]))
    with_zero = settle_hour(_hour(grid, [0.40, 0.0, 0.30, 0.20]))
    assert with_negative.export_value_pln == with_zero.export_value_pln
    assert with_negative.export_kwh == 4.0 and with_negative.import_kwh == 0.0


def test_ac_009_07_net_export_hour_earns_the_hours_mean_floored_price() -> None:
    prices = [0.40, -0.10, 0.30, -0.05]
    row = settle_hour(_hour([-3.0, 0.5, -1.0, -0.5], prices))
    assert row.export_kwh == 4.0
    mean = math.fsum(max(p, 0.0) for p in prices) / 4
    assert row.export_value_pln == pytest.approx(row.export_kwh * mean, rel=1e-15)


def test_ac_009_07_charge_and_export_in_one_hour_settle_the_net() -> None:
    row = settle_hour(_hour([1.0, -1.5, -0.5, 0.0], [0.3, 0.3, 0.3, 0.3]))
    assert (row.import_kwh, row.export_kwh) == (0.0, 1.0)
    assert row.energy_cost_pln == 0.0 and row.dist_cost_pln == 0.0
    assert row.export_value_pln == pytest.approx(0.3, rel=1e-15)


def test_ac_009_08_proxy_matches_its_definition() -> None:
    rce = [0.5, -0.2, 0.1, 0.3]
    w = [0.0, 2.0, 1.0, 3.0]
    expected = (0.0 * 0.5 + 2.0 * 0.0 + 1.0 * 0.1 + 3.0 * 0.3) / 6.0
    assert rcem_proxy(rce, w) == pytest.approx(expected, rel=1e-15)
    with pytest.raises(ValueError, match="sum to zero"):
        rcem_proxy(rce, [0.0, 0.0, 0.0, 0.0])


def test_ac_009_08_proxy_table_covers_whole_months_only() -> None:
    t0 = quarter_index(datetime(2026, 8, 31, 22, tzinfo=UTC))  # 2026-09-01 00:00 CEST
    t1 = quarter_index(datetime(2026, 10, 31, 23, tzinfo=UTC))  # 2026-11-01 00:00 CET
    n = t1 - t0
    rce = [0.1 + 0.001 * (i % 7) for i in range(n)]
    w = [float(i % 5) for i in range(n)]
    table = proxy_rcem(t0, rce, w)
    assert [e.month for e in table.entries] == ["2026-09", "2026-10"]
    sep = table.entry("2026-09")
    n_sep = quarter_index(datetime(2026, 9, 30, 22, tzinfo=UTC)) - t0
    assert sep.source == "proxy"
    assert sep.billing_value == rcem_proxy(rce[:n_sep], w[:n_sep])
    assert sep.publications[-1].published == date(2026, 10, 11)
    with pytest.raises(ValueError, match="whole month"):
        proxy_rcem(t0 + 4, rce[4:], w[4:])
    with pytest.raises(ValueError, match="whole month"):
        proxy_rcem(t0, rce[:-4], w[:-4])


def test_ac_009_08_real_mode_fails_on_a_missing_month() -> None:
    entries = load_rcem(CONFIG / "tariffs" / "rcem.yaml")
    table = real_rcem(entries, ["2026-09", "2026-10"])
    assert table.billing("2026-10") == 439.78 / 1000.0
    with pytest.raises(ValueError, match="2026-11"):
        real_rcem(entries, ["2026-10", "2026-11"])


def test_ac_009_08_corrections_billing_uses_the_latest() -> None:
    entries = {e.month: e for e in load_rcem(CONFIG / "tariffs" / "rcem.yaml")}
    october = entries["2025-10"]
    assert [p.published for p in october.publications] == [
        date(2025, 11, 11),
        date(2026, 2, 11),
        date(2026, 10, 11),
    ]
    assert october.billing_value == 330.27 / 1000.0
    # A value counts as known from the day after its publication date.
    before, after = october.known_on(date(2026, 2, 11)), october.known_on(date(2026, 2, 12))
    assert before is not None and before.value_pln_per_kwh == 334.02 / 1000.0
    assert after is not None and after.value_pln_per_kwh == 340.84 / 1000.0
    assert october.known_on(date(2025, 11, 11)) is None


def test_rcem_entry_rejects_bad_publications() -> None:
    p = Publication(0.3, date(2026, 4, 11))
    with pytest.raises(ValueError, match="increasing"):
        RcemEntry("2026-02", (p, Publication(0.31, date(2026, 4, 11))), "pse")
    with pytest.raises(ValueError, match="source"):
        RcemEntry("2026-02", (p,), "guess")
    with pytest.raises(ValueError, match="before the month ended"):
        RcemEntry("2026-04", (p,), "pse")
    with pytest.raises(ValueError, match="sorted"):
        RcemTable((RcemEntry("2026-03", (p,), "pse"), RcemEntry("2026-02", (p,), "pse")))
    with pytest.raises(ValueError, match="contiguous"):
        RcemTable((RcemEntry("2026-01", (p,), "pse"), RcemEntry("2026-03", (p,), "pse")))


@pytest.mark.parametrize(
    ("cohort", "month", "basis"),
    [
        (Cohort(date(2024, 7, 1)), "2026-01", PriceBasis.RCE),
        (Cohort(date(2025, 3, 15)), "2026-06", PriceBasis.RCE),
        (Cohort(date(2024, 6, 30)), "2026-06", PriceBasis.RCEM),
        (Cohort(date(2022, 4, 1)), "2026-06", PriceBasis.RCEM),
        (Cohort(date(2023, 5, 1), "2026-03"), "2026-02", PriceBasis.RCEM),
        (Cohort(date(2023, 5, 1), "2026-03"), "2026-03", PriceBasis.RCE),
        (Cohort(date(2023, 5, 1), "2026-03"), "2026-12", PriceBasis.RCE),
    ],
)
def test_ac_009_09_cohort_table(cohort: Cohort, month: str, basis: PriceBasis) -> None:
    assert basis_for(cohort, month) is basis


@pytest.mark.parametrize(
    ("first", "rce_from", "match"),
    [
        (date(2022, 3, 31), None, "net-metering"),
        (date(2024, 7, 1), "2026-01", "this cohort is RCE"),
        (date(2023, 5, 10), "2023-05", "after the month"),
        (date(2023, 5, 10), "2023-04", "after the month"),
        (date(2023, 5, 10), "2026-13", "YYYY-MM"),
        (date(2023, 5, 10), "202603", "YYYY-MM"),
    ],
)
def test_ac_009_09_cohort_rejections(first: date, rce_from: str | None, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Cohort(first, rce_from)
