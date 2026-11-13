"""AC-009-14: the per-quarter price signal a policy sees."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ems.domain.netbilling import NetBillingRules, Publication, RcemEntry, RcemTable
from ems.domain.settlement import price_signal
from ems.domain.tariff import BuyPrice
from ems.tariffs import load_rcem, real_rcem

CONFIG = Path(__file__).resolve().parents[3] / "config"
RULES = NetBillingRules(1.23, 0.20, 0.30, 12)
BUY = BuyPrice(0.7, 0.35, "night", "op", "sel", "q")


def test_buy_is_energy_plus_distribution() -> None:
    assert price_signal(BUY, 0.3, RULES).buy_pln_per_kwh == 0.7 + 0.35


@pytest.mark.parametrize(("p", "sell"), [(0.4, 1.23 * 0.4), (0.0, 0.0), (-0.05, 0.0)])
def test_rce_sell_is_floored_and_scaled(p: float, sell: float) -> None:
    assert price_signal(BUY, p, RULES).sell_pln_per_kwh == sell


def test_rcem_signal_moves_the_day_after_publication() -> None:
    table = real_rcem(load_rcem(CONFIG / "tariffs" / "rcem.yaml"), ["2026-09", "2026-10"])
    entry, pub = table.signal(date(2026, 11, 11))  # October is published on the 11th
    assert (entry.month, pub.value_pln_per_kwh) == ("2026-09", 345.68 / 1000.0)
    entry, pub = table.signal(date(2026, 11, 12))
    assert (entry.month, pub.value_pln_per_kwh) == ("2026-10", 439.78 / 1000.0)


def test_february_correction_never_reaches_the_signal() -> None:
    entries = load_rcem(CONFIG / "tariffs" / "rcem.yaml")
    table = real_rcem(entries, ["2026-02", "2026-05"])
    entry, pub = table.signal(date(2026, 6, 12))  # Feb's correction and May's value, both 06-11
    assert (entry.month, pub.value_pln_per_kwh) == ("2026-05", 191.37 / 1000.0)
    assert table.billing("2026-02") == 331.39 / 1000.0


def test_a_correction_reaches_the_signal_only_once_known() -> None:
    m1 = RcemEntry(
        "2030-01",
        (Publication(0.30, date(2030, 2, 11)), Publication(0.25, date(2030, 2, 20))),
        "pse",
    )
    table = RcemTable((m1,))
    assert table.signal(date(2030, 2, 20))[1].value_pln_per_kwh == 0.30
    assert table.signal(date(2030, 2, 21))[1].value_pln_per_kwh == 0.25
    with pytest.raises(ValueError, match="published before"):
        table.signal(date(2030, 2, 11))
