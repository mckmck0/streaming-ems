"""AC-009-03: zones on tariff time (UTC+1 all year), seasons, day types and statutory holidays."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import holidays
import pytest

from ems.contracts.time import local_day_slots, quarter_index
from ems.domain.calendar import (
    DayType,
    ZoneRule,
    ZoneScheme,
    civil_date,
    civil_month,
    day_type,
    month_add,
    tariff_time,
    zone_at,
)
from ems.tariffs import TariffBook, load_book

CONFIG = Path(__file__).resolve().parents[3] / "config"
WAW = ZoneInfo("Europe/Warsaw")
DSOS = ("Enea", "Energa", "PGE", "Tauron")


@pytest.fixture(scope="module")
def book() -> TariffBook:
    return load_book(CONFIG / "tariffs")


def _t(y: int, mo: int, d: int, hh: int, mm: int = 30) -> int:
    """Quarter index of a Europe/Warsaw civil time."""
    return quarter_index(datetime(y, mo, d, hh, mm, tzinfo=WAW))


def _zone(book: TariffBook, dso: str, group: str, t: int) -> str:
    return zone_at(t, book.scheme(dso, group), book.holidays)


# Expected zones per DSO and group, for civil times. 2026-07-15 is a Wednesday (summer, CEST: tariff
# time is one hour behind local time); 2026-01-14 a Wednesday (winter, CET: tariff time = local).
SUMMER_WEDNESDAY = {
    # local time -> tariff hour: 06:30 → 05:30, 21:30 → 20:30, 22:30 → 21:30, 15:30 → 14:30
    ("Tauron", "G12"): {6: "night", 21: "day", 22: "day", 15: "night", 17: "day"},
    ("Energa", "G12"): {6: "night", 21: "day", 22: "day", 15: "night", 17: "day"},
    ("Enea", "G12"): {6: "night", 21: "day", 22: "day", 15: "night", 17: "day"},
    ("PGE", "G12"): {6: "night", 21: "day", 22: "day", 15: "day", 17: "night"},
    ("Tauron", "G12w"): {6: "offpeak", 21: "peak", 22: "peak", 15: "offpeak", 17: "peak"},
    ("Energa", "G12w"): {6: "offpeak", 21: "peak", 22: "peak", 15: "offpeak", 17: "peak"},
    ("Enea", "G12w"): {6: "offpeak", 21: "peak", 22: "offpeak", 15: "peak", 17: "peak"},
    ("PGE", "G12w"): {6: "offpeak", 21: "peak", 22: "peak", 15: "peak", 17: "offpeak"},
}


@pytest.mark.parametrize(("key", "expected"), sorted(SUMMER_WEDNESDAY.items()))
def test_summer_weekday_edges_on_tariff_time(
    book: TariffBook, key: tuple[str, str], expected: dict[int, str]
) -> None:
    dso, group = key
    for hour, zone in expected.items():
        assert _zone(book, dso, group, _t(2026, 7, 15, hour)) == zone, (dso, group, hour)


@pytest.mark.parametrize("dso", DSOS)
def test_g11_is_one_zone(book: TariffBook, dso: str) -> None:
    for day in (date(2026, 7, 15), date(2026, 7, 18), date(2026, 7, 19), date(2026, 12, 24)):
        quarters = [quarter_index(s) for s, _ in local_day_slots(day)]
        assert {_zone(book, dso, "G11", t) for t in quarters} == {"all"}


@pytest.mark.parametrize("dso", DSOS)
def test_winter_weekday(book: TariffBook, dso: str) -> None:
    # CET: tariff time equals local time.
    g12 = {hh: _zone(book, dso, "G12", _t(2026, 1, 14, hh)) for hh in range(24)}
    night = {hh for hh, z in g12.items() if z == "night"}
    assert night == {13, 14, 22, 23, 0, 1, 2, 3, 4, 5}
    g12w = {hh: _zone(book, dso, "G12w", _t(2026, 1, 14, hh)) for hh in range(24)}
    off = {hh for hh, z in g12w.items() if z == "offpeak"}
    if dso == "Enea":
        assert off == {21, 22, 23, 0, 1, 2, 3, 4, 5}
    else:
        assert off == {13, 14, 22, 23, 0, 1, 2, 3, 4, 5}


@pytest.mark.parametrize("dso", DSOS)
@pytest.mark.parametrize(
    "day",
    [date(2026, 7, 18), date(2026, 7, 19), date(2026, 11, 11), date(2026, 12, 24)],
    ids=["saturday", "sunday", "independence-day", "christmas-eve"],
)
def test_g12w_offpeak_all_day_on_weekends_and_holidays(
    book: TariffBook, dso: str, day: date
) -> None:
    for start, _end in local_day_slots(day):
        assert _zone(book, dso, "G12w", quarter_index(start)) == "offpeak"


@pytest.mark.parametrize("dso", DSOS)
def test_g12_ignores_day_types(book: TariffBook, dso: str) -> None:
    for day in (date(2026, 1, 17), date(2026, 1, 18), date(2026, 11, 11), date(2026, 12, 24)):
        zones = {
            hh: _zone(book, dso, "G12", _t(day.year, day.month, day.day, hh)) for hh in (10, 14)
        }
        assert zones == {10: "day", 14: "night"}, (dso, day)


def test_pge_seasons(book: TariffBook) -> None:
    # Summer night 15-17 tariff time = 16-18 CEST; winter night 13-15 = 13-15 CET.
    assert _zone(book, "PGE", "G12", _t(2026, 7, 15, 15)) == "day"
    assert _zone(book, "PGE", "G12", _t(2026, 7, 15, 16)) == "night"
    assert _zone(book, "PGE", "G12", _t(2026, 7, 15, 17)) == "night"
    assert _zone(book, "PGE", "G12", _t(2026, 7, 15, 18)) == "day"
    assert _zone(book, "PGE", "G12", _t(2026, 1, 14, 13)) == "night"
    assert _zone(book, "PGE", "G12", _t(2026, 1, 14, 15)) == "day"
    # Season edges on tariff time: 2026-04-01 00:00 UTC+1 is summer, 2026-03-31 23:00 is winter.
    assert _zone(book, "PGE", "G12", _t(2026, 4, 1, 14)) == "day"  # 13:30 tariff time, summer
    assert _zone(book, "PGE", "G12", _t(2026, 3, 31, 14)) == "night"  # winter
    assert _zone(book, "PGE", "G12w", _t(2026, 7, 15, 14)) == "peak"
    assert _zone(book, "PGE", "G12w", _t(2026, 1, 14, 14)) == "offpeak"


@pytest.mark.parametrize(("day", "slots"), [(date(2026, 3, 29), 92), (date(2026, 10, 25), 100)])
def test_dst_days_are_civil_billing_days(book: TariffBook, day: date, slots: int) -> None:
    quarters = [quarter_index(s) for s, _ in local_day_slots(day)]
    assert len(quarters) == slots
    assert {civil_date(t) for t in quarters} == {day}
    # Both are Sundays: every scheme gives exactly one zone per quarter, night or off-peak at 02:00.
    for dso in DSOS:
        for group in ("G11", "G12", "G12w"):
            zones = [_zone(book, dso, group, t) for t in quarters]
            assert len(zones) == slots
            assert zones[8] in {"all", "night", "offpeak"}


def test_civil_and_tariff_calendars_differ_at_the_month_edge() -> None:
    t = quarter_index(datetime(2026, 9, 30, 22, 0, tzinfo=UTC))
    assert civil_month(t) == "2026-10"
    assert civil_date(t) == date(2026, 10, 1)
    assert tariff_time(t) == datetime(2026, 9, 30, 23, 0)


def test_friday_to_saturday_edge_on_tariff_time() -> None:
    # A synthetic scheme without a window across midnight, so the day-type edge is visible.
    scheme = ZoneScheme(
        "peak", (ZoneRule("offpeak", (DayType.SATURDAY, DayType.SUNDAY), ((0, 24),)),)
    )
    hol: frozenset[date] = frozenset()
    friday_2330_cest = _t(2026, 7, 17, 23, 30)  # 22:30 Friday on tariff time
    saturday_0030_cest = _t(2026, 7, 18, 0, 30)  # 23:30 Friday on tariff time
    saturday_0130_cest = _t(2026, 7, 18, 1, 30)  # 00:30 Saturday on tariff time
    assert zone_at(friday_2330_cest, scheme, hol) == "peak"
    assert zone_at(saturday_0030_cest, scheme, hol) == "peak"
    assert zone_at(saturday_0130_cest, scheme, hol) == "offpeak"


def test_holidays_match_the_statutory_list(book: TariffBook) -> None:
    assert book.holidays == frozenset(holidays.Poland(years=2026))
    assert date(2026, 12, 24) in book.holidays


def test_day_type_precedence() -> None:
    hol = frozenset({date(2026, 11, 1)})  # a Sunday
    assert day_type(date(2026, 11, 1), hol) is DayType.HOLIDAY
    assert day_type(date(2026, 11, 8), hol) is DayType.SUNDAY
    assert day_type(date(2026, 11, 7), hol) is DayType.SATURDAY
    assert day_type(date(2026, 11, 6), hol) is DayType.WEEKDAY


def test_month_arithmetic() -> None:
    assert month_add("2025-11", 11) == "2026-10"
    assert month_add("2026-12", 1) == "2027-01"
    assert month_add("2026-01", -1) == "2025-12"
