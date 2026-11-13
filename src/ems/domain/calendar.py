"""Two calendars for settlement: tariff time for zones, the civil calendar for legal dates.

- Tariff time is UTC+1 all year. Every DSO's 2026 tariff sets meter clocks to winter time and
  keeps them there during summer time, so zone date, hour, day type and season are read on it.
- The civil calendar (Europe/Warsaw) decides version validity, billing months and RCEm
  publication dates.

``t`` is the UTC quarter-hour index; an hour is ``h = t // 4`` (Warsaw offsets are whole hours, so
UTC hours are local hours). Months are ``"YYYY-MM"`` strings.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from functools import lru_cache

from ems.contracts.time import QUARTER_S, WARSAW
from ems.domain.units import QUARTERS_PER_HOUR

TARIFF_UTC_OFFSET = timedelta(hours=1)
_EPOCH = datetime(1970, 1, 1)  # naive: tariff time carries no zone, only the fixed offset


class DayType(StrEnum):
    WEEKDAY = "weekday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"
    HOLIDAY = "holiday"


def tariff_time(t: int) -> datetime:
    """Naive wall time of the start of ``t`` on UTC+1 (meter clocks on winter time)."""
    return _EPOCH + timedelta(seconds=t * QUARTER_S) + TARIFF_UTC_OFFSET


@lru_cache(maxsize=65536)
def _civil_hour(h: int) -> datetime:
    return datetime.fromtimestamp(h * QUARTERS_PER_HOUR * QUARTER_S, tz=UTC).astimezone(WARSAW)


def civil_date(t: int) -> date:
    return _civil_hour(t // QUARTERS_PER_HOUR).date()


def hour_month(h: int) -> str:
    """Civil (Europe/Warsaw) billing month of the hour starting at ``h``."""
    d = _civil_hour(h)
    return f"{d.year:04d}-{d.month:02d}"


def civil_month(t: int) -> str:
    return hour_month(t // QUARTERS_PER_HOUR)


def month_index(month: str) -> int:
    year, mon = month.split("-")
    return int(year) * 12 + int(mon) - 1


def month_add(month: str, k: int) -> str:
    i = month_index(month) + k
    return f"{i // 12:04d}-{i % 12 + 1:02d}"


def month_of(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def day_type(day: date, holidays: frozenset[date]) -> DayType:
    """Precedence: statutory holiday > Sunday > Saturday > weekday."""
    if day in holidays:
        return DayType.HOLIDAY
    wd = day.weekday()
    if wd == 6:
        return DayType.SUNDAY
    if wd == 5:
        return DayType.SATURDAY
    return DayType.WEEKDAY


def _in_window(hour: int, window: tuple[int, int]) -> bool:
    """``[start, end)``; ``end <= start`` wraps midnight within the same date."""
    start, end = window
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _in_season(day: date, season: tuple[str, str]) -> bool:
    """Half-open month-day range ``("04-01", "10-01")``; wraps the year end when end <= start."""
    md = f"{day.month:02d}-{day.day:02d}"
    start, end = season
    if start < end:
        return start <= md < end
    return md >= start or md < end


@dataclass(frozen=True)
class ZoneRule:
    zone: str
    days: tuple[DayType, ...]
    hours: tuple[tuple[int, int], ...]  # whole hours, [start, end)
    season: tuple[str, str] | None = None

    def __post_init__(self) -> None:
        if not self.days or not self.hours:
            raise ValueError(f"zone rule {self.zone!r} needs days and hours")
        for start, end in self.hours:
            if not (0 <= start <= 23 and 0 <= end <= 24 and start != end):
                raise ValueError(f"zone rule {self.zone!r}: bad hour window {(start, end)}")

    def matches(self, day: date, hour: int, dtype: DayType) -> bool:
        if dtype not in self.days:
            return False
        if self.season is not None and not _in_season(day, self.season):
            return False
        return any(_in_window(hour, w) for w in self.hours)


@dataclass(frozen=True)
class ZoneScheme:
    default: str
    rules: tuple[ZoneRule, ...] = ()

    @property
    def zones(self) -> tuple[str, ...]:
        out = [self.default]
        for r in self.rules:
            if r.zone not in out:
                out.append(r.zone)
        return tuple(out)


def zone_at(t: int, scheme: ZoneScheme, holidays: frozenset[date]) -> str:
    """The zone of ``t``: the first matching rule on tariff time, else the default zone."""
    if not scheme.rules:
        return scheme.default
    wall = tariff_time(t)
    day = wall.date()
    dtype = day_type(day, holidays)
    for rule in scheme.rules:
        if rule.matches(day, wall.hour, dtype):
            return rule.zone
    return scheme.default
