"""Time base of the wire contract: UTC storage, quarter-hour index ``t``, Polish delivery days.

Storage is always UTC. Europe/Warsaw is used only for calendar logic (delivery days, tariffs,
holidays).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

WARSAW = ZoneInfo("Europe/Warsaw")
QUARTER = timedelta(minutes=15)
QUARTER_S = 900

RESOLUTIONS: dict[str, timedelta] = {
    "PT1M": timedelta(minutes=1),
    "PT15M": QUARTER,
    "PT1H": timedelta(hours=1),
    "P1D": timedelta(days=1),
}


def require_utc(dt: datetime) -> datetime:
    """Return ``dt`` as UTC; reject naive datetimes."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"naive datetime not allowed: {dt!r}")
    return dt.astimezone(UTC)


def fmt_utc(dt: datetime) -> str:
    """Canonical text form used for identity: RFC 3339 UTC, millisecond precision, ``Z``."""
    d = require_utc(dt)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{d.microsecond // 1000:03d}Z"


def quarter_index(dt: datetime) -> int:
    """UTC quarter-hour index ``t`` = floor(unix seconds / 900)."""
    return int(require_utc(dt).timestamp()) // QUARTER_S


def from_quarter_index(t: int) -> datetime:
    return datetime.fromtimestamp(t * QUARTER_S, tz=UTC)


def floor_quarter(dt: datetime) -> datetime:
    return from_quarter_index(quarter_index(dt))


def local_day_bounds(day: date) -> tuple[datetime, datetime]:
    """UTC start and end of a Europe/Warsaw calendar day (local midnight to local midnight)."""
    start = datetime(day.year, day.month, day.day, tzinfo=WARSAW)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=WARSAW)
    return start.astimezone(UTC), end.astimezone(UTC)


def local_day_slots(day: date, step: timedelta = QUARTER) -> list[tuple[datetime, datetime]]:
    """Half-open UTC slots covering a local day: 92/96/100 quarter-hours (23/24/25 hours)."""
    start, end = local_day_bounds(day)
    slots: list[tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        slots.append((cur, cur + step))
        cur += step
    return slots


def local_date(dt: datetime) -> date:
    return require_utc(dt).astimezone(WARSAW).date()


def iso_duration(td: timedelta) -> str:
    """ISO 8601 duration with hours and minutes only (``PT1H15M``, ``PT0S``, ``PT48H``)."""
    total = int(td.total_seconds())
    if total < 0:
        raise ValueError("negative duration")
    if total == 0:
        return "PT0S"
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    out = "PT"
    if hours:
        out += f"{hours}H"
    if minutes:
        out += f"{minutes}M"
    if seconds:
        out += f"{seconds}S"
    return out
