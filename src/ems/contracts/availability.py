"""When a consumer may use an event: the availability rule of ``08-contracts.md`` (spec 008).

No field carries it; it follows from the event:
- measurements: at the end of their interval (plus the producer's meter delay, if any);
- weather forecasts: ``issued_at`` plus the delay of their ``forecast_model``;
- prices: at their publication time, ``issued_at``.

A forecast delay is the largest measured Open-Meteo availability delay of that model
(``last_run_availability_time - last_run_initialisation_time`` in ``meta.json``) plus Open-Meteo's
recommended 10-minute wait, rounded up to a quarter-hour. It describes when a run was publicly
usable; a live poller can only be later. Synthetic forecasts use the ICON-D2 value. Producers use
the same table to order events, so a replay never makes a forecast usable earlier than it was.

Schedule constants for the watermarks of the sparse streams (ADR 0017, spec 011). They are code,
not wire contract, and the generator is tested against them:
- the run cadence per forecast model: the next run cannot be usable earlier than one cadence after
  the current one;
- the opening of the price publication window: the curve of delivery day D is published on D-1, not
  before 13:00 local (PSE RCE publications over 2025-10-01 … 2026-11-21 fell between 13:17 and
  16:13; the publication terms of IRiESP-OIRE are unread, risk A2).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

from ems.contracts.envelope import EnvelopeBase, MeasurementEvent, PriceEvent, WeatherForecastEvent
from ems.contracts.time import WARSAW

# meta.json samples 2026-10-20 ... 2026-11-13 (spec 008, F6 and 008/T07): 6 ICON-D2 runs
# (85.2-106.7 min; the 12 UTC run of 2026-11-13 took 106.7) and 3 DMI runs (168.9-170.4 min).
# More samples may raise them.
MEASURED_MAX_MIN: dict[str, float] = {"icon_d2": 106.7, "dmi_harmonie_arome_europe": 170.4}
GRACE_MIN = 10  # Open-Meteo: wait 10 minutes after last_run_availability_time
FORECAST_DELAY_MIN: dict[str, int] = {
    "synthetic": 120,
    "icon_d2": 120,
    "dmi_harmonie_arome_europe": 195,
}


def forecast_delay(model: str) -> timedelta:
    """Delay from ``issued_at`` until a run of ``model`` is usable; unknown models are refused."""
    try:
        return timedelta(minutes=FORECAST_DELAY_MIN[model])
    except KeyError:
        known = ", ".join(sorted(FORECAST_DELAY_MIN))
        raise ValueError(
            f"no availability delay for forecast model {model!r} (known: {known})"
        ) from None


# Runs every 3 h: DWD ICON-D2 at 00 … 21 UTC; DMI HARMONIE (DINI) keeps every third hour;
# Open-Meteo reports update_interval_seconds 10800 for both (read 2026-11-20). Synthetic runs follow
# the generator.
FORECAST_CADENCE_MIN: dict[str, int] = {
    "synthetic": 180,
    "icon_d2": 180,
    "dmi_harmonie_arome_europe": 180,
}
# Local Europe/Warsaw time, on D-1, from which the curve of delivery day D can be published.
PRICE_WINDOW_OPENS: dict[str, time] = {"PL:RCE": time(13, 0)}


def forecast_cadence(model: str) -> timedelta:
    """Time between two runs of ``model``; unknown models are refused."""
    try:
        return timedelta(minutes=FORECAST_CADENCE_MIN[model])
    except KeyError:
        raise ValueError(f"no run cadence for forecast model {model!r}") from None


def price_window_opens(market_key: str, day: date) -> datetime:
    """When the publication window on ``day`` opens (for the curve of the next day), in UTC.

    Once the curve of delivery day D has arrived, nothing more is due before this instant on D, so
    it is the price stream's watermark. Unknown markets are refused.
    """
    try:
        at = PRICE_WINDOW_OPENS[market_key]
    except KeyError:
        raise ValueError(f"no publication window for market {market_key!r}") from None
    return datetime.combine(day, at, tzinfo=WARSAW).astimezone(UTC)


def available_at(event: EnvelopeBase, meter_delay: timedelta = timedelta(0)) -> datetime:
    """The earliest time a consumer may use ``event``."""
    if isinstance(event, MeasurementEvent):
        return event.interval_end + meter_delay
    if isinstance(event, WeatherForecastEvent):
        assert event.issued_at is not None
        return event.issued_at + forecast_delay(event.payload.forecast_model)
    if isinstance(event, PriceEvent):
        assert event.issued_at is not None
        return event.issued_at
    raise TypeError(f"no availability rule for {type(event).__name__}")
