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
"""

from __future__ import annotations

from datetime import datetime, timedelta

from ems.contracts.envelope import EnvelopeBase, MeasurementEvent, PriceEvent, WeatherForecastEvent

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
