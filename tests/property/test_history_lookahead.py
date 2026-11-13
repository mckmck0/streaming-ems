"""AC-008-07: a built history recording never makes an event available before reality did.

Hypothesis (the fixed ``ci`` profile, few examples: each one fetches through respx and builds with
the real models) draws whole-UTC-day windows in the Single Runs archive, satellite gaps and the
D-1 publication times of the PSE days. For both real-data sites (ICON-D2 and DMI) it checks:
- availability - ``issued_at`` >= the stream's delay, and the position in ``events.jsonl`` is no
  earlier than the source's own time read from the raw files (the run, the latest publication);
- ``events.jsonl`` is sorted by ``emission_key``;
- per stream and subject (topic and key), ``interval_start`` never decreases;
- the sink's record timestamp follows 06 (meters ``interval_end``; forecasts, prices ``issued_at``).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ems.contracts.envelope import MeasurementEvent, Stream
from ems.contracts.time import WARSAW
from ems.ingest.generator import load_generator_config
from ems.ingest.kafka.sink import record_timestamp
from tests.unit.ingest.history_recording import (
    POZNAN,
    RADOM,
    Gap,
    build,
    event_lines,
    make_recording,
    manifest,
    raw_pse_days,
    raw_publications,
    raw_runs,
    read_built,
    window,
)

ARCHIVE_START = date(2026, 4, 2)
LAST_START = date(2026, 10, 20)  # whole days before the 2026-10-25 change of clocks
SLOTS_10MIN_PER_DAY = 144


@st.composite
def histories(draw: st.DrawFn) -> dict[str, Any]:
    start = draw(st.dates(min_value=ARCHIVE_START, max_value=LAST_START))
    window_days = draw(st.integers(min_value=1, max_value=3))
    t0 = datetime.combine(start, time(), UTC)
    gaps = [
        Gap(
            site,
            t0 + timedelta(minutes=10 * first_slot_10min),
            t0 + timedelta(minutes=10 * (first_slot_10min + length_slots_10min)),
        )
        for site, first_slot_10min, length_slots_10min in draw(
            st.lists(
                st.tuples(
                    st.sampled_from([POZNAN, RADOM]),
                    st.integers(0, window_days * SLOTS_10MIN_PER_DAY - 1),
                    st.integers(1, 18),  # up to 3 h
                ),
                max_size=3,
            )
        )
    ]
    # One D-1 publication per business day (local_date(start) … local_date(end - 1 s)).
    publish_local = draw(
        st.lists(
            st.times(min_value=time(13, 0), max_value=time(15, 59, 59, 999_000)),
            min_size=window_days + 1,
            max_size=window_days + 1,
        )
    )
    return {
        "start": start,
        "end": start + timedelta(days=window_days),
        "gaps": gaps,
        "publish_local": publish_local,
    }


def _publication(start: date, publish_local: list[time]) -> Callable[[date], datetime]:
    def published(day: date) -> datetime:
        local = publish_local[(day - start).days]
        when = datetime.combine(day - timedelta(days=1), local.replace(microsecond=0), WARSAW)
        return when.replace(microsecond=(local.microsecond // 1000) * 1000)

    return published


@settings(max_examples=6)
@given(history=histories())
def test_ac_008_07_no_event_is_available_before_its_source(
    tmp_path_factory: pytest.TempPathFactory, history: dict[str, Any]
) -> None:
    from ems.contracts.availability import available_at, forecast_delay
    from ems.ingest.history.build import emission_key

    rec = make_recording(
        tmp_path_factory.mktemp("lookahead"),
        history["start"],
        history["end"],
        name="lookahead",
        locations=(POZNAN, RADOM),
        synthetic_locations=4,
        sat_gaps=history["gaps"],
        publication=_publication(history["start"], history["publish_local"]),
    )
    build(rec)
    root = rec.root
    events = read_built(root)
    assert len(events) == len(event_lines(root)) > 0
    start, _end = window(root)
    meter_delay = timedelta(seconds=load_generator_config(root / "generator.yaml").meter_delay_s)
    model = {s["location_id"]: s["model"] for s in manifest(root)["sites"]}
    runs, days = raw_runs(root), raw_pse_days(root)

    keys = [emission_key(e, start, meter_delay) for e in events]
    assert keys == sorted(keys)  # the file is in emission order

    last_start: dict[tuple[str, str], datetime] = {}
    for e, key in zip(events, keys, strict=True):
        emitted = key[0]
        if isinstance(e, MeasurementEvent):
            assert available_at(e, meter_delay) - e.interval_end >= meter_delay >= timedelta(0)
            assert emitted >= e.interval_end + meter_delay
            assert record_timestamp(e) == e.interval_end
        elif e.stream is Stream.WEATHER_FORECAST:
            delay = forecast_delay(e.payload.forecast_model)
            assert e.payload.forecast_model == model[e.location_id]
            assert available_at(e, meter_delay) - e.issued_at >= delay
            run = e.issued_at
            assert (e.location_id, run) in runs  # issued_at is the raw ``run``
            assert emitted >= run + forecast_delay(model[e.location_id])
            assert record_timestamp(e) == e.issued_at
        else:
            assert e.stream is Stream.PRICE
            published = max(raw_publications(days[e.payload.delivery_day]))
            assert e.issued_at == published  # the latest raw publication_ts_utc
            assert available_at(e, meter_delay) - e.issued_at >= timedelta(0)
            assert emitted >= published
            assert record_timestamp(e) == e.issued_at
        assert emitted >= start

        subject = e.location_id or e.payload.market.value
        prev = last_start.get((e.stream.value, subject))
        assert prev is None or e.interval_start >= prev, (e.stream, subject, prev)
        last_start[(e.stream.value, subject)] = e.interval_start
