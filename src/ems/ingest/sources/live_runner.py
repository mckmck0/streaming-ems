"""Run live sources (PSE, Open-Meteo, optionally ENTSO-E) next to synthetic locations (hybrid mode).

Live sources poll in realtime (wall clock, principle 7 allows it outside experiments). For
live-weather sites the synthetic meters follow the live weather: at each interval end their PV and
heat-pump energy are computed from a truth that is the latest live forecast step minus the synthetic
forecast error at that step's lead (``LiveForecastTruth``, spec 007), so the meters deviate from the
forecasts strategies see. Before the first run arrives, or past its horizon, the truth is synthetic;
base load stays synthetic.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import structlog

from ems.contracts.envelope import WeatherForecastEvent
from ems.contracts.time import QUARTER, floor_quarter, quarter_index
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import GeneratorConfig, SyntheticModel, _meter_builders
from ems.ingest.kafka.sink import Sink
from ems.ingest.live.entsoe import EntsoeSource
from ems.ingest.live.http import Fetcher, Recorder
from ems.ingest.live.openmeteo import OpenMeteoSource
from ems.ingest.live.pse import REPORTS, PseSource, days_to_poll, poll_interval
from ems.ingest.rng import location_key
from ems.ingest.synthetic.truth import LiveForecastTruth, SyntheticTruth
from ems.settings import EmsSettings

log = structlog.get_logger(__name__)
Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]


@dataclass
class LiveState:
    forecasts: dict[str, WeatherForecastEvent] = field(default_factory=dict)
    counts: Counter[str] = field(default_factory=Counter)
    tomorrow_complete: bool = False


def _secret(value: Any) -> str | None:
    return value.get_secret_value() if value is not None else None


@dataclass
class LiveRunner:
    settings: EmsSettings
    cfg: GeneratorConfig
    sink: Sink
    live_sites: tuple[Site, ...]
    price_live: bool
    clock: Clock
    sleep: Sleep = asyncio.sleep
    run_id: str | None = None
    recorder: Recorder | None = None
    state: LiveState = field(default_factory=LiveState)

    def __post_init__(self) -> None:
        mappings = self.settings.mappings_dir
        om_key = _secret(self.settings.open_meteo_api_key)
        token = _secret(self.settings.entsoe_token)
        secrets = [s for s in (om_key, token) if s]
        self.pse = [
            PseSource(
                Fetcher(REPORTS[r].source, recorder=self.recorder, secrets=secrets),
                REPORTS[r],
                mappings,
            )
            for r in self.cfg.live.get("pse", {}).get("reports", ["rce-pln", "csdac-pln"])
        ]
        self.openmeteo = OpenMeteoSource(
            Fetcher("open-meteo", recorder=self.recorder, secrets=secrets), mappings, api_key=om_key
        )
        self.entsoe: EntsoeSource | None = None
        if self.cfg.live.get("entsoe", {}).get("enabled") and token:
            self.entsoe = EntsoeSource(
                Fetcher("entsoe", recorder=self.recorder, secrets=secrets, retry_on_429=False),
                token,
                mappings,
            )
        self.live_model = (
            SyntheticModel(list(self.live_sites), self.cfg.seed, self.settings.config_dir)
            if self.live_sites
            else None
        )
        self.location_by_key = {
            int(location_key(s.location_id)): s.location_id for s in self.live_sites
        }

    def _dlq(self, value: str, error: str) -> None:
        publish_invalid = getattr(self.sink, "publish_invalid", None)
        if publish_invalid is not None:
            publish_invalid(value.encode(), "ENTSOE:PL", error)
        self.state.counts["dlq"] += 1

    def _publish(self, event: Any) -> None:
        self.sink.publish(event)
        self.state.counts[event.stream.value] += 1

    async def prices_once(self) -> timedelta:
        now = self.clock()
        complete = True
        for src in self.pse:
            for day in days_to_poll(now):
                ev = await src.poll(day, now, self.run_id)
                if ev is not None:
                    self._publish(ev)
                if day == days_to_poll(now)[1] and src.report.endpoint == "rce-pln":
                    complete = day in src.last_published
        if self.entsoe is not None:
            for day in days_to_poll(now):
                events, rejected = await self.entsoe.poll(day, now, self.run_id)
                for ev in events:
                    self._publish(ev)
                for dp in rejected:
                    log.error("entsoe_conflicts", day=dp.day.isoformat(), slots=len(dp.conflicts))
                    self._dlq(
                        json.dumps(
                            {
                                "source": "entsoe",
                                "day": dp.day.isoformat(),
                                "conflicts": dp.conflicts,
                            }
                        ),
                        f"entsoe A44 {dp.day}: conflicting series at {len(dp.conflicts)} slots",
                    )
        self.state.tomorrow_complete = complete
        return poll_interval(now, complete)

    async def weather_once(self) -> None:
        now = self.clock()
        for site in self.live_sites:
            ev = await self.openmeteo.poll(site, now, now, self.run_id)
            if ev is not None:
                self.state.forecasts[site.location_id] = ev
                self._publish(ev)

    def live_meters(self, interval_start: datetime) -> None:
        """Meter events of live-weather sites for one interval, from their live weather."""
        if self.live_model is None:
            return
        t = quarter_index(interval_start)
        model = self.live_model
        source = LiveForecastTruth(
            location_by_key=self.location_by_key,
            forecasts=dict(self.state.forecasts),  # snapshot: the chunk is a function of its inputs
            fallback=SyntheticTruth(model.seed, model.weather_params),
            seed=model.seed,
            params=model.weather_params,
        )
        chunk = model.chunk(t, 1, [], truth_source=source)
        for stream, build in _meter_builders(self.live_model, chunk, 0, self.run_id):
            if self.cfg.stream_on(stream):
                self._publish(build(self.clock()))

    async def run_forever(self) -> None:
        async def prices() -> None:
            while True:
                try:
                    wait = await self.prices_once()
                except Exception:
                    log.exception("price_poll_failed")
                    wait = timedelta(minutes=15)
                await self.sleep(wait.total_seconds())

        async def weather() -> None:
            period = float(self.cfg.live.get("open_meteo", {}).get("meta_poll_s", 300))
            while True:
                try:
                    await self.weather_once()
                except Exception:
                    log.exception("weather_poll_failed")
                await self.sleep(period)

        async def meters() -> None:
            nxt = floor_quarter(self.clock()) + QUARTER
            while True:
                await self.sleep(max(0.0, (nxt - self.clock()).total_seconds()))
                self.live_meters(nxt - QUARTER)
                nxt += QUARTER

        tasks = []
        if self.price_live:
            tasks.append(prices())
        if self.live_sites:
            tasks += [weather(), meters()]
        await asyncio.gather(*tasks)

    async def once(self) -> None:
        if self.price_live:
            await self.prices_once()
        if self.live_sites:
            await self.weather_once()


def mask_synthetic(cfg: GeneratorConfig, price_live: bool) -> GeneratorConfig:
    """Config for the synthetic part: prices off when prices are live (one price source)."""
    streams = {k: dict(v) for k, v in cfg.streams.items()}
    if price_live:
        streams["price"] = {"mode": "off"}
    if streams.get("weather_forecast", {}).get("mode") == "live":
        streams["weather_forecast"] = {"mode": "synthetic"}  # synthetic sites still get forecasts
    return dataclasses.replace(cfg, streams=streams)
