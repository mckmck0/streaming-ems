"""Pick the source per stream and location: synthetic, live (for ``live.locations``) or off.

Hybrid mode = some streams live for the live locations while every other location stays synthetic,
on the same topics. Prices are market-wide: exactly one price source (synthetic or live) is active.
"""

from __future__ import annotations

from dataclasses import dataclass

from ems.ingest.catalog.sites import Site
from ems.ingest.generator import GeneratorConfig

MODES = {"synthetic", "live", "replay", "off"}


class RoutingError(ValueError):
    pass


@dataclass(frozen=True)
class Routing:
    price: str
    weather: str
    live_sites: tuple[Site, ...]  # weather from live sources; synthetic meters follow it
    synthetic_sites: tuple[Site, ...]  # fully synthetic

    @property
    def hybrid(self) -> bool:
        return bool(self.live_sites) and bool(self.synthetic_sites)

    @property
    def needs_live(self) -> bool:
        return self.price == "live" or bool(self.live_sites)


def route(cfg: GeneratorConfig, sites: list[Site]) -> Routing:
    modes = {
        name: cfg.stream_mode(name)
        for name in ("weather_forecast", "production", "consumption", "price")
    }
    for name, mode in modes.items():
        if mode not in MODES:
            raise RoutingError(f"stream {name}: unknown mode {mode!r}")
    for name in ("production", "consumption"):
        if modes[name] == "live":
            raise RoutingError(f"{name}: no live meter source is configured (§16.3: none)")
    live_ids = set(cfg.live_locations)
    unknown = live_ids - {s.location_id for s in sites}
    if unknown:
        raise RoutingError(f"live.locations not in the catalog: {sorted(unknown)}")
    if modes["weather_forecast"] == "live":
        live = tuple(s for s in sites if s.location_id in live_ids)
        synthetic = tuple(s for s in sites if s.location_id not in live_ids)
    else:
        live, synthetic = (), tuple(sites)
    return Routing(
        price=modes["price"],
        weather=modes["weather_forecast"],
        live_sites=live,
        synthetic_sites=synthetic,
    )
