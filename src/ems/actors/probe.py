"""Probe: the Virtual Object fed from the four Kafka topics in stage 2 (spec 010, plan §5 and Actor
contract).

Key: the record key (``[run_id:]location_id`` or ``[run_id:]market_key``). One exclusive handler per
ingress container. Each takes the raw record value (the integration sets no content type and
validates nothing), reads the Kafka coordinates from the invocation headers, applies the pure check
of ``ingress_check`` and writes only its own state: ``log`` (compared with the offline fold),
``leads`` (measured) and ``n`` (records handled, last, so a progress query never counts a record
whose log is not written). It sends nothing and never raises on the content of a record. Stage 3
removes this object and keeps ``ingress_check.log_apply`` in the real handlers.
"""

from __future__ import annotations

from typing import Any

from restate import ObjectContext, ObjectSharedContext, VirtualObject
from restate.serde import BytesSerde

from ems.actors import ingress_check

probe = VirtualObject("Probe")


async def _handle(ctx: ObjectContext, raw: bytes) -> None:
    meta = ingress_check.kafka_meta(ctx.request().headers)
    log = await ctx.get("log") or ingress_check.new_log()
    leads = await ctx.get("leads") or ingress_check.new_leads()
    n = await ctx.get("n") or 0
    log, leads = ingress_check.apply(log, leads, meta, raw or b"")
    ctx.set("log", log)
    ctx.set("leads", leads)
    ctx.set("n", n + 1)


@probe.handler(input_serde=BytesSerde(), accept="*/*")
async def on_measurement(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.production.v1` and `ems.consumption.v1`, through `ingress-meters`."""
    await _handle(ctx, raw)


@probe.handler(input_serde=BytesSerde(), accept="*/*")
async def on_forecast(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.weather.forecast.v1`, through `ingress-forecasts`."""
    await _handle(ctx, raw)


@probe.handler(input_serde=BytesSerde(), accept="*/*")
async def on_price(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.price.v1`, through `ingress-prices`."""
    await _handle(ctx, raw)


@probe.handler(kind="shared")
async def status(ctx: ObjectSharedContext) -> dict[str, Any]:
    """The key's log, leads and record count (called only from outside: tests and jobs)."""
    return {
        "log": await ctx.get("log") or ingress_check.new_log(),
        "leads": await ctx.get("leads") or ingress_check.new_leads(),
        "n": await ctx.get("n") or 0,
    }
