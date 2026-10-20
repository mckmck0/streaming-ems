"""ENTSO-E A44 day-ahead prices for the PL zone (optional source, needs a token).

The XML is parsed defensively (no entities, no DTD, no network) and by local name, so both schema
versions 7:0 and 7:3 work. Only contract type A01 (day-ahead) is used. curveType A03 sends a
position only where the value changes: the value carries forward to the next position or to the
period end; gaps between periods stay missing. Identical duplicated series (a known platform issue)
collapse; conflicting ones are reported.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog
from lxml import etree  # ty: ignore[unresolved-import]

from ems.contracts.envelope import Market, PriceEvent, Resolution
from ems.contracts.examples import price_event
from ems.contracts.time import local_date, local_day_bounds, local_day_slots
from ems.ingest.live.http import Fetcher, SourceHTTPError
from ems.ingest.mapping.engine import Row, apply, builtin

log = structlog.get_logger(__name__)

BASE_URL = "https://web-api.tp.entsoe.eu/api"
PL_ZONE = "10YPL-AREA-----S"
NAMESPACES = {
    "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0",
    "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3",
}
BAN_BACKOFF = timedelta(minutes=10)
RESOLUTIONS = {"PT15M": timedelta(minutes=15), "PT60M": timedelta(hours=1)}


class EntsoeError(ValueError):
    pass


@dataclass(frozen=True)
class Normalised:
    rows: list[dict[str, Any]]
    no_data: bool = False
    reason: str = ""


def _parser() -> etree.XMLParser:
    return etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False)


def _local(el: Any) -> str:
    return etree.QName(el).localname


def _child(el: Any, name: str) -> Any:
    for c in el:
        if isinstance(c.tag, str) and _local(c) == name:
            return c
    return None


def _text(el: Any, name: str) -> str | None:
    c = _child(el, name)
    return None if c is None or c.text is None else c.text.strip()


def _ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def normalise(xml: bytes, contract: str = "A01") -> Normalised:
    """One row per slot (A01 series only), with A03 positions forward-filled to the period end."""
    try:
        root = etree.fromstring(xml, _parser())
    except etree.XMLSyntaxError as exc:
        raise EntsoeError(f"malformed XML: {exc}") from exc
    if _local(root) == "Acknowledgement_MarketDocument":
        reason = _child(root, "Reason")
        code = _text(reason, "code") if reason is not None else None
        text = _text(reason, "text") if reason is not None else ""
        if code == "999":
            return Normalised(rows=[], no_data=True, reason=text or "")
        raise EntsoeError(f"acknowledgement {code}: {text}")
    if (
        _local(root) != "Publication_MarketDocument"
        or etree.QName(root).namespace not in NAMESPACES
    ):
        raise EntsoeError(f"unexpected document {etree.QName(root)}")
    rows: list[dict[str, Any]] = []
    for ts in root:
        if not isinstance(ts.tag, str) or _local(ts) != "TimeSeries":
            continue
        if _text(ts, "contract_MarketAgreement.type") not in (None, contract):
            continue
        curve = _text(ts, "curveType") or "A01"
        base = {
            "mRID": _text(ts, "mRID"),
            "currency_Unit.name": _text(ts, "currency_Unit.name"),
            "price_Measure_Unit.name": _text(ts, "price_Measure_Unit.name"),
        }
        for period in ts:
            if not isinstance(period.tag, str) or _local(period) != "Period":
                continue
            interval = _child(period, "timeInterval")
            start = _ts(_text(interval, "start") or "")
            end = _ts(_text(interval, "end") or "")
            res_text = _text(period, "resolution") or ""
            if res_text not in RESOLUTIONS:
                raise EntsoeError(f"unsupported resolution {res_text}")
            step = RESOLUTIONS[res_text]
            n = int((end - start) / step)
            points = {}
            for pt in period:
                if isinstance(pt.tag, str) and _local(pt) == "Point":
                    points[int(_text(pt, "position") or 0)] = _text(pt, "price.amount")
            if not points:
                continue
            if curve == "A01" and len(points) != n:
                raise EntsoeError(f"A01 period with {len(points)} of {n} positions")
            current = None
            for pos in range(1, n + 1):
                if pos in points:
                    current = points[pos]
                if current is None:
                    continue
                rows.append(
                    {
                        **base,
                        "interval_start": (start + (pos - 1) * step).isoformat(),
                        "price.amount": current,
                        "resolution": res_text,
                    }
                )
    return Normalised(rows=rows)


@dataclass(frozen=True)
class DayPrices:
    day: date
    resolution: Resolution
    currency: str
    prices: list[float]
    conflicts: list[str]


def day_prices(rows: list[Row]) -> list[DayPrices]:
    """Group mapped rows into complete local delivery days; collapse identical duplicates."""
    by_slot: dict[tuple[datetime, str], set[float]] = defaultdict(set)
    meta: dict[tuple[datetime, str], str] = {}
    for r in rows:
        key = (r["interval_start"], r["resolution"])
        by_slot[key].add(float(r["price"]))
        meta[key] = r["currency"]
    days: dict[tuple[date, str], dict[datetime, set[float]]] = defaultdict(dict)
    for (start, res), values in by_slot.items():
        days[(local_date(start), res)][start] = values
    out = []
    for (day, res), slots in sorted(days.items()):
        resolution = Resolution(res)
        expected = [s for s, _ in local_day_slots(day, resolution.delta)]
        if sorted(slots) != expected:
            log.info(
                "entsoe_incomplete_day",
                day=day.isoformat(),
                slots=len(slots),
                expected=len(expected),
            )
            continue
        conflicts = [s.isoformat() for s in expected if len(slots[s]) > 1]
        currencies = {meta[(s, res)] for s in expected}
        if len(currencies) != 1 or next(iter(currencies)) not in ("EUR", "PLN"):
            raise EntsoeError(f"{day}: unexpected currencies {currencies}")
        out.append(
            DayPrices(
                day=day,
                resolution=resolution,
                currency=next(iter(currencies)),
                prices=[min(slots[s]) for s in expected],
                conflicts=conflicts,
            )
        )
    return out


def build_events(
    xml: bytes,
    mappings_dir: Path,
    issued_at: datetime,
    ingested_at: datetime,
    run_id: str | None = None,
) -> tuple[list[PriceEvent], list[DayPrices]]:
    """Price events for complete conflict-free days; days with conflicts are returned
    for the DLQ.
    """
    normalised = normalise(xml)
    if normalised.no_data:
        return [], []
    rows = apply(builtin("entsoe.a44.v1", mappings_dir), {"rows": normalised.rows})
    events, rejected = [], []
    for dp in day_prices(rows):
        if dp.conflicts:
            rejected.append(dp)
            continue
        events.append(
            price_event(
                market=Market.ENTSOE_A44_PL,
                delivery_day=dp.day,
                prices=dp.prices,
                published_at=issued_at,
                ingested_at=ingested_at,
                resolution=dp.resolution,
                source_system="entsoe",
                run_id=run_id,
                synthetic=False,
                currency="EUR" if dp.currency == "EUR" else "PLN",
                source_record_ref=f"entsoe:A44:{dp.day.isoformat()}",
            )
        )
    return events, rejected


def content_hash(dp_prices: list[float]) -> str:
    return hashlib.sha256(json.dumps(dp_prices).encode()).hexdigest()[:16]


@dataclass
class EntsoeSource:
    """Polls one delivery day at a time. ``issued_at`` is the first time a content version was seen
    (A44 carries no publication time; ``createdDateTime`` changes on every request).
    """

    fetcher: Fetcher
    token: str
    mappings_dir: Path
    first_seen: dict[tuple[date, str], datetime] = field(default_factory=dict)
    banned_until: datetime | None = None

    def params(self, day: date) -> dict[str, str]:
        start, end = local_day_bounds(day)
        return {
            "documentType": "A44",
            "in_Domain": PL_ZONE,
            "out_Domain": PL_ZONE,
            "periodStart": start.strftime("%Y%m%d%H%M"),
            "periodEnd": end.strftime("%Y%m%d%H%M"),
            "contract_MarketAgreement.type": "A01",
        }

    async def poll(
        self, day: date, now: datetime, run_id: str | None = None
    ) -> tuple[list[PriceEvent], list[DayPrices]]:
        if self.banned_until is not None and now < self.banned_until:
            return [], []
        try:
            fetched = await self.fetcher.get(
                BASE_URL, params=self.params(day), headers={"SECURITY_TOKEN": self.token}
            )
        except SourceHTTPError as exc:
            if exc.status == 429:
                self.banned_until = now + BAN_BACKOFF
                log.warning("entsoe_rate_limited", until=self.banned_until.isoformat())
                return [], []
            raise
        events, rejected = build_events(fetched.body, self.mappings_dir, now, now, run_id)
        out = []
        for ev in events:
            key = (ev.payload.delivery_day, content_hash([s.price for s in ev.payload.slots]))
            if key in self.first_seen:
                continue  # same content already emitted
            self.first_seen[key] = now
            out.append(ev)
        return out, rejected
