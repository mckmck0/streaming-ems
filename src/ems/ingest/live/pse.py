"""PSE prices (Raporty API v2): RCE (net-billing reference) and the SDAC day-ahead price (csdac).

One event per report and business day, emitted only when every local-day slot is present.
``issued_at`` is PSE's publication time, so re-polling an unchanged publication yields the same
event id and a revision (new publication time) yields a new event.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import structlog

from ems.contracts.envelope import Market, PriceEvent
from ems.contracts.examples import price_event
from ems.contracts.time import WARSAW, local_day_slots
from ems.ingest.live.http import Fetcher
from ems.ingest.mapping.engine import Row, apply, builtin

log = structlog.get_logger(__name__)

BASE_URL = "https://api.raporty.pse.pl/api"


@dataclass(frozen=True)
class Report:
    endpoint: str
    market: Market
    mapping: str
    source: str


REPORTS = {
    "rce-pln": Report("rce-pln", Market.PL_RCE, "pse.rce.v2", "pse-rce"),
    "csdac-pln": Report("csdac-pln", Market.PL_SDAC, "pse.csdac.v2", "pse-csdac"),
}


class IncompleteDay(ValueError):
    """PSE has not published every slot of the day (yet)."""


def build_day_event(
    report: Report,
    day: date,
    rows: list[Row],
    ingested_at: datetime,
    run_id: str | None = None,
) -> PriceEvent:
    """Price event for ``day`` from mapped rows; raises ``IncompleteDay`` unless every slot is
    there.
    """
    rows = [r for r in rows if r.get("business_date") == day]
    expected = [s for s, _ in local_day_slots(day)]
    by_start = {r["interval_start"]: r for r in rows}
    if not rows or sorted(by_start) != expected:
        raise IncompleteDay(f"{report.endpoint} {day}: {len(by_start)} of {len(expected)} slots")
    published = max(r["published"] for r in rows)
    return price_event(
        market=report.market,
        delivery_day=day,
        prices=[float(by_start[s]["price"]) for s in expected],
        published_at=published,
        ingested_at=ingested_at,
        source_system=report.source,
        run_id=run_id,
        synthetic=False,
        source_record_ref=f"{report.endpoint}:{day.isoformat()}:{published.isoformat()}",
    )


@dataclass
class PseSource:
    fetcher: Fetcher
    report: Report
    mappings_dir: Path
    base_url: str = BASE_URL
    last_published: dict[date, datetime] = field(default_factory=dict)

    async def fetch_rows(self, day: date) -> list[Row]:
        """All records of a business day, following ``nextLink`` pages (``$first``,
        never ``$top``).
        """
        mapping = builtin(self.report.mapping, self.mappings_dir)
        url: str | None = f"{self.base_url}/{self.report.endpoint}"
        params: dict[str, Any] | None = {
            "$filter": f"business_date eq '{day.isoformat()}'",
            "$first": 1000,
        }
        rows: list[Row] = []
        pages = 0
        while url is not None:
            fetched = await self.fetcher.get(url, params=params)
            payload = json.loads(fetched.body)
            rows += apply(mapping, payload)
            url = payload.get("nextLink")
            params = None  # nextLink already carries the query
            pages += 1
            if pages > 20:
                raise RuntimeError("PSE paging did not terminate")
        return rows

    async def poll(
        self, day: date, ingested_at: datetime, run_id: str | None = None
    ) -> PriceEvent | None:
        """New event for ``day`` if PSE has a complete publication not emitted yet, else None."""
        rows = await self.fetch_rows(day)
        try:
            event = build_day_event(self.report, day, rows, ingested_at, run_id)
        except IncompleteDay as exc:
            log.info("pse_not_ready", detail=str(exc))
            return None
        if self.last_published.get(day) == event.issued_at:
            return None
        if day in self.last_published:
            log.warning("pse_revision", report=self.report.endpoint, day=day.isoformat())
        self.last_published[day] = event.issued_at
        return event


def poll_interval(now: datetime, tomorrow_complete: bool) -> timedelta:
    """15 min inside the D-1 publication window (13:00-18:00 local) until D+1 is complete, else
    hourly.
    """
    local = now.astimezone(WARSAW).time()
    if not tomorrow_complete and time(13, 0) <= local < time(18, 0):
        return timedelta(minutes=15)
    return timedelta(hours=1)


def days_to_poll(now: datetime) -> list[date]:
    """Today and tomorrow (local): D for revisions and the initial day, D+1 for the next
    publication.
    """
    today = now.astimezone(WARSAW).date()
    return [today, today + timedelta(days=1)]
