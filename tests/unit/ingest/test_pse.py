"""AC-002-03/-04/-05: PSE mappings on recorded fixtures, completeness, revisions,
paging, schedule.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from ems.contracts.envelope import Market, to_json_bytes, validate_strict
from ems.ingest.live.http import Fetcher
from ems.ingest.live.pse import (
    REPORTS,
    IncompleteDay,
    PseSource,
    build_day_event,
    days_to_poll,
    poll_interval,
)
from ems.ingest.mapping.engine import apply, builtin

REPO = Path(__file__).resolve().parents[3]
FIX = REPO / "tests" / "fixtures" / "pse"
MAPPINGS = REPO / "mappings"
NOW = datetime(2026, 10, 20, 20, tzinfo=UTC)


def rows(name: str, report: str = "rce-pln") -> list:
    return apply(builtin(REPORTS[report].mapping, MAPPINGS), json.loads((FIX / name).read_text()))


@pytest.mark.parametrize(
    ("day", "slots", "first", "last_end"),
    [
        (
            "2025-03-30",
            92,
            datetime(2025, 3, 29, 23, tzinfo=UTC),
            datetime(2025, 3, 30, 22, tzinfo=UTC),
        ),
        (
            "2025-10-26",
            100,
            datetime(2025, 10, 25, 22, tzinfo=UTC),
            datetime(2025, 10, 26, 23, tzinfo=UTC),
        ),
        (
            "2026-03-29",
            92,
            datetime(2026, 3, 28, 23, tzinfo=UTC),
            datetime(2026, 3, 29, 22, tzinfo=UTC),
        ),
        (
            "2026-10-21",
            96,
            datetime(2026, 10, 20, 22, tzinfo=UTC),
            datetime(2026, 10, 21, 22, tzinfo=UTC),
        ),
    ],
)
def test_ac_002_03_dst_days_map_to_unique_utc_slots(
    day: str, slots: int, first: datetime, last_end: datetime
) -> None:
    d = date.fromisoformat(day)
    event = build_day_event(REPORTS["rce-pln"], d, rows(f"rce-pln_{day}.json"), NOW)
    s = event.payload.slots
    assert len(s) == slots and s[0].interval_start == first and s[-1].interval_end == last_end
    assert all(
        b.interval_start - a.interval_start == timedelta(minutes=15)
        for a, b in itertools.pairwise(s)
    )
    assert event.payload.unit == "PLN/MWh" and event.payload.market is Market.PL_RCE
    assert not event.synthetic and event.source_system == "pse-rce"
    validate_strict(json.loads(to_json_bytes(event)))


def test_issued_at_is_the_pse_publication_time() -> None:
    event = build_day_event(
        REPORTS["rce-pln"], date(2026, 10, 21), rows("rce-pln_2026-10-21.json"), NOW
    )
    assert event.issued_at == datetime(2026, 10, 20, 11, 42, 11, 720000, tzinfo=UTC)


def test_csdac_is_the_sdac_market() -> None:
    event = build_day_event(
        REPORTS["csdac-pln"],
        date(2026, 10, 21),
        rows("csdac-pln_2026-10-21.json", "csdac-pln"),
        NOW,
    )
    assert event.payload.market is Market.PL_SDAC and len(event.payload.slots) == 96


def test_ac_002_04_empty_and_incomplete_days_are_not_emitted() -> None:
    with pytest.raises(IncompleteDay):
        build_day_event(REPORTS["rce-pln"], date(2026, 12, 24), rows("rce-pln_empty.json"), NOW)
    partial = rows("rce-pln_2026-10-21.json")[:-1]
    with pytest.raises(IncompleteDay):
        build_day_event(REPORTS["rce-pln"], date(2026, 10, 21), partial, NOW)


def _source() -> PseSource:
    async def no_sleep(_s: float) -> None:
        return None

    return PseSource(
        Fetcher("pse-rce", client=httpx.AsyncClient(), sleep=no_sleep), REPORTS["rce-pln"], MAPPINGS
    )


@respx.mock
def test_ac_002_05_same_publication_once_revision_again() -> None:
    body = json.loads((FIX / "rce-pln_2026-10-21.json").read_text())
    route = respx.get(url__startswith="https://api.raporty.pse.pl/api/rce-pln").mock(
        return_value=httpx.Response(200, json=body)
    )
    src = _source()
    d = date(2026, 10, 21)
    first = asyncio.run(src.poll(d, NOW))
    again = asyncio.run(src.poll(d, NOW + timedelta(minutes=15)))
    assert first is not None and again is None
    revised = json.loads(json.dumps(body))
    for r in revised["value"]:
        r["publication_ts_utc"] = "2026-10-20 14:05:00.000"
    route.mock(return_value=httpx.Response(200, json=revised))
    third = asyncio.run(src.poll(d, NOW + timedelta(hours=1)))
    assert third is not None and third.event_id != first.event_id


@respx.mock
def test_paging_follows_next_link() -> None:
    p1 = json.loads((FIX / "rce-pln_2025-10-26_page1.json").read_text())
    p2 = json.loads((FIX / "rce-pln_2025-10-26_page2.json").read_text())
    respx.get(url__startswith="https://api.raporty.pse.pl/api/rce-pln?%24filter").mock(
        return_value=httpx.Response(200, json=p1)
    )
    respx.get(url__regex=r".*\$after=.*").mock(return_value=httpx.Response(200, json=p2))
    event = asyncio.run(_source().poll(date(2025, 10, 26), NOW))
    assert event is not None and len(event.payload.slots) == 100


def test_poll_schedule() -> None:
    at = lambda h: datetime(2026, 10, 20, h, 30, tzinfo=UTC)  # noqa: E731  (UTC+2 local)
    assert poll_interval(at(11), tomorrow_complete=False) == timedelta(minutes=15)  # 13:30 local
    assert poll_interval(at(11), tomorrow_complete=True) == timedelta(hours=1)
    assert poll_interval(at(17), tomorrow_complete=False) == timedelta(hours=1)  # 19:30 local
    assert days_to_poll(at(23)) == [date(2026, 10, 21), date(2026, 10, 22)]  # 01:30 local next day
