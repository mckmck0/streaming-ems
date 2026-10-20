"""AC-002-08: A44 normalisation, A03 forward-fill, contract selection, PT60M,
duplicates, conflicts.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx

from ems.contracts.envelope import Market, Resolution, to_json_bytes, validate_strict
from ems.ingest.live.entsoe import BASE_URL, EntsoeError, EntsoeSource, build_events, normalise
from ems.ingest.live.http import Fetcher

REPO = Path(__file__).resolve().parents[3]
FIX = REPO / "tests" / "fixtures" / "entsoe"
MAPPINGS = REPO / "mappings"
NOW = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)


def test_ac_002_08_a03_forward_fill_and_a01_selection() -> None:
    events, rejected = build_events((FIX / "a44_2026-10-21.xml").read_bytes(), MAPPINGS, NOW, NOW)
    assert rejected == [] and len(events) == 1
    ev = events[0]
    prices = [s.price for s in ev.payload.slots]
    assert len(prices) == 96 and ev.payload.resolution is Resolution.PT15M
    assert prices[:4] == [95.10] * 4 and prices[4] == 92.40  # positions 2-4 omitted (A03)
    assert (
        prices[32:48] == [-3.5] * 16 and prices[92:] == [98.70] * 4
    )  # last block to the period end
    assert 999.99 not in prices  # the A07 (intraday) series is excluded
    assert ev.payload.market is Market.ENTSOE_A44_PL and ev.payload.currency == "EUR"
    assert ev.payload.unit == "EUR/MWh"
    validate_strict(json.loads(to_json_bytes(ev)))


def test_history_keeps_native_pt60m() -> None:
    events, _ = build_events((FIX / "a44_2025-09-30_pt60m.xml").read_bytes(), MAPPINGS, NOW, NOW)
    assert len(events) == 1 and events[0].payload.resolution is Resolution.PT1H
    assert len(events[0].payload.slots) == 24 and events[0].payload.delivery_day == date(
        2025, 9, 30
    )


def test_conflicting_duplicates_are_rejected_not_emitted() -> None:
    events, rejected = build_events((FIX / "a44_conflict.xml").read_bytes(), MAPPINGS, NOW, NOW)
    assert events == [] and len(rejected) == 1 and rejected[0].conflicts


def test_no_data_acknowledgement_and_bad_documents() -> None:
    assert normalise((FIX / "ack_no_data.xml").read_bytes()).no_data
    with pytest.raises(EntsoeError):
        normalise(b"<html>oops</html>")
    with pytest.raises(EntsoeError):
        normalise(b"<not-closed")


def test_entities_are_not_resolved() -> None:
    ns = b"urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"
    evil = (
        b'<?xml version="1.0"?>\n<!DOCTYPE d [<!ENTITY x SYSTEM "file:///etc/passwd">]>\n'
        b'<Publication_MarketDocument xmlns="'
        + ns
        + b'"><mRID>&x;</mRID></Publication_MarketDocument>'
    )
    assert normalise(evil).rows == []


async def no_sleep(_s: float) -> None:
    return None


@respx.mock
def test_token_in_header_and_429_backs_off_ten_minutes() -> None:
    token = "entsoe-token-abc"
    route = respx.get(url__startswith=BASE_URL).mock(
        return_value=httpx.Response(200, content=(FIX / "a44_2026-10-21.xml").read_bytes())
    )
    fetcher = Fetcher(
        "entsoe", client=httpx.AsyncClient(), sleep=no_sleep, secrets=[token], retry_on_429=False
    )
    src = EntsoeSource(fetcher, token, MAPPINGS)
    events, _ = asyncio.run(src.poll(date(2026, 10, 21), NOW))
    req = route.calls[0].request
    assert len(events) == 1 and req.headers["SECURITY_TOKEN"] == token and token not in str(req.url)
    assert "contract_MarketAgreement.type=A01" in str(req.url)
    again, _ = asyncio.run(src.poll(date(2026, 10, 21), NOW))
    assert again == []  # identical content is not re-emitted
    route.mock(return_value=httpx.Response(429))
    assert asyncio.run(src.poll(date(2026, 10, 22), NOW)) == ([], [])
    assert src.banned_until is not None and (src.banned_until - NOW).total_seconds() == 600
    calls = route.call_count
    asyncio.run(src.poll(date(2026, 10, 22), NOW))
    assert route.call_count == calls  # no request inside the ban window
