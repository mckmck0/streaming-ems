"""AC-001-08: envelope rules, strict producer validation, tolerant consumer parsing."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from ems.contracts.envelope import (
    AssetType,
    ContractError,
    Direction,
    Market,
    Stream,
    parse_event,
    to_json_bytes,
    validate_strict,
)
from ems.contracts.examples import energy, golden_examples, measurement_event, price_event
from ems.contracts.time import local_day_slots

T0 = datetime(2026, 10, 20, tzinfo=UTC)
NOW = datetime(2026, 10, 20, 0, 20, tzinfo=UTC)


def _dump(event: Any) -> dict[str, Any]:
    return json.loads(to_json_bytes(event))


@pytest.mark.parametrize("name", sorted(golden_examples()))
def test_golden_examples_validate_strictly(name: str) -> None:
    validate_strict(_dump(golden_examples()[name]))


def test_datetimes_serialise_with_z() -> None:
    d = _dump(golden_examples()["production.json"])
    assert d["interval_start"] == "2026-10-20T00:00:00Z"
    assert d["schema"] == "ems.measurement"


def test_unknown_field_rejected_by_producer_but_ignored_by_consumer() -> None:
    d = _dump(golden_examples()["production.json"])
    d["payload"]["measurements"][0]["added_in_v1_1"] = 1
    d["schema_version"] = "1.1.0"
    with pytest.raises(ContractError, match="unknown fields"):
        validate_strict(d)
    assert parse_event(d).schema_version == "1.1.0"  # v1.0 reader accepts a v1.1 record


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(t=d["t"] + 1),
        lambda d: d.update(interval_end=d["interval_start"]),
        lambda d: d.update(location_id=None),
        lambda d: d.update(location_id="LOC-1"),
        lambda d: d.update(metering_point_id="590322400000000000"),
        lambda d: d.update(run_id="bad:id"),
        lambda d: d.update(schema_version="2.0.0"),
        lambda d: d["payload"]["measurements"][0].update(value=-1.0),
        lambda d: d["payload"]["measurements"][0].update(direction="consumption"),
        lambda d: d.update(interval_start="2026-10-20T00:00:00"),
    ],
)
def test_contract_violations_are_rejected(mutate: Any) -> None:
    d = _dump(golden_examples()["production.json"])
    mutate(d)
    with pytest.raises(ContractError):
        validate_strict(d)


def test_tampered_event_id_is_rejected() -> None:
    d = _dump(golden_examples()["production.json"])
    d["location_id"] = "PL-LOC-000009"
    with pytest.raises(ContractError, match="event_id"):
        validate_strict(d)


def test_nan_never_becomes_null() -> None:
    with pytest.raises(ValueError):
        energy("pv-1", AssetType.PV, Direction.GENERATION, float("nan"))


def test_price_event_needs_every_slot_of_the_local_day() -> None:
    dst = date(2026, 10, 25)
    ev = price_event(
        market=Market.PL_RCE,
        delivery_day=dst,
        prices=[1.0] * len(local_day_slots(dst)),
        published_at=datetime(2026, 10, 24, 12, tzinfo=UTC),
        ingested_at=NOW,
    )
    assert len(ev.payload.slots) == 100
    assert ev.location_id is None
    with pytest.raises(ValueError, match="prices needed"):
        price_event(
            market=Market.PL_RCE,
            delivery_day=dst,
            prices=[1.0] * 96,
            published_at=datetime(2026, 10, 24, 12, tzinfo=UTC),
            ingested_at=NOW,
        )
    d = _dump(ev)
    d["payload"]["slots"].pop()
    with pytest.raises(ContractError):
        validate_strict(d)


def test_negative_prices_allowed() -> None:
    day = date(2026, 4, 12)
    ev = price_event(
        market=Market.PL_RCE,
        delivery_day=day,
        prices=[-120.5] * 96,
        published_at=datetime(2026, 4, 11, 11, 30, tzinfo=UTC),
        ingested_at=NOW,
    )
    validate_strict(_dump(ev))


def test_ingested_at_does_not_change_identity() -> None:
    def build(ingested_at: datetime) -> Any:
        return measurement_event(
            stream=Stream.PRODUCTION,
            location_id="PL-LOC-000001",
            interval_start=T0,
            measurements=[energy("pv-1", AssetType.PV, Direction.GENERATION, 0.0)],
            ingested_at=ingested_at,
        )

    a = build(NOW)
    b = build(NOW + timedelta(hours=3))
    assert a.event_id == b.event_id
