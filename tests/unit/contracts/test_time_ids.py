"""AC-001-08/-09/-10/-18/-25: time base, identity and metering-point codes."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from ems.contracts.ids import event_id, gs1_check_digit, is_valid_gsrn, location_seed, make_gsrn
from ems.contracts.time import (
    fmt_utc,
    iso_duration,
    local_day_bounds,
    local_day_slots,
    quarter_index,
)

T0 = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)


def test_ac_001_25_delivery_day_slot_counts() -> None:
    assert len(local_day_slots(date(2026, 3, 29))) == 92
    assert len(local_day_slots(date(2026, 10, 25))) == 100
    assert len(local_day_slots(date(2026, 10, 21))) == 96
    assert len(local_day_slots(date(2025, 10, 26), timedelta(hours=1))) == 25


def test_local_day_bounds_are_local_midnights_in_utc() -> None:
    assert local_day_bounds(date(2026, 10, 21)) == (
        datetime(2026, 10, 20, 22, tzinfo=UTC),
        datetime(2026, 10, 21, 22, tzinfo=UTC),
    )
    assert local_day_bounds(date(2026, 10, 25)) == (
        datetime(2026, 10, 24, 22, tzinfo=UTC),
        datetime(2026, 10, 25, 23, tzinfo=UTC),
    )


def test_ac_001_08_t_is_utc_quarter_index() -> None:
    assert quarter_index(T0) == int(T0.timestamp()) // 900
    assert quarter_index(T0 + timedelta(minutes=14, seconds=59)) == quarter_index(T0)
    plus_two = T0.astimezone(timezone(timedelta(hours=2)))
    assert quarter_index(plus_two) == quarter_index(T0)


def test_canonical_utc_format_is_fixed_width_with_z() -> None:
    assert fmt_utc(T0) == "2026-10-20T00:00:00.000Z"
    assert fmt_utc(T0.astimezone(timezone(timedelta(hours=2)))) == "2026-10-20T00:00:00.000Z"
    assert fmt_utc(T0 + timedelta(microseconds=31_999)) == "2026-10-20T00:00:00.031Z"
    with pytest.raises(ValueError):
        fmt_utc(datetime(2026, 10, 20))


def test_iso_duration() -> None:
    assert iso_duration(timedelta(minutes=75)) == "PT1H15M"
    assert iso_duration(timedelta(0)) == "PT0S"
    assert iso_duration(timedelta(hours=48)) == "PT48H"


def _id(**overrides: object) -> object:
    base: dict[str, object] = {
        "stream": "production",
        "subject": "PL-LOC-000001",
        "interval_start": T0,
        "issued_at": None,
        "source_system": "synthetic",
        "run_id": None,
        "major": 1,
    }
    base.update(overrides)
    return event_id(**base)


def test_ac_001_09_event_id_is_deterministic() -> None:
    assert _id() == _id()
    assert _id(interval_start=T0.astimezone(timezone(timedelta(hours=2)))) == _id()
    assert _id(subject="PL-LOC-000002") != _id()
    assert _id(issued_at=T0) != _id()


def test_ac_001_10_replay_keeps_id_within_run_scope() -> None:
    live = _id(source_system="open-meteo")
    assert _id(source_system="replay:open-meteo") == live
    assert _id(source_system="replay:open-meteo", run_id="r1") != live
    assert _id(run_id="r1") != _id(run_id="r2")


def test_ac_001_18_gsrn() -> None:
    code = make_gsrn("3224", 1234567890)
    assert len(code) == 18 and code.startswith("5903224")
    assert is_valid_gsrn(code)
    bad = code[:-1] + str((int(code[-1]) + 1) % 10)
    assert not is_valid_gsrn(bad)
    # GS1 published example: 629104150021 -> check digit 3
    assert gs1_check_digit("629104150021") == 3


def test_location_seed_is_stable_and_bounded() -> None:
    assert location_seed(4242, "PL-LOC-000001") == location_seed(4242, "PL-LOC-000001")
    assert location_seed(4242, "PL-LOC-000001") != location_seed(4243, "PL-LOC-000001")
    assert 0 <= location_seed(4242, "PL-LOC-000001") < 2**63
