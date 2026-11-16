"""AC-010-10, AC-010-12, AC-010-17: the pure ingress check behind the probe (spec 010, plan §4).

- AC-010-12: the log comes from one pure function (standard library and ``ems.contracts`` only, no
  clock). Per topic it keeps the partition, the record count, the last offset, a SHA-256 chain over
  the ``partition:offset`` steps, the non-increasing offset steps and the partition changes.
- AC-010-10: a record without Kafka metadata, or whose value is not JSON or breaks the v1 contract
  (a date at the end of the calendar included, which raises ``OverflowError`` in the parser), counts
  as rejected; its topic's entry still advances, so the later records of the key follow in order.
- AC-010-17: per key, the newest ``available_at`` per wire stream (quarter-hours since the epoch)
  and the largest lead of each stream over each other stream; a record without a slot is skipped.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ems.actors import ingress_check as ic
from ems.contracts.envelope import AssetType, Direction, Stream, parse_event, to_json_bytes
from ems.contracts.examples import energy, forecast_event, golden_examples, measurement_event
from ems.contracts.time import QUARTER, quarter_index

SOURCE = Path(__file__).resolve().parents[3] / "src" / "ems" / "actors" / "ingress_check.py"
ALLOWED = {
    "__future__",
    "hashlib",
    "json",
    "functools",
    "collections.abc",
    "typing",
    "dataclasses",
    "datetime",
}

T0 = datetime(2026, 10, 20, tzinfo=UTC)
Q0 = quarter_index(T0)
INGESTED = datetime(2026, 10, 20, 12, tzinfo=UTC)
LOC = "PL-LOC-000001"
TOPIC = "ems.production.v1"
GOLDEN = {name: to_json_bytes(event) for name, event in golden_examples().items()}
VALID = GOLDEN["production.json"]
NOT_JSON = b"\xff\xfe{not json"
STEP: dict[str, Any] = {
    "ghi_wm2": 0.0,
    "dni_wm2": 0.0,
    "dhi_wm2": 0.0,
    "temp_air_c": 7.4,
    "cloud_cover_pct": 88,
    "wind_speed_ms": {"10": 3.1, "100": 6.0, "120": 6.2},
    "wind_dir_deg": 240,
}
HEADERS = {
    "kafka.key": LOC,
    "kafka.topic": TOPIC,
    "kafka.partition": "3",
    "kafka.offset": "42",
    "kafka.timestamp": "1792022400000",
}


def _meter(stream: Stream, quarter: int) -> bytes:
    """A measurement of ``LOC`` for the interval starting ``quarter`` quarter-hours after T0.

    It is available at the end of its interval, in slot ``Q0 + quarter + 1``.
    """
    if stream is Stream.PRODUCTION:
        measurement = energy("pv-1", AssetType.PV, Direction.GENERATION, 0.25)
    else:
        measurement = energy("load-1", AssetType.LOAD, Direction.CONSUMPTION, 0.1)
    event = measurement_event(
        stream=stream,
        location_id=LOC,
        interval_start=T0 + quarter * QUARTER,
        measurements=[measurement],
        ingested_at=INGESTED,
    )
    return to_json_bytes(event)


def _chain(*steps: tuple[int, int]) -> str:
    """The digest after the (partition, offset) steps: ``sha256(previous + ":p:o")`` from ``""``."""
    digest = ""
    for partition, offset in steps:
        digest = hashlib.sha256(f"{digest}:{partition}:{offset}".encode()).hexdigest()
    return digest


def _contract_breaking() -> bytes:
    doc = json.loads(VALID)
    doc["t"] += 1  # valid JSON; v1 requires t == quarter_index(interval_start)
    return json.dumps(doc).encode()


def _end_of_calendar() -> bytes:
    doc = json.loads(GOLDEN["price.json"])
    doc["payload"]["delivery_day"] = (
        "9999-12-31"  # the day after it overflows in ems.contracts.time
    )
    return json.dumps(doc).encode()


def _unknown_model_forecast() -> bytes:
    event = forecast_event(
        location_id=LOC,
        issued_at=T0,
        steps=[STEP, STEP],
        ingested_at=INGESTED,
        forecast_model="gfs_seamless",  # valid on the wire, but has no availability delay
    )
    return to_json_bytes(event)


BAD_RECORDS: dict[str, tuple[bytes, type[Exception]]] = {
    "not-json": (NOT_JSON, ValueError),
    "contract-breaking": (_contract_breaking(), ValueError),
    "end-of-calendar": (_end_of_calendar(), OverflowError),
}


# ── Kafka metadata ────────────────────────────────────────────────────────────────────────────


def test_ac_010_12_kafka_meta_reads_topic_partition_and_offset() -> None:
    assert ic.kafka_meta(HEADERS) == (TOPIC, 3, 42)


def test_ac_010_12_kafka_meta_matches_header_names_case_insensitively() -> None:
    headers = {"Kafka.Topic": TOPIC, "KAFKA.PARTITION": "3", "kafka.Offset": "42"}
    assert ic.kafka_meta(headers) == (TOPIC, 3, 42)


@pytest.mark.parametrize("missing", ["kafka.topic", "kafka.partition", "kafka.offset"])
def test_ac_010_12_kafka_meta_is_none_without_one_of_its_headers(missing: str) -> None:
    assert ic.kafka_meta({k: v for k, v in HEADERS.items() if k != missing}) is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("kafka.partition", "three"),
        ("kafka.partition", ""),
        ("kafka.offset", "4.2"),
        ("kafka.offset", ""),
    ],
)
def test_ac_010_12_kafka_meta_is_none_for_a_non_integer_partition_or_offset(
    name: str, value: str
) -> None:
    assert ic.kafka_meta({**HEADERS, name: value}) is None


# ── the log ───────────────────────────────────────────────────────────────────────────────────


def test_ac_010_12_new_log_is_empty() -> None:
    assert ic.new_log() == {"topics": {}, "rejected": 0}


def test_ac_010_12_first_record_of_a_topic_opens_its_entry() -> None:
    log = ic.log_apply(ic.new_log(), (TOPIC, 2, 7), VALID)
    assert log == {
        "topics": {
            TOPIC: {
                "partition": 2,
                "n": 1,
                "last_offset": 7,
                # sha256(b":2:7"): the previous digest of a new entry is the empty string
                "digest": "07c4f54a1416d23a9e5a7a0c5692182df3c51b24f2d4813615ae6621aeef0fd0",
                "regressions": 0,
                "partition_changes": 0,
            }
        },
        "rejected": 0,
    }


def test_ac_010_12_increasing_offsets_extend_the_digest_chain() -> None:
    log = ic.fold_log([((TOPIC, 2, offset), VALID) for offset in (7, 8, 11)])
    assert log == {
        "topics": {
            TOPIC: {
                "partition": 2,
                "n": 3,
                "last_offset": 11,
                "digest": _chain((2, 7), (2, 8), (2, 11)),
                "regressions": 0,
                "partition_changes": 0,
            }
        },
        "rejected": 0,
    }


@pytest.mark.parametrize("second", [7, 5], ids=["equal", "lower"])
def test_ac_010_12_non_increasing_offset_is_a_regression(second: int) -> None:
    log = ic.fold_log([((TOPIC, 2, 7), VALID), ((TOPIC, 2, second), VALID)])
    assert log == {
        "topics": {
            TOPIC: {
                "partition": 2,
                "n": 2,
                "last_offset": second,
                "digest": _chain((2, 7), (2, second)),
                "regressions": 1,
                "partition_changes": 0,
            }
        },
        "rejected": 0,
    }


def test_ac_010_12_offsets_after_a_regression_run_from_the_offset_just_seen() -> None:
    """7, 5, 6 is one non-increasing step: 6 follows 5, not 7."""
    entry = ic.fold_log([((TOPIC, 2, offset), VALID) for offset in (7, 5, 6)])["topics"][TOPIC]
    assert (entry["n"], entry["last_offset"], entry["regressions"]) == (3, 6, 1)


def test_ac_010_12_new_partition_is_a_partition_change_and_starts_a_new_offset_run() -> None:
    log = ic.fold_log([((TOPIC, 0, 10), VALID), ((TOPIC, 1, 2), VALID), ((TOPIC, 1, 3), VALID)])
    assert log["topics"][TOPIC] == {
        "partition": 1,
        "n": 3,
        "last_offset": 3,
        "digest": _chain((0, 10), (1, 2), (1, 3)),
        "regressions": 0,
        "partition_changes": 1,
    }


def test_ac_010_12_log_apply_returns_a_new_dict_and_leaves_its_input_alone() -> None:
    log = ic.fold_log([((TOPIC, 2, 7), VALID), (("ems.consumption.v1", 1, 3), NOT_JSON)])
    before = copy.deepcopy(log)
    for meta, raw in [
        ((TOPIC, 2, 8), VALID),  # same partition
        ((TOPIC, 2, 7), NOT_JSON),  # regression and rejected
        ((TOPIC, 3, 0), VALID),  # partition change
        (("ems.price.v1", 0, 0), GOLDEN["price.json"]),  # first record of a topic
        (None, VALID),  # no Kafka metadata
    ]:
        assert ic.log_apply(log, meta, raw) is not log
        assert log == before


# ── rejected records (AC-010-10) ──────────────────────────────────────────────────────────────


def test_ac_010_10_record_without_kafka_metadata_is_rejected() -> None:
    assert ic.log_apply(ic.new_log(), None, VALID) == {"topics": {}, "rejected": 1}


@pytest.mark.parametrize("case", sorted(BAD_RECORDS))
def test_ac_010_10_bad_record_is_rejected_and_its_entry_still_advances(case: str) -> None:
    raw, error = BAD_RECORDS[case]
    with pytest.raises(error):  # the v1 parser refuses it; an OverflowError is no ValueError
        parse_event(raw)
    log = ic.fold_log([((TOPIC, 2, 7), raw), ((TOPIC, 2, 8), VALID)])
    assert log == {
        "topics": {
            TOPIC: {
                "partition": 2,
                "n": 2,
                "last_offset": 8,
                "digest": _chain((2, 7), (2, 8)),
                "regressions": 0,
                "partition_changes": 0,
            }
        },
        "rejected": 1,
    }


# ── the leads (AC-010-17) ─────────────────────────────────────────────────────────────────────


def test_ac_010_17_new_leads_are_empty() -> None:
    assert ic.new_leads() == {"newest": {}, "max": {}, "skipped": 0}


@pytest.mark.parametrize(
    ("name", "stream", "slot"),
    [
        ("production.json", "production", Q0 + 1),  # interval end 00:15
        ("consumption.json", "consumption", Q0 + 1),
        ("weather_forecast.json", "weather_forecast", Q0 + 8),  # issued 00:00 + 120 min (synthetic)
        ("price.json", "price", Q0 + 47),  # published 11:59:15, in the quarter from 11:45
    ],
)
def test_ac_010_17_slot_is_the_quarter_hour_of_available_at(
    name: str, stream: str, slot: int
) -> None:
    leads = ic.lead_apply(ic.new_leads(), GOLDEN[name])
    assert leads == {"newest": {stream: slot}, "max": {}, "skipped": 0}


def test_ac_010_17_production_then_consumption_of_one_key() -> None:
    """Production for 01:00-01:15 (slot Q0 + 5), then consumption for 00:00-00:15 (slot Q0 + 1):
    consumption trails production by four quarter-hours, and only its own pair is written.
    """
    leads = ic.lead_apply(ic.new_leads(), _meter(Stream.PRODUCTION, 4))
    assert leads == {"newest": {"production": Q0 + 5}, "max": {}, "skipped": 0}
    leads = ic.lead_apply(leads, _meter(Stream.CONSUMPTION, 0))
    assert leads == {
        "newest": {"production": Q0 + 5, "consumption": Q0 + 1},
        "max": {"consumption>production": -4},
        "skipped": 0,
    }


def test_ac_010_17_largest_lead_is_kept_and_newest_never_falls() -> None:
    leads = ic.new_leads()
    for raw in (
        _meter(Stream.PRODUCTION, 4),  # production Q0 + 5
        _meter(Stream.CONSUMPTION, 0),  # consumption Q0 + 1: lead -4
        _meter(Stream.CONSUMPTION, 8),  # consumption Q0 + 9: lead 4
        _meter(Stream.CONSUMPTION, 2),  # a late Q0 + 3: newest stays Q0 + 9, lead stays 4
    ):
        leads = ic.lead_apply(leads, raw)
    assert leads == {
        "newest": {"production": Q0 + 5, "consumption": Q0 + 9},
        "max": {"consumption>production": 4},
        "skipped": 0,
    }
    leads = ic.lead_apply(leads, _meter(Stream.PRODUCTION, 6))  # production Q0 + 7: lead 7 - 9
    assert leads["max"] == {"consumption>production": 4, "production>consumption": -2}
    leads = ic.lead_apply(leads, _meter(Stream.CONSUMPTION, 9))  # Q0 + 10: lead 3, below 4
    assert leads == {
        "newest": {"production": Q0 + 7, "consumption": Q0 + 10},
        "max": {"consumption>production": 4, "production>consumption": -2},
        "skipped": 0,
    }


def test_ac_010_17_a_record_updates_its_lead_over_every_other_stream() -> None:
    leads = ic.new_leads()
    for raw in (
        _meter(Stream.PRODUCTION, 4),  # Q0 + 5
        _meter(Stream.CONSUMPTION, 0),  # Q0 + 1
        GOLDEN["weather_forecast.json"],  # Q0 + 8
    ):
        leads = ic.lead_apply(leads, raw)
    assert leads["max"] == {
        "consumption>production": -4,
        "weather_forecast>production": 3,
        "weather_forecast>consumption": 7,
    }


@pytest.mark.parametrize(
    "raw",
    [b"garbage", NOT_JSON, _contract_breaking(), _unknown_model_forecast(), _end_of_calendar()],
    ids=["garbage", "not-json", "contract-breaking", "unknown-forecast-model", "end-of-calendar"],
)
def test_ac_010_17_record_without_a_slot_only_counts_as_skipped(raw: bytes) -> None:
    leads = ic.lead_apply(ic.new_leads(), _meter(Stream.PRODUCTION, 4))
    assert ic.lead_apply(leads, raw) == {**leads, "skipped": 1}


def test_ac_010_17_lead_apply_returns_a_new_dict_and_leaves_its_input_alone() -> None:
    leads = ic.new_leads()
    for raw in (_meter(Stream.PRODUCTION, 4), _meter(Stream.CONSUMPTION, 0)):
        leads = ic.lead_apply(leads, raw)
    before = copy.deepcopy(leads)
    for raw in (
        _meter(Stream.PRODUCTION, 9),  # newest and a lead rise
        _meter(Stream.CONSUMPTION, 9),
        GOLDEN["weather_forecast.json"],  # a new stream
        NOT_JSON,  # skipped
    ):
        assert ic.lead_apply(leads, raw) is not leads
        assert leads == before


# ── purity (AC-010-12) ────────────────────────────────────────────────────────────────────────


def _imports(tree: ast.AST) -> list[str]:
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            out.append("." * node.level + (node.module or ""))
    return out


def test_ac_010_12_imports_only_the_allowed_standard_library_and_contracts() -> None:
    for name in _imports(ast.parse(SOURCE.read_text())):
        ok = name in ALLOWED or name.startswith("ems.contracts")
        assert ok, f"ingress_check.py imports {name}"


def test_ac_010_12_reads_no_clock() -> None:
    for node in ast.walk(ast.parse(SOURCE.read_text())):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "today", "utcnow", "time"}, ast.unparse(node)
