"""AC-010-09, AC-010-10, AC-010-12: properties of the pure ingress log and leads (spec 010, §4).

- AC-010-09: within a key, a topic's log entry depends only on that topic's records. Any
  interleaving of the key's topics therefore gives each topic the entry of folding it alone, and
  ``rejected`` adds up, so the probe's log can equal (``==``) the offline fold of the records read
  back per partition. Different offset sequences give different digests.
- AC-010-12: ``regressions`` counts the non-increasing offset steps and ``n`` the records.
- AC-010-10: ``log_apply`` and ``lead_apply`` are total. Arbitrary bytes and metadata, and wire
  examples with one field replaced by any JSON value (dates at the end of the calendar included),
  never make them raise; a value counts as rejected (log) or skipped (leads) exactly when the v1
  parser or the availability rule refuses it.
"""

from __future__ import annotations

import copy
import functools
import hashlib
import itertools
import json
from typing import Any

from hypothesis import example, given
from hypothesis import strategies as st

from ems.actors import ingress_check as ic
from ems.contracts.availability import available_at
from ems.contracts.envelope import parse_event, to_json_bytes
from ems.contracts.examples import golden_examples
from ems.contracts.time import quarter_index

type Meta = tuple[str, int, int]
type Record = tuple[Meta | None, bytes]
type JsonPath = tuple[str | int, ...]

TOPIC = "ems.production.v1"
TOPICS = ("ems.production.v1", "ems.consumption.v1", "ems.weather.forecast.v1", "ems.price.v1")
GOLDEN = {name: to_json_bytes(event) for name, event in sorted(golden_examples().items())}
DOCS: dict[str, Any] = {name: json.loads(raw) for name, raw in GOLDEN.items()}

# Kafka partitions are int32 and offsets int64; the narrow ranges make equal offsets common.
partition_index = st.integers(min_value=0, max_value=63)
first_offset_records = st.integers(min_value=0, max_value=2**62)
offset_gap_records = st.integers(min_value=1, max_value=1_000)
offset_records = st.one_of(
    st.integers(min_value=0, max_value=20), st.integers(min_value=0, max_value=2**63 - 1)
)
offset_sequences = st.lists(offset_records, min_size=1, max_size=40)
record_values = st.one_of(st.sampled_from(list(GOLDEN.values())), st.binary(max_size=256))

# Arbitrary metadata, biased towards repeated topics so that entries get updated, not only opened.
any_topic = st.one_of(st.sampled_from(TOPICS), st.text(max_size=20))
any_partition = st.one_of(
    st.integers(min_value=0, max_value=3), st.integers(min_value=-(2**31), max_value=2**31 - 1)
)
any_offset = st.one_of(
    st.integers(min_value=0, max_value=20), st.integers(min_value=-(2**63), max_value=2**63 - 1)
)
any_meta = st.one_of(st.none(), st.tuples(any_topic, any_partition, any_offset))

EDGE_TEXTS = (
    "9999-12-31",  # a delivery day whose next day overflows in ems.contracts.time
    "0001-01-01",
    "9999-12-31T23:59:59-01:00",  # overflows on the way to UTC
    "0001-01-01T00:00:00+01:00",
    "gfs_seamless",  # a forecast model without an availability delay
    "weather_actual",
    "price",
    "P1D",
)
json_leaves = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**70), max_value=2**70),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=30),
    st.sampled_from(EDGE_TEXTS),
)
json_values = st.recursive(
    json_leaves,
    lambda inner: st.one_of(
        st.lists(inner, max_size=4), st.dictionaries(st.text(max_size=10), inner, max_size=4)
    ),
    max_leaves=12,
)


def _paths(node: Any, prefix: JsonPath = ()) -> list[JsonPath]:
    """Every place in a JSON document whose value can be replaced, the root included."""
    out = [prefix]
    if isinstance(node, dict):
        for key, child in node.items():
            out += _paths(child, (*prefix, key))
    elif isinstance(node, list):
        for index, child in enumerate(node):
            out += _paths(child, (*prefix, index))
    return out


PATHS = {name: _paths(doc) for name, doc in DOCS.items()}


def _replaced(name: str, path: JsonPath, value: Any) -> bytes:
    """The golden example ``name`` with the value at ``path`` replaced, as JSON bytes."""
    if not path:
        return json.dumps(value).encode()
    doc = copy.deepcopy(DOCS[name])
    node = doc
    for step in path[:-1]:
        node = node[step]
    node[path[-1]] = value
    return json.dumps(doc).encode()


@st.composite
def mutated_examples(draw: st.DrawFn) -> bytes:
    name = draw(st.sampled_from(sorted(DOCS)))
    path = draw(st.sampled_from(PATHS[name]))
    return _replaced(name, path, draw(json_values))


@st.composite
def interleaved_topics(draw: st.DrawFn) -> tuple[dict[str, list[Record]], list[Record]]:
    """Two to four topics, each with its own fixed partition and strictly increasing offsets, and
    one interleaving that keeps the order within each topic."""
    names = draw(st.lists(st.sampled_from(TOPICS), min_size=2, max_size=4, unique=True))
    per_topic: dict[str, list[Record]] = {}
    for name in names:
        partition = draw(partition_index)
        gaps = draw(st.lists(offset_gap_records, max_size=10))
        offsets = itertools.accumulate(gaps, initial=draw(first_offset_records))
        per_topic[name] = [((name, partition, offset), draw(record_values)) for offset in offsets]
    order = draw(st.permutations([name for name in names for _ in per_topic[name]]))
    pending = {name: iter(records) for name, records in per_topic.items()}
    return per_topic, [next(pending[name]) for name in order]


def _entry(partition: int, offsets: list[int]) -> dict[str, Any]:
    records: list[Record] = [
        ((TOPIC, partition, offset), GOLDEN["production.json"]) for offset in offsets
    ]
    return ic.fold_log(records)["topics"][TOPIC]


def _chain(partition: int, offsets: list[int]) -> str:
    digest = ""
    for offset in offsets:
        digest = hashlib.sha256(f"{digest}:{partition}:{offset}".encode()).hexdigest()
    return digest


def _parses(raw: bytes) -> bool:
    try:
        parse_event(raw)
    except Exception:
        return False
    return True


def _slot(raw: bytes) -> tuple[str, int] | None:
    """(wire stream, quarter-hour of ``available_at``) of a value, or None when it has none."""
    try:
        event = parse_event(raw)
        return event.stream.value, quarter_index(available_at(event))
    except Exception:
        return None


@given(case=interleaved_topics())
def test_ac_010_09_log_does_not_depend_on_how_a_keys_topics_interleave(
    case: tuple[dict[str, list[Record]], list[Record]],
) -> None:
    per_topic, interleaved = case
    log = ic.fold_log(interleaved)
    alone = {name: ic.fold_log(records) for name, records in per_topic.items()}
    assert set(log["topics"]) == set(per_topic)
    for name in per_topic:
        assert log["topics"][name] == alone[name]["topics"][name]
    assert log["rejected"] == sum(each["rejected"] for each in alone.values())


@given(partition=partition_index, offsets=offset_sequences)
def test_ac_010_12_regressions_count_the_non_increasing_offset_steps(
    partition: int, offsets: list[int]
) -> None:
    entry = _entry(partition, offsets)
    assert entry["regressions"] == sum(1 for a, b in itertools.pairwise(offsets) if b <= a)
    assert entry["n"] == len(offsets)
    assert entry["last_offset"] == offsets[-1]
    assert entry["partition"] == partition
    assert entry["partition_changes"] == 0
    assert entry["digest"] == _chain(partition, offsets)


@given(
    partition=partition_index,
    pair=st.tuples(offset_sequences, offset_sequences).filter(lambda pair: pair[0] != pair[1]),
)
@example(partition=0, pair=([1, 23], [12, 3]))  # the same digits, split differently
@example(partition=0, pair=([5], [5, 6]))  # a prefix
@example(partition=0, pair=([5, 6], [6, 5]))  # the same offsets, reordered
def test_ac_010_09_different_offset_sequences_give_different_digests(
    partition: int, pair: tuple[list[int], list[int]]
) -> None:
    first, second = pair
    assert _entry(partition, first)["digest"] != _entry(partition, second)["digest"]


@given(records=st.lists(st.tuples(any_meta, st.one_of(record_values, st.binary())), max_size=12))
def test_ac_010_10_log_apply_and_lead_apply_never_raise_on_arbitrary_records(
    records: list[Record],
) -> None:
    log = ic.fold_log(records)
    leads = functools.reduce(ic.lead_apply, [raw for _, raw in records], ic.new_leads())
    assert json.loads(json.dumps(log)) == log  # plain JSON types, as object state
    assert json.loads(json.dumps(leads)) == leads
    with_meta = sum(meta is not None for meta, _ in records)
    assert sum(entry["n"] for entry in log["topics"].values()) == with_meta
    assert leads["skipped"] == sum(_slot(raw) is None for _, raw in records)


@given(raw=mutated_examples())
@example(raw=_replaced("price.json", ("payload", "delivery_day"), "9999-12-31"))
@example(raw=_replaced("price.json", ("payload", "delivery_day"), "0001-01-01"))
@example(raw=_replaced("production.json", ("interval_start",), "0001-01-01T00:00:00+01:00"))
@example(raw=_replaced("weather_forecast.json", ("payload", "forecast_model"), "gfs_seamless"))
@example(raw=_replaced("consumption.json", (), None))
@example(raw=_replaced("production.json", ("seed",), 7))  # still a valid event
def test_ac_010_10_examples_with_one_field_replaced_never_make_them_raise(raw: bytes) -> None:
    log = ic.log_apply(ic.new_log(), (TOPIC, 2, 7), raw)
    leads = ic.lead_apply(ic.new_leads(), raw)
    assert log == {
        "topics": {
            TOPIC: {
                "partition": 2,
                "n": 1,
                "last_offset": 7,
                "digest": _chain(2, [7]),
                "regressions": 0,
                "partition_changes": 0,
            }
        },
        "rejected": 0 if _parses(raw) else 1,
    }
    slot = _slot(raw)
    if slot is None:
        assert leads == {"newest": {}, "max": {}, "skipped": 1}
    else:
        stream, quarter = slot
        assert leads == {"newest": {stream: quarter}, "max": {}, "skipped": 0}
