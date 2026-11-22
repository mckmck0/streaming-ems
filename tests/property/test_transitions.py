"""AC-011-26 (and AC-011-21's "no transition raises"): the Kafka-fed transitions never raise.

``household.on_record``, ``weather.on_record`` and ``tariff.on_record`` take the event a record
parses to (``mapping.parse``; None for anything that is not a v1 event). Over arbitrary bytes and
the golden examples with one field replaced by any JSON value (dates at the end of the calendar, an
unknown forecast model, another market included), applied to a start state and to a state that
already holds the golden record of the same stream, they never raise; a record they cannot use is
counted and sends nothing; and the state they return can be stored (it round-trips through its
serde equal, so no non-finite value entered it).

The mutation strategy follows ``tests/property/test_ingress_check.py`` (copied, not imported, so the
two files evolve on their own).
"""

from __future__ import annotations

import copy
import dataclasses
import json
from datetime import UTC, datetime
from typing import Any

from hypothesis import example, given
from hypothesis import strategies as st

from ems.actors import household, tariff, weather
from ems.actors.household import HouseholdState
from ems.actors.messages import PriceUpdate, Subscribe
from ems.actors.serde import serde_for
from ems.actors.tariff import TariffState
from ems.actors.weather import WeatherState
from ems.assembler.join import Step
from ems.assembler.mapping import curve, parse, reading, run
from ems.contracts.envelope import to_json_bytes
from ems.contracts.examples import golden_examples
from tests.network import managed_params

type JsonPath = tuple[str | int, ...]

GOLDEN = {name: to_json_bytes(event) for name, event in sorted(golden_examples().items())}
DOCS: dict[str, Any] = {name: json.loads(raw) for name, raw in GOLDEN.items()}
# The golden meter records are for PL-LOC-000001 at 2026-10-20T00:00Z, without a run id.
PARAMS = managed_params(datetime(2026, 10, 20, tzinfo=UTC), 24, 2, None)["PL-LOC-000001"]

EDGE_TEXTS = (
    "9999-12-31",  # a delivery day whose next day overflows
    "0001-01-01",
    "9999-12-31T23:59:59-01:00",  # overflows on the way to UTC
    "0001-01-01T00:00:00+01:00",
    "2026-10-20T00:15:00Z",  # another slot of the window
    "gfs_seamless",  # a forecast model without an availability delay or cadence
    "PL_SDAC",  # a market without a publication window
    "EUR",
    "EUR/MWh",
    "PT1H",
    "missing",
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


records = st.one_of(
    st.binary(max_size=512), mutated_examples(), st.sampled_from(sorted(GOLDEN.values()))
)


def _stored[S](tp: type[S], state: S) -> None:
    serde = serde_for(tp)
    assert serde.deserialize(serde.serialize(state)) == state


def _household_states() -> list[HouseholdState]:
    start, _ = household.start(PARAMS)
    snapshot = PriceUpdate(0, 0, True, ())
    with_prices, _ = household.on_prices(PARAMS, start, snapshot)
    filled = with_prices
    for name in ("production.json", "consumption.json"):
        filled, _ = household.on_record(PARAMS, filled, parse(GOLDEN[name]))
    assert filled.join.consumption.stream.newest is not None  # the golden readings were taken
    return [start, filled]


def _weather_states() -> list[WeatherState]:
    start, _ = weather.start(PARAMS)
    filled, sends = weather.on_record(PARAMS, start, parse(GOLDEN["weather_forecast.json"]))
    assert len(sends) == 1
    return [start, filled]


def _tariff_states() -> list[TariffState]:
    subscribed, _ = tariff.subscribe(
        TariffState(), Subscribe(PARAMS.first_step, PARAMS.household_key)
    )
    filled, sends = tariff.on_record(subscribed, parse(GOLDEN["price.json"]))
    assert len(sends) == 1
    return [TariffState(), subscribed, filled]


HOUSEHOLD_STATES = _household_states()
WEATHER_STATES = _weather_states()
TARIFF_STATES = _tariff_states()


@given(raw=records)
@example(raw=_replaced("production.json", ("interval_start",), "0001-01-01T00:00:00+01:00"))
@example(raw=_replaced("consumption.json", ("t",), 2**62))
@example(raw=_replaced("consumption.json", ("resolution",), "PT1H"))
@example(raw=_replaced("consumption.json", (), None))
def test_ac_011_26_household_on_record_never_raises(raw: bytes) -> None:
    event = parse(raw)
    for state in HOUSEHOLD_STATES:
        new, sends = household.on_record(PARAMS, state, event)
        assert all(s.handler == "on_step" and isinstance(s.message, Step) for s in sends)
        if reading(event) is None:
            assert sends == ()
            assert new.join == dataclasses.replace(state.join, unusable=state.join.unusable + 1)
        _stored(HouseholdState, new)


@given(raw=records)
@example(raw=_replaced("weather_forecast.json", ("payload", "forecast_model"), "gfs_seamless"))
@example(raw=_replaced("weather_forecast.json", ("issued_at",), "9999-12-31T23:59:59-01:00"))
@example(raw=_replaced("weather_forecast.json", (), None))
def test_ac_011_26_weather_on_record_never_raises(raw: bytes) -> None:
    event = parse(raw)
    for state in WEATHER_STATES:
        new, sends = weather.on_record(PARAMS, state, event)
        assert len(sends) <= 1
        if run(event) is None:
            assert sends == ()
            assert new == dataclasses.replace(state, unusable=state.unusable + 1)
        _stored(WeatherState, new)


@given(raw=records)
@example(raw=_replaced("price.json", ("payload", "delivery_day"), "9999-12-31"))
@example(raw=_replaced("price.json", ("payload", "delivery_day"), "0001-01-01"))
@example(raw=_replaced("price.json", ("payload", "market"), "PL_SDAC"))
@example(raw=_replaced("price.json", ("payload", "slots", 0, "price"), 1e300))
@example(raw=_replaced("price.json", (), None))
def test_ac_011_26_tariff_on_record_never_raises(raw: bytes) -> None:
    event = parse(raw)
    for state in TARIFF_STATES:
        new, sends = tariff.on_record(state, event)
        assert len(sends) <= len(state.subscribers)
        if curve(event) is None:
            assert sends == ()
            assert new == dataclasses.replace(state, unusable=state.unusable + 1)
        _stored(TariffState, new)
