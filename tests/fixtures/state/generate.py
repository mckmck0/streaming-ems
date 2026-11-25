"""The stored-state fixtures of a release (spec 012, AC-012-41; plan §9, F5).

Writes every stored and sent type (``STORED_AND_SENT`` of the serde tests: the actors' state, the
run parameters, and the messages, which wait in Restate's queues too) as this checkout encodes it
with ``serde_for(type).serialize``, one value per line, to ``<out>/<Type>.jsonl``.
``tests/unit/actors/test_state_compat.py`` decodes them with the current serde.

The values:
- those of the ``values`` fixture of ``tests/unit/actors/test_serde.py``: start states, a half run
  with held messages, a full run, every message sent;
- the same with faults (``FAULT_RUN_ID``) at L = 8 (estimated readings, duplicates) and at L = 2
  (``Late`` readings, corrections, ``too_old``);
- hand-built settlement values, which the 6 h window has none of: a month that offsets against an
  opening tranche, one where a tranche expires with a refund, one that creates a tranche, a
  ``SettleAcc`` with an open hour and the rows of two months; ``Apply`` with an EXPORT and a
  SETPOINT action; a site whose cohort switched to RCE (``rce_from``), and one with PGE's seasonal
  G12 zones (``ZoneRule.season``, which no site of the catalog uses). Each sits in the stored or
  sent type that holds it.

It fails unless every dataclass reachable from ``STORED_AND_SENT`` occurs in some line, and every
optional field holds a value other than ``None`` in some line, both read back from the encoded
lines. Run it once per release, before the types change (stage 3 is 1.0.0):

    uv run python -m tests.fixtures.state.generate

It refuses to write into a directory that already holds fixtures: they record what an older
release stored, and encoding them again with changed types would test nothing.
"""

from __future__ import annotations

import argparse
import dataclasses
import types
import typing
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ems.actors.battery import BatteryState
from ems.actors.messages import Apply
from ems.actors.meter import MeterState
from ems.actors.serde import serde_for
from ems.actors.strategy import StrategyState
from ems.assembler.params import RunParams
from ems.contracts.time import quarter_index
from ems.domain.actions import Action, ActionKind
from ems.domain.calendar import DayType, ZoneRule, ZoneScheme
from ems.domain.netbilling import Cohort, basis_for
from ems.domain.reference import prices
from ems.domain.settlement import (
    MonthSettlement,
    SettleAcc,
    SettleIn,
    Tranche,
    add_quarter,
    hour_rows,
    settle_month,
)
from ems.domain.tariff import fixed_charges
from tests.network import (
    FAULT_RUN_ID,
    HOURS,
    START,
    STARTS,
    STATE_TYPES,
    Generated,
    Network,
    Order,
    generate,
)
from tests.unit.actors.test_serde import STORED_AND_SENT, _set_types, stored_and_sent_values

VERSION = "1.0.0"
OUT = Path(__file__).resolve().parent / VERSION
FAULT_LATE_SLOTS = (8, 2)
HALF = Order(seed=3, weights=(1, 1, 1, 1, 1), pick="newest")
FULL = Order(seed=4, weights=(1, 1, 1, 1, 1), pick="any")
HALF_LIMIT = 450
OCTOBER = "2026-10"
RCE_PLN_PER_KWH = 0.45
# Grid energy per quarter-hour (kWh, > 0 import) from START, 2026-10-31T21:00Z (October to 23:00Z).
IMPORT_KWH = (0.4, 0.3, 0.5, 0.2, 0.6, 0.4, 0.3, 0.5)  # two October hours of import
EXPORT_KWH = (-0.6, -0.5, -0.4, -0.3, 0.2, 0.1, 0.3, 0.2)  # an October export hour, then import
# From 22:00Z: one October hour, one November hour, two quarters of the next (the open hour).
ACC_KWH = (0.3, -0.2, 0.4, 0.1, 0.5, 0.2, -0.1, 0.3, 0.2, 0.4)
# PGE G12 (config/tariffs/2026/pge.yaml, pkt 2.2.6): the afternoon window moves with the season.
SEASONAL_G12 = ZoneScheme(
    "day",
    (
        ZoneRule("night", tuple(DayType), ((15, 17), (22, 6)), season=("04-01", "10-01")),
        ZoneRule("night", tuple(DayType), ((13, 15), (22, 6)), season=("10-01", "04-01")),
    ),
)


# ── values from the network ─────────────────────────────────────────────────────────────────────


def _network_values(gen: Generated) -> dict[type[Any], list[Any]]:
    """As the ``values`` fixture collects them: parameters, start states, a half and a full run's
    stored states, and every message the full run sent."""
    out: dict[type[Any], list[Any]] = {tp: [] for tp in STORED_AND_SENT}
    for params in gen.params.values():
        out[RunParams].append(params)
        for service, start in STARTS.items():
            out[STATE_TYPES[service]].append(start(params)[0])
    half = Network(gen)
    half.run(HALF, limit=HALF_LIMIT)
    full = Network(gen)
    full.run(FULL)
    for net in (half, full):
        for service, key in net.stored:
            out[STATE_TYPES[service]].append(net.state(service, key))
    for s in full.sent:
        out[type(s.message)].append(s.message)
    return out


# ── hand-built settlement values ────────────────────────────────────────────────────────────────


def _settle_in(p: RunParams, t: int, grid_kwh: float) -> SettleIn:
    pr = prices(p.model, p.rcem, t, RCE_PLN_PER_KWH)
    return SettleIn(t, grid_kwh, pr.buy, pr.export_price_pln_per_kwh)


def _quarters(p: RunParams, t0: int, grid_kwh: Sequence[float]) -> list[SettleIn]:
    return [_settle_in(p, t0 + i, g) for i, g in enumerate(grid_kwh)]


def _months(p: RunParams) -> tuple[MonthSettlement, MonthSettlement, MonthSettlement]:
    """October settled three ways: against an opening tranche, with a tranche expiring (refund),
    and with export that creates a tranche."""
    t0 = quarter_index(START)
    imports = hour_rows(_quarters(p, t0, IMPORT_KWH))
    exports = hour_rows(_quarters(p, t0, EXPORT_KWH))
    assert {r.month for r in (*imports, *exports)} == {OCTOBER}
    rules = p.model.rules
    basis = basis_for(p.model.cohort, OCTOBER)
    fixed = fixed_charges(p.model.tariff, OCTOBER, p.model.fixed_inputs)

    usable = Tranche("2026-08", "2026-09", 2.46, 2.46, 0.4, basis)
    offset = settle_month(OCTOBER, imports, fixed, basis, rules, (usable,))
    assert offset.offsets and offset.offset_pln > 0.0, offset

    expiring = Tranche("2025-10", "2025-11", 12.3, 12.3, 2.0, basis)  # last usable in October
    assert expiring.last_month(rules) == OCTOBER
    expiry = settle_month(OCTOBER, imports, fixed, basis, rules, (expiring,))
    assert expiry.expired_pln > 0.0 and expiry.refund_pln > 0.0, expiry

    created = settle_month(OCTOBER, exports, fixed, basis, rules, ())
    assert created.created is not None and created.closing == (created.created,), created
    return offset, expiry, created


def _acc(p: RunParams, tranches: tuple[Tranche, ...]) -> SettleAcc:
    """An account during the lag: the rows of October and November, and an open hour."""
    t0 = quarter_index(START) + 4
    acc = SettleAcc(t0, (), (), tranches)
    for s in _quarters(p, t0, ACC_KWH):
        acc, _ = add_quarter(acc, s)
    assert len(acc.hour) == 2 and acc.tranches, acc
    assert [r.month for r in acc.month_rows] == [OCTOBER, "2026-11"], acc.month_rows
    return acc


def _hand_built(gen: Generated) -> dict[type[Any], list[Any]]:
    key = sorted(gen.params)[0]
    p = gen.params[key]
    full = Network(gen)
    full.run(FULL)

    offset, expiry, created = _months(p)
    months = (offset, expiry, created)
    acc = _acc(p, (*offset.closing, *created.closing))

    strategy = full.state("SelfConsumption", key)
    result = strategy.result
    strategy = dataclasses.replace(
        strategy,
        result=dataclasses.replace(
            result, state=dataclasses.replace(result.state, acc=acc), months=months
        ),
    )
    meter = dataclasses.replace(full.state("Meter", key), acc=acc, months=months)

    sent = next(s.message for s in full.sent if isinstance(s.message, Apply))
    export = dataclasses.replace(sent, action=Action(ActionKind.EXPORT, level=0.5))
    setpoint = dataclasses.replace(sent, action=Action(ActionKind.SETPOINT, p_kw=-1.25))
    battery = full.state("Battery", key)
    after = battery.commands.expected
    assert after is not None
    held = ((after + 1, export), (after + 2, setpoint))
    battery = dataclasses.replace(
        battery, commands=dataclasses.replace(battery.commands, held=held)
    )

    switched = Cohort(first_feed_in=date(2023, 5, 10), rce_from="2025-01")
    rce_from = dataclasses.replace(p, model=dataclasses.replace(p.model, cohort=switched))
    tariff = dataclasses.replace(p.model.tariff, scheme=SEASONAL_G12)
    seasonal = dataclasses.replace(p, model=dataclasses.replace(p.model, tariff=tariff))
    return {
        StrategyState: [strategy],
        MeterState: [meter],
        Apply: [export, setpoint],
        BatteryState: [battery],
        RunParams: [rce_from, seasonal],
    }


# ── coverage and writing ────────────────────────────────────────────────────────────────────────


def _classes(value: Any, out: set[type[Any]], fields: set[tuple[type[Any], str]]) -> None:
    """Every dataclass that occurs in a value, and every field that holds something other than
    ``None``."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        out.add(type(value))
        for f in dataclasses.fields(value):
            item = getattr(value, f.name)
            if item is not None:
                fields.add((type(value), f.name))
            _classes(item, out, fields)
    elif isinstance(value, (tuple, list)):
        for item in value:
            _classes(item, out, fields)
    elif isinstance(value, dict):
        for item in value.values():
            _classes(item, out, fields)


def _optional(tp: type[Any]) -> list[str]:
    """The fields of a dataclass that may hold ``None``."""
    return [
        name
        for name, hint in typing.get_type_hints(tp).items()
        if typing.get_origin(hint) in (types.UnionType, typing.Union)
        and type(None) in typing.get_args(hint)
    ]


def reachable() -> set[type[Any]]:
    """Every dataclass reachable from ``STORED_AND_SENT`` (the type walk of the serde tests)."""
    seen: set[Any] = set()
    for tp in STORED_AND_SENT:
        _set_types(tp, seen, tp.__name__)
    return seen


def collect() -> dict[type[Any], list[bytes]]:
    """Every value, encoded, per type; equal encodings once, in first-seen order."""
    sources: list[dict[type[Any], list[Any]]] = [stored_and_sent_values()]
    for late_slots in FAULT_LATE_SLOTS:
        sources.append(_network_values(generate(START, HOURS, late_slots, True, FAULT_RUN_ID)))
    sources.append(_hand_built(generate(START, HOURS, 8, False)))
    lines: dict[type[Any], dict[bytes, None]] = {tp: {} for tp in STORED_AND_SENT}
    for source in sources:
        for tp, values in source.items():
            serde = serde_for(tp)
            for value in values:
                assert type(value) is tp, (tp, type(value))
                lines[tp][serde.serialize(value)] = None
    return {tp: list(encoded) for tp, encoded in lines.items()}


def check_coverage(lines: Mapping[type[Any], Iterable[bytes]]) -> None:
    """Every reachable dataclass occurs in some line, and every optional field of one holds a
    value in some line, as the lines decode."""
    found: set[type[Any]] = set()
    held: set[tuple[type[Any], str]] = set()
    for tp, encoded in lines.items():
        serde = serde_for(tp)
        for raw in encoded:
            _classes(serde.deserialize(raw), found, held)
    missing = sorted(t.__qualname__ for t in reachable() - found)
    assert not missing, f"no line holds {missing}"
    unset = sorted(
        f"{tp.__qualname__}.{name}"
        for tp in reachable()
        for name in _optional(tp)
        if (tp, name) not in held
    )
    assert not unset, f"these optional fields are None in every line: {unset}"


def write(out: Path) -> dict[str, int]:
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} already holds fixtures: they are written once per release")
    lines = collect()
    check_coverage(lines)
    out.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for tp, encoded in lines.items():
        assert encoded, f"no value of {tp.__name__}"
        (out / f"{tp.__name__}.jsonl").write_bytes(b"".join(raw + b"\n" for raw in encoded))
        counts[tp.__name__] = len(encoded)
    return counts


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Write the stored-state fixtures of a release.")
    parser.add_argument("--out", type=Path, default=OUT, help=f"default: {OUT}")
    args = parser.parse_args(argv)
    for name, count in sorted(write(args.out).items()):
        print(f"{name}: {count} lines")


if __name__ == "__main__":
    main()
