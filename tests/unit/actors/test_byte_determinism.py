"""AC-011-33: two processes with different hash seeds produce identical bytes (risk A11).

Restate compares a retried handler's state writes and sends with its journal; bytes that depend on
the process (a set's order follows ``PYTHONHASHSEED``) would fail the retry with RT0016. ``dump``
runs in two subprocesses with different ``PYTHONHASHSEED`` and prints, as hex:

1. values: the real ``RunParams`` of site PL-LOC-000001 (its ``SiteModel`` holds the holidays, once
   a set) and its ``SiteModel`` alone; every object's state half-way through a run of the in-memory
   network, where messages are held;
2. a trace: the three managed households of a generated 6 h run configured and driven in one fixed
   delivery order, with every transition's written state and encoded sends, in order.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import subprocess
import sys
from typing import Any

import pytest

from ems.actors.serde import serde_for
from ems.assembler.params import RunParams
from ems.domain.reference import SiteModel
from tests.network import (
    HOURS,
    REPO,
    RUN_ID,
    START,
    STATE_TYPES,
    Network,
    Order,
    generate,
    managed_params,
)

SEEDS = ("1", "2")
SITE = f"{RUN_ID}:PL-LOC-000001"
HALF = Order(seed=11, weights=(1, 1, 1, 1, 1), pick="newest")  # 70 messages held at 400
HALF_DELIVERIES = 400
TRACE = Order(seed=12, weights=(1, 1, 1, 1, 1), pick="any")


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def dump() -> dict[str, Any]:
    """The bytes this process produces (called in each subprocess)."""
    params = managed_params(START, HOURS, 8)[SITE]
    values = {
        "RunParams": serde_for(RunParams).serialize(params).hex(),
        "SiteModel": serde_for(SiteModel).serialize(params.model).hex(),
    }
    gen = generate(START, HOURS, 8, False)
    half = Network(gen)
    half.run(HALF, limit=HALF_DELIVERIES)
    for (service, key), raw in sorted(half.stored.items()):
        values[f"{service} {key}"] = raw.hex()

    net = Network(gen)
    net.run(TRACE)
    trace = [
        [
            c.service,
            c.key,
            _digest(c.state),
            [[s.service, s.handler, s.key, _digest(s.raw)] for s in c.sends],
        ]
        for c in net.commits
    ]
    return {"hash": hash("ems: byte determinism"), "values": values, "trace": trace}


def main() -> None:
    print(json.dumps(dump()))


def _run(seed: str) -> dict[str, Any]:
    env = {**os.environ, "PYTHONHASHSEED": seed}
    code = "from tests.unit.actors.test_byte_determinism import main; main()"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    return json.loads(proc.stdout)


@pytest.fixture(scope="module")
def runs() -> list[dict[str, Any]]:
    return [_run(seed) for seed in SEEDS]


def test_ac_011_33_the_processes_hash_differently(runs: list[dict[str, Any]]) -> None:
    assert runs[0]["hash"] != runs[1]["hash"]


def test_ac_011_33_values_encode_to_identical_bytes(runs: list[dict[str, Any]]) -> None:
    one, other = runs[0]["values"], runs[1]["values"]
    assert sorted(one) == sorted(other)
    differ = [name for name in one if one[name] != other[name]]
    assert differ == []


def _held(state: Any) -> int:
    return sum(
        len(getattr(state, f.name).held)
        for f in dataclasses.fields(state)
        if hasattr(getattr(state, f.name), "held")
    )


def test_ac_011_33_the_values_hold_messages_and_holidays(runs: list[dict[str, Any]]) -> None:
    """Not vacuous: some state half-way through holds early messages, and the site model has
    holidays."""
    values = runs[0]["values"]
    held = 0
    for name, raw in values.items():
        service = name.split(" ")[0]
        if service in STATE_TYPES:
            held += _held(serde_for(STATE_TYPES[service]).deserialize(bytes.fromhex(raw)))
    assert held > 0
    model = serde_for(SiteModel).deserialize(bytes.fromhex(values["SiteModel"]))
    assert model is not None and len(model.tariff.national.holidays) > 1


def test_ac_011_33_a_trace_of_transitions_gives_identical_states_and_sends(
    runs: list[dict[str, Any]],
) -> None:
    one, other = runs[0]["trace"], runs[1]["trace"]
    assert len(one) == len(other) > 0
    first = next((i for i, (a, b) in enumerate(zip(one, other, strict=True)) if a != b), None)
    assert first is None, (first, one[first], other[first])
    assert sum(len(sends) for *_, sends in one) > 0
