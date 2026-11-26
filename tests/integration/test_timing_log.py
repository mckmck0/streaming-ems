"""AC-012-19 (T20): the timing log of a real SDK service, written through its queue and complete
after a SIGTERM, with and without coverage measuring the service.

Each case starts its own SDK service with ``EMS_TIMING_LOG`` naming a fresh file and sends, through
the HTTP ingress:
- ``N`` price records to ``Tariff.on_price``, one per market key ``<run_id>:M<i>`` (a market-keyed
  object is never configured, and every one of its invocations is marked);
- one production record to ``Household.on_measurement`` and one forecast record to
  ``Weather.on_forecast`` for a key that was never configured (an unmanaged site: its ingress log
  and counter change, nothing is marked).

The records are those `ems-gen` emits for the 10-household catalog (``tests.network.generate``), so
the transitions would run if the objects were configured. Once every record is counted in state
(``n`` is written after the transition, and the line is queued before the handler returns), the
service gets SIGTERM (``SdkService.stop``), and the file then holds exactly ``N`` lines, one per
``Tariff`` invocation, with service, handler and key.

"With coverage" runs the service as the integration tier does (coverage's subprocess patch, with
``sigterm = true`` from ``pyproject.toml``: its SIGTERM handler re-raises with the default action,
which skips ``atexit``), here with its data written to the test's own directory so the run shows
the measurement was on. "Without coverage" blanks both of coverage's startup variables, as the
experiment runner starts the service.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import coverage
import pytest

from ems.experiments import stack
from tests.network import CONSUMPTION, FORECAST, PRICE, PRODUCTION, START, generate
from tests.stack_fixtures import LOGS, REPO, new_prefix

N = 12  # Tariff invocations
UNMANAGED = "PL-LOC-000001"  # a managed site in the catalog, never configured here
TIMEOUT_S = 120.0
NO_COVERAGE = {"COVERAGE_PROCESS_START": "", "COVERAGE_PROCESS_CONFIG": ""}


def _coverage_env(case: str, data_dir: Path) -> dict[str, str]:
    """The service's coverage variables. With coverage: ``COVERAGE_PROCESS_CONFIG`` holds the
    serialized configuration of ``pyproject.toml``, as coverage's ``patch = subprocess`` sets it for
    the tier, with the data file in ``data_dir``. Without: both startup variables blank (the
    ``.pth`` hook tests them for truth, so a blank value starts nothing)."""
    if case == "plain":
        return dict(NO_COVERAGE)
    config = coverage.Coverage(
        data_file=str(data_dir / ".coverage"), config_file=str(REPO / "pyproject.toml")
    ).config
    assert config.sigterm and "subprocess" in config.patch, "not the tier's coverage settings"
    return {"COVERAGE_PROCESS_CONFIG": config.serialize(), "COVERAGE_PROCESS_START": ""}


@pytest.fixture
def env_factory(bootstrap: str) -> Iterator[Any]:
    opened: list[stack.StackEnv] = []

    def make(sdk_env: dict[str, str]) -> stack.StackEnv:
        env = stack.StackEnv(new_prefix(), log_dir=LOGS, sdk_env=sdk_env)
        opened.append(env)
        return env.open(start_ingress=False)

    yield make
    for env in opened:
        env.close()


def _records(run_id: str) -> tuple[list[bytes], bytes, bytes]:
    gen = generate(START, 1, 8, False, run_id)
    key = f"{run_id}:{UNMANAGED}"
    prices = list(gen.of(PRICE, gen.market_key))
    assert prices, "no price record in the generated hour"
    meters = gen.of(PRODUCTION, key) or gen.of(CONSUMPTION, key)
    forecasts = gen.of(FORECAST, key)
    assert meters and forecasts, key
    return prices, meters[0], forecasts[0]


def _lines(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


@pytest.mark.parametrize("case", ["coverage", "plain"])
def test_ac_012_19_one_line_per_marked_invocation_after_sigterm(
    case: str, env_factory: Any, run_id: str, tmp_path: Path
) -> None:
    log = tmp_path / "timings.jsonl"
    data_dir = tmp_path / "coverage"
    data_dir.mkdir()
    env = env_factory({"EMS_TIMING_LOG": str(log), **_coverage_env(case, data_dir)})
    prices, meter, forecast = _records(run_id)
    unmanaged = f"{run_id}:{UNMANAGED}"

    env.restate.send("Household", unmanaged, "on_measurement", meter)
    env.restate.send("Weather", unmanaged, "on_forecast", forecast)
    markets = [f"{run_id}:M{i}" for i in range(N)]
    for i, market in enumerate(markets):
        env.restate.send("Tariff", market, "on_price", prices[i % len(prices)])

    env.restate.wait_handled(run_id, N, timeout=TIMEOUT_S, services=("Tariff",))
    env.restate.wait_handled(run_id, 2, timeout=TIMEOUT_S, services=("Household", "Weather"))
    # The unmanaged site's records reached the objects and changed only the log and counter.
    for service in ("Household", "Weather"):
        held = env.restate.states(run_id, service)[unmanaged]
        assert set(held) == {"log", "n"}, (service, sorted(held))
    env.sdk.stop()  # SIGTERM to the process group; returns once nothing listens on the port

    lines = _lines(log)
    assert len(lines) == N, lines
    assert sorted((x["svc"], x["h"], x["key"]) for x in lines) == sorted(
        ("Tariff", "on_price", m) for m in markets
    )
    for x in lines:
        assert set(x) == {"ns", "svc", "h", "key", "inv", "in_t", "sent"}, x
        assert isinstance(x["ns"], int) and x["ns"] > 0, x
        assert x["in_t"] is None and x["sent"] == [], x  # a record with no subscriber sends nothing
    invocations = [x["inv"] for x in lines]
    assert len(set(invocations)) == N and all(i.startswith("inv_") for i in invocations)
    if case == "coverage":
        # Coverage measured the service and saved its data at SIGTERM, as in the tier.
        assert list(data_dir.glob(".coverage*")), sorted(p.name for p in data_dir.iterdir())
