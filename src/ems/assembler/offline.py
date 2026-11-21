"""The offline side: a household's recorded streams folded through the same functions the actors
run, then each strategy stepped over the released steps (ADR 0017).

Each stream is read separately, in its own partition order, and folded one stream after another;
the interleaving property tests show that any other merge gives the same steps. The strategy step is
shared with the strategy actors, so both sides run one code path.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ems.assembler.join import Join, Step, on_curves, on_reading, on_run, start
from ems.assembler.mapping import curve, parse, reading, run
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import ForecastStream, PriceStream, forecast_apply, price_apply
from ems.domain.actions import POLICIES
from ems.domain.reference import QuarterIn, RefState, initial_state
from ems.domain.settlement import Flow, MonthSettlement
from ems.domain.stepper import step


@dataclass(frozen=True)
class StrategyResult:
    """A strategy on its virtual battery: state, flows still open to correction, closed months."""

    state: RefState
    recent: tuple[Flow, ...] = ()
    months: tuple[MonthSettlement, ...] = ()


@dataclass(frozen=True)
class Offline:
    join: Join
    steps: tuple[Step, ...]
    strategies: dict[str, StrategyResult]


def start_strategy(params: RunParams) -> StrategyResult:
    return StrategyResult(initial_state(params.model, params.first_step, params.initial_soc_kwh))


def step_strategy(params: RunParams, name: str, r: StrategyResult, s: Step) -> StrategyResult:
    """One released step on the strategy's virtual battery, with the step's corrections."""
    q = QuarterIn(s.t, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh)
    out = step(
        params.model, POLICIES[name], params.rcem, r.state, q, r.recent, s.corrections, params.lag
    )
    months = r.months if out.month is None else (*r.months, out.month)
    return StrategyResult(out.state, out.recent, months)


def fold(
    params: RunParams,
    production: Sequence[bytes],
    consumption: Sequence[bytes],
    forecasts: Sequence[bytes],
    prices: Sequence[bytes],
) -> Offline:
    """Fold the raw records of each stream, in partition order, into steps and strategy results."""
    j = start(params)
    steps: list[Step] = []

    price_stream = PriceStream()
    for raw in prices:
        c = curve(parse(raw))
        if c is None:
            continue  # counted by Tariff
        price_stream, accepted = price_apply(price_stream, c)
        if accepted is not None:
            j, out = on_curves(params, j, price_stream.covered, (accepted,))
            steps += out

    forecast_stream = ForecastStream()
    for raw in forecasts:
        r = run(parse(raw))
        if r is None:
            continue  # counted by Weather
        forecast_stream, accepted_run = forecast_apply(forecast_stream, r)
        if accepted_run is not None:
            j, out = on_run(params, j, forecast_stream.covered, accepted_run)
            steps += out

    for raw in (*production, *consumption):
        j, out = on_reading(params, j, reading(parse(raw)))
        steps += out

    results: dict[str, StrategyResult] = {}
    for name in params.strategies:
        result = start_strategy(params)
        for s in steps:
            result = step_strategy(params, name, result, s)
        results[name] = result
    return Offline(j, tuple(steps), results)
