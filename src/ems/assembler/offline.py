"""The offline side: a household's recorded streams folded through the same functions the actors
run, then each strategy stepped over the released steps (ADR 0017).

Each stream is read separately, in its own partition order, and folded one stream after another;
the interleaving property tests show that any other merge gives the same steps. The strategy step is
shared with the strategy actors, so both sides run one code path.

With a strategy schedule (spec 012, ADR 0021) each strategy is stepped only over the released steps
of its window, an added one on a fresh virtual battery from its first step.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ems.assembler.join import Join, Step, on_curves, on_reading, on_run, start
from ems.assembler.mapping import curve, parse, reading, run
from ems.assembler.params import RunParams
from ems.assembler.schedule import Change, Schedule, strategies_at, strategy_params, windows
from ems.assembler.sparse_streams import ForecastStream, PriceStream, forecast_apply, price_apply
from ems.domain.actions import POLICIES
from ems.domain.reference import QuarterIn, RefState, initial_state
from ems.domain.settlement import Flow, MonthSettlement
from ems.domain.stepper import StepOut, step

NO_CHANGES = Schedule()  # a run whose strategies never change


@dataclass(frozen=True)
class StrategyResult:
    """A strategy on its virtual battery: state, flows still open to correction, closed months."""

    state: RefState
    recent: tuple[Flow, ...] = ()
    months: tuple[MonthSettlement, ...] = ()
    # corrections for slots before the strategy's first step (an added strategy has no flow there)
    dropped_corrections: int = 0


@dataclass(frozen=True)
class Offline:
    join: Join
    steps: tuple[Step, ...]
    strategies: dict[str, StrategyResult]


def start_strategy(params: RunParams) -> StrategyResult:
    return StrategyResult(initial_state(params.model, params.first_step, params.initial_soc_kwh))


def step_strategy(params: RunParams, name: str, r: StrategyResult, s: Step) -> StrategyResult:
    """One released step on the strategy's virtual battery, with the step's corrections."""
    return step_strategy_out(params, name, r, s)[0]


def step_strategy_out(
    params: RunParams, name: str, r: StrategyResult, s: Step
) -> tuple[StrategyResult, StepOut]:
    """``step_strategy`` with the stepper's output of the step (for the steps table)."""
    kept = tuple(c for c in s.corrections if c.t >= params.first_step)
    q = QuarterIn(s.t, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh)
    out = step(params.model, POLICIES[name], params.rcem, r.state, q, r.recent, kept, params.lag)
    months = r.months if out.month is None else (*r.months, out.month)
    dropped = r.dropped_corrections + len(s.corrections) - len(kept)
    return StrategyResult(out.state, out.recent, months, dropped), out


def strategy_runs(
    params: RunParams, schedule: Schedule
) -> dict[str, tuple[RunParams, int, int | None]]:
    """Each strategy's parameters and window: the run's own for the initial ones,
    ``strategy_params`` for one added by a change.
    """
    out: dict[str, tuple[RunParams, int, int | None]] = {}
    for name, (begin, stop) in windows(params, schedule).items():
        if begin == params.first_step and name in params.strategies:
            out[name] = (params, begin, stop)
        else:
            change = Change(begin, strategies_at(params, schedule, begin))
            out[name] = (strategy_params(params, change), begin, stop)
    return out


def trace(
    params: RunParams, steps: Sequence[Step], schedule: Schedule = NO_CHANGES
) -> dict[str, tuple[StepOut, ...]]:
    """The stepper's output of every step each strategy ran, in order."""
    out: dict[str, tuple[StepOut, ...]] = {}
    for name, (p, begin, stop) in strategy_runs(params, schedule).items():
        result = start_strategy(p)
        outs: list[StepOut] = []
        for s in steps:
            if s.t < begin or (stop is not None and s.t >= stop):
                continue
            result, o = step_strategy_out(p, name, result, s)
            outs.append(o)
        out[name] = tuple(outs)
    return out


def fold(
    params: RunParams,
    production: Sequence[bytes],
    consumption: Sequence[bytes],
    forecasts: Sequence[bytes],
    prices: Sequence[bytes],
    schedule: Schedule = NO_CHANGES,
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
    for name, (p, begin, stop) in strategy_runs(params, schedule).items():
        result = start_strategy(p)
        for s in steps:
            if begin <= s.t and (stop is None or s.t < stop):
                result = step_strategy(p, name, result, s)
        results[name] = result
    return Offline(j, tuple(steps), results)
