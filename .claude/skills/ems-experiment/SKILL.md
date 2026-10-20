---
name: ems-experiment
description: Creates, runs or reports an EMS experiment following the protocol in docs/context/11-experiments.md (spec fields, metadata, energy and systems metrics, paired statistics).
argument-hint: "create|run|report <experiment-id>"
disable-model-invocation: true
---

# /ems-experiment

Arguments: `$ARGUMENTS` (mode and experiment id). Read `docs/context/11-experiments.md` first.

## Prerequisites

The experiment harness is backlog spec 010 and the dev environment is spec 001. If
`src/ems/experiments/run.py` does not exist yet, say so and stop: experiments cannot run through
`make exp` before that. Until then the only runnable experiments are the reference spike's (A–E), whose
reproduction is part of spec 001.

## create `<id>`

1. Write `experiments/configs/<id>.yaml` with every spec field: id, hypothesis, config, dataset and
   period, seeds, metrics (energy and systems), acceptance criteria, output location.
2. Check the hypothesis is falsifiable and the acceptance criteria are numeric.
3. Show the config and stop for approval. If the experiment needs new code, it needs a spec first
   (`ems-plan-change`).

## run `<id>`

1. Confirm a clean git tree (the commit is recorded) and that `make up` reports healthy services.
2. Run `make exp NAME=<id>`. Results go to the configured location with config, seed, data window,
   hardware, software versions and git commit.
3. Never edit or delete recorded results; rerun instead.

## report `<id>`

Delegate to the `experiment-analyst` subagent. The report contains: per-household paired comparisons,
distributions (median, IQR, p50/p99), energy metrics (cost and savings vs NoBattery and SelfConsumption,
self-consumption, self-sufficiency, equivalent full cycles, constraint violations, regret vs the oracle),
systems metrics (events/s, invocations/s, decision latency p50/p99, recovery latency by phase,
exactly-once check, view staleness), and caveats (hardware, number of households, seeds).
