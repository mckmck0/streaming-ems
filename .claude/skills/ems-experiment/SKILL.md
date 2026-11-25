---
name: ems-experiment
description: Creates, runs or reports an EMS experiment following the protocol in docs/context/11-experiments.md (spec fields, metadata, energy and systems metrics, paired statistics).
argument-hint: "create|run|report <experiment-id>"
disable-model-invocation: true
---

# /ems-experiment

Arguments: `$ARGUMENTS` (mode and experiment id). Read `docs/context/11-experiments.md` first.

## Prerequisites

The runner is built (spec 012): `make exp NAME=<id>` runs `experiments/configs/<id>.yaml` through
`python -m ems.experiments.run`. It needs Docker running and port 9080 free (stop `make dev`); every run wipes
the Kafka and Restate volumes, so it asks once (`CONFIRM=yes` skips the prompt). The runner publishes synthetic
data only; a history recording needs a spec first. The reference spike's experiments stay on `make spike`.

## create `<id>`

1. Write `experiments/configs/<id>.yaml` in the schema of `ems.experiments.config.Experiment` (extra keys are
   refused): `id`, `hypothesis`, `data` (window, catalog size, managed households, faults; the seed comes
   from the generator config and is recorded in the manifest), `run`, and optionally `kills`, `changes`,
   `matrix`, `sweep`, `repetitions`. Put the metrics and numeric acceptance criteria in a header comment and in
   the spec that motivates the experiment; results go to `experiments/results/<id>-<UTC>/`.
2. Check the hypothesis is falsifiable and the acceptance criteria are numeric.
3. Show the config and stop for approval. If the experiment needs new code, it needs a spec first
   (`ems-plan-change`).

## run `<id>`

1. Confirm a clean git tree (the commit is recorded) and that port 9080 is free; the runner starts its own
   stack, SDK service and ingress containers.
2. Run `make exp NAME=<id>`. Each run writes `config.yaml`, `manifest.json` (seed, data window, hardware,
   software versions, git commit, RCEm table), `summary.json` (status, exactly-once check, systems metrics) and
   the Parquet tables to `experiments/results/<id>-<UTC>/<run>/`. A run with `status: failed` names its reason.
3. Never edit or delete recorded results; rerun instead.

## report `<id>`

Delegate to the `experiment-analyst` subagent. The report contains: per-household paired comparisons,
distributions (median, IQR, p50/p99), energy metrics (cost and savings vs NoBattery and SelfConsumption,
self-consumption, self-sufficiency, equivalent full cycles, constraint violations, regret vs the oracle),
systems metrics (events/s, invocations/s, decision latency p50/p99, recovery latency by phase,
exactly-once check, view staleness), and caveats (hardware, number of households, seeds).
