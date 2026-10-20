---
paths:
  - "experiments/**"
  - "src/ems/experiments/**"
---

# Experiments (`experiments/`, `src/ems/experiments/`)

- Each experiment has a spec (id, hypothesis, YAML config, dataset and period, seeds, metrics, acceptance
  criteria, output location, git commit, hardware); the protocol is in `docs/context/11-experiments.md`.
  Use `/ems-experiment` to create, run or report one.
- Every result is written by code, together with its config, seed, data window, hardware, software versions
  and git commit (principle 14). A result without that metadata is not reportable.
- Results are never edited by hand (an `Edit` deny rule covers `experiments/results/`). Fix the code or the
  config and rerun instead; raw results stay out of git.
- Report systems metrics (events/s, decision latency p50/p99, recovery latency by phase, exactly-once
  check, view staleness) next to energy metrics (principle 16), with per-household paired comparisons and
  distributions, not only means.
- Experiments run on simulated time; no wall-clock timers in experiment logic (principle 7).
