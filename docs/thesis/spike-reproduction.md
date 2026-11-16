# Reference spike reproduced on the pinned Restate server

Notes for the thesis (spec 010, AC-010-22 … -25). Every number below comes from
`experiments/results/spike-20261116T080544Z/` (`results.json`, `ctl.log`, `manifest.json`), run on commit
`78e6ecd` with a clean tree.

## What was reproduced

The reference spike (`reference/restate-ems-spike/`, tracked byte-identical with the hashes in
`tests/fixtures/spike-files.sha256`) was built in a sandbox with restate-server 1.7.12 from npm and
`restate-sdk` 1.0.5. `make spike` ran its experiments A, B and C unedited:
- **Server:** the image pinned for the project, `restatedev/restate:1.8.1-hotfix.2-20261102` (digest
  `sha256:e3a11e7c…a62e`), as a container of its own (`ems-spike-restate`), separate from the stack's.
- **SDK service:** the spike's own `requirements.txt` in a throwaway virtualenv: `restate-sdk` 1.0.5,
  hypercorn 0.18.0, httpx 0.28.1 (full list in `manifest.json`). The project itself pins `restate-sdk` 1.5.2;
  only the server differs from the spike's original pins.
- **Outside the spike:** only the `ctl.sh` verbs (`infra/spike/ctl.sh`), because the spike's own script started a
  native server from npm. `kill-server` sends SIGKILL to the container; `kill-svc1` sends SIGKILL to every process
  listening on port 9080; `register` rewrites `127.0.0.1` to `host.docker.internal`, which the container reaches.
- **Host:** Apple M4, 10 cores, 16 GiB; Docker Desktop 29.7.2 with a 7.75 GiB VM (the sandbox had 1 vCPU and
  4 GB).

## Results

| Experiment | What it does | Result |
|---|---|---|
| A | 50 households × 96 quarter-hours, 3 strategies and the live loop | 150 of 150 strategy states equal to the offline reference; 0 out of order; the loop's bill equals SelfConsumption's in 50 of 50 households. 4,800 events and 38,400 handler invocations in 64.8 s (74 events/s, 593 invocations/s) |
| B | As A, with `kill -9` of the SDK service after 1,487 events and of the server after 4,223 | Both kills confirmed by `ctl.log` (SDK service pids 70765 and 70771; server exit code 137). 150 of 150 states equal, 96 events in every strategy state (14,400 in all), 0 out of order; the server answered healthy 0.5 s after its restart; 336 ingest retries by the load generator |
| C | A second deployment with a fourth strategy registered mid-stream, no restart | 200 of 200 states equal, 50 of 50 households joined the new strategy; the loop's bill equals SelfConsumption's |

The mean daily cost per household in A (NoBattery 6.958 PLN, SelfConsumption −0.526 PLN, PriceAware 2.271 PLN)
matches the original run, as it should: the spike's synthetic data are deterministic.

## Reading the numbers

- The exactly-once property of the original spike holds on Restate 1.8.1-hotfix.2, including after `kill -9` of
  the server with VQueues on (the risk A5 concerned this restart).
- Throughput is not comparable with the original run. The original `results.json` (tracked as found) was
  rewritten by a later local run on 2026-10-15 (A in 20.5 s), and the spike's README reports the sandbox run
  (A in 82.2 s); neither ran on this machine under Docker Desktop.
- The live loop's staleness (`ems_stale_decisions_pct`, 31 % in A and 75 % in B here) depends on load and
  timing, not on correctness; ADR 0017 removes the stale view by design in the actor stages.
- D, D2 and E (timers, scale) were not part of this reproduction; recovery latency by phase and scale are
  stage 4.
