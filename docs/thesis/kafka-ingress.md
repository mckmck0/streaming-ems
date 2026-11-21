# Kafka into Restate: order, exactly-once delivery, skew and costs

Notes for the thesis (spec 010, AC-010-17 … -21). The measurements come from three result files, each made by
`make measure` on commit `78e6ecd` with a clean tree on an Apple M4 (10 cores, 16 GiB) under Docker Desktop
29.7.2 (7.75 GiB VM). Every file stores its config, seed, window, images, package versions, hardware and commit:

- skew: `experiments/results/ingress-skew-20261116T081120Z/results.json`;
- state cost: `experiments/results/ingress-state-cost-20261116T081221Z/results.json` (its objects and mode were
  removed in spec 011; reproducing it needs the spec 010 commit `78e6ecd`);
- backlog: `experiments/results/ingress-backlog-20261116T081316Z/results.json`.

The stack: Restate 1.8.1-hotfix.2 (one node), the Kafka ingress integration 0.1.1 (three containers, linux/amd64
emulated on Apple Silicon), Kafka 4.3.1 (one broker), one SDK process (`restate-sdk` 1.5.2 on hypercorn 0.18.0).

## Order and exactly-once delivery (tests)

The order and exactly-once claims rest on tests, not on these files. Each test compares, with `==`, the probe's
log for every record key and topic (count, last offset, a SHA-256 chain over the offsets, regressions, rejected
records) with the same pure function folded over the records read back from Kafka:
- `tests/e2e/test_probe_backfill.py` (`test_ingress_backfill.py` since spec 011): 10 households over one day;
- `tests/fault/test_probe_kills.py` (`test_ingress_kills.py` since spec 011): 120 households over two days, killed with `kill -9` at 30 % of the run (the
  Restate server, the SDK service, an ingress container), each kill proved mid-run from data that survives it;
  and a Restate outage longer than the ingress containers' reconnect budget;
- `tests/integration/`: bad records, records published before ingestion opened, each topic to its handler.

All passed on 2026-11-15/16: each key's records arrived in partition order, none lost or doubled.

## Skew between a household's streams (AC-010-17, -18)

One unpaced backfill: 100 households over two days from 2026-10-20 (seed 4242), 34,627 records, 101 keys (100
households, 72 of them with PV, and the price market). The lead of stream a over stream b is the newest
`available_at` handled from a minus the newest handled from b, in quarter-hours of data time, at the moment a
record of a is handled; each key keeps the largest. Across keys:

| Lead | Keys | Median | 95th percentile | Max |
|---|---|---|---|---|
| weather forecast over production | 72 | 160 | 166 | 167 |
| weather forecast over consumption | 100 | 127 | 127 | 127 |
| consumption over production | 72 | 127 | 130 | 130 |
| production over consumption | 72 | 6 | 14 | 15 |
| consumption over weather forecast | 100 | 4 | 4 | 4 |
| production over weather forecast | 72 | 4 | 4 | 4 |

Unpaced, one stream of a household ran up to 167 quarter-hours (about 42 hours of data) ahead of another in a
192-quarter-hour window: the three ingress containers and their consumers work through the topics at their own
speed. Under ADR 0017 a household would hold that much of the leading streams in state until the lagging
watermark catches up (risk A1). Paced publication bounds it (stage 4). Prices arrive under the market key, so
their skew is measured in stage 3 at `Household`.

The same run handled 34,627 records in 85.6 s between the first invocation's creation and the last one's
completion (404 records per second; 89.5 s of wall time from the start of publishing, which took 17.5 s), with
every key's log equal to the offline fold, 0 regressions and 0 rejected records.

Peak memory (`docker stats` every 2 s; no container was OOM-killed):

| Container | Peak | Limit |
|---|---|---|
| ingress-forecasts | 558 MiB | 768 MiB |
| ingress-meters | 475 MiB | 768 MiB |
| ingress-prices | 288 MiB | 768 MiB |
| kafka | 797 MiB | 1 GiB (the figure includes page cache) |
| restate | 1,556 MiB | 2 GiB |
| kafka-ui | 405 MiB | none |

## Cost of object state (AC-010-20, V7)

500 one-way calls that read and write one small key, on one object whose state is padded to 1 kB, 100 kB or
1 MB; invocation time is `completed_at − running_at` from Restate's `sys_invocation` (millisecond resolution):

| State | Eager: median / p99 / max (ms) | Eager calls/s | Lazy: median / p99 / max (ms) | Lazy calls/s |
|---|---|---|---|---|
| 1 kB | 2 / 9 / 22 | 299 | 3 / 8 / 15 | 239 |
| 100 kB | 3 / 9 / 15 | 235 | 4 / 9 / 34 | 217 |
| 1 MB | 9 / 20 / 563 | 84 | 3 / 6 / 39,399 | 12 |

With eager state (the default) the cost grows with the state: at 1 MB the median call takes 4.5 times as long
and throughput falls to about a quarter. Lazy state (`state_preload=False`) keeps the median flat at 3–4 ms but
costs about 1 ms at small sizes, and at 1 MB one call in 500 took 39 s, which also drags its throughput down.
An earlier run on a tree with uncommitted doc edits (`ingress-state-cost-20261116T080400Z`) showed the same
pattern (a mean of 62 ms against a median of 4 ms at 1 MB). The cause is not known; stage 4 checks it before
any actor relies on lazy state. For the actors' state budget (A1) the eager numbers are the ones that apply.

## Where a backlog lives (AC-010-21, V8)

20 households over one day (3,426 records) were published while the SDK service was stopped:
- the consumer lag of all three ingress groups fell to 0 within 7.4 s: the integration hands every record to
  Restate, which acknowledges it durably, and only then commits the Kafka offset;
- Restate then held 3,405 invocations `pending` and 21 `ready` (one at the head of each key's queue: 20
  households and the price market), unchanged for 30 s;
- after the service came back at 36.7 s, the backlog drained in 8.8 s.

So ingestion does not slow down when the handlers stop: a backlog moves into Restate as queued invocations per
key (risk A6), and Kafka's lag says nothing about how far the actors are behind.

## Behaviour of the ingress integration worth knowing

- While Restate is down, the integration reconnects every 500 ms and never exits, even with a budget of two
  attempts (`RESTATE_RETRY_MAX_ATTEMPTS=2`): each gRPC stream opens before it fails, and a successful open resets
  the back-off. The documented exit after the budget did not happen in a 30 s outage in the test or in a ten-minute
  one before it. Its restart policy covers a process that exits for other reasons.
- Registering the same URI again with `force: false` answers 200 and keeps the old handlers, even when handlers
  were added or removed; only `force: true` discovers the service again (`02-subsystem-actors.md`).

## With the actors (spec 011)

`make measure MODE=skew` on commit `92d0f0c` with a clean tree, same machine and stack
(`experiments/results/ingress-skew-20261121T005630Z/results.json`): the same backfill of 100 households over two
days from 2026-10-20 (34,627 records), with the 35 EMS-managed households configured before publishing, so every
record also drives the actors.

- Every key's ingress log equals the offline fold, with 0 rejected records and 0 regressions.
- 257.8 records per second between the first invocation's creation and the last one's completion (134.3 s),
  against 404 with the probe: each released step now also runs two strategies, `Ems`, `Battery` and `Meter`.
- Leads per managed household, now with prices (median / 95th percentile / max, quarter-hours): prices over
  consumption 122 / 122 / 122, over production 122 / 122 / 122, over forecasts 97 / 109 / 109; forecasts over
  production 169 / 169 / 169 and over consumption 123 / 124 / 124; consumption over prices 51 / 51 / 51.
- Re-ordering between actors (V5): every receiving service counted 0 messages that arrived early and 0 stale.
  Restate documents no order across invocations, so the hold-back stays; the in-memory network test covers it.
- Peak memory: Restate 1,833 MiB of its 2 GiB limit (1,556 with the probe), Kafka 838 MiB, ingress containers
  284–565 MiB; no container was OOM-killed.
