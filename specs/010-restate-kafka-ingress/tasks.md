# Tasks: Restate environment and Kafka into a Virtual Object

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md). Spec and plan approved 2026-11-15; the user asked on the same
day to implement through to the pull request without a separate stop for this list.
- Tests come first (tasks marked [test]); each implementation task turns its tests green.
- Section numbers (§) refer to the plan's Design.
- Implementation details the plan left open, decided here:
  - the pure check returns new dictionaries and never mutates its input; a regression leaves `last_offset` at the
    offset just seen, so `regressions` counts non-increasing steps;
  - stream names in the leads are the wire `stream` values (`production`, `consumption`, `weather_forecast`,
    `price`); a slot is `quarter_index(available_at(event))`;
  - a port counts as in use when binding `127.0.0.1:<port>` fails (the unit tier blocks connects);
  - percentiles in result files use the nearest-rank method;
  - the spike's `ctl.log` has one line per verb: `<UTC> <verb> <key=value …>`; a kill of the SDK service logs
    `pids=`, a kill of the server logs `exit=` with the container's exit code.

## Phase 1: Version policy and dependencies

- [ ] T01 [docs] (AC-010-02, P12, P15) Write the version policy (exact pins, newest release when pinned, no
  minimum age) into `CLAUDE.md`, `docs/context/07-design-principles.md` (P12), `05-deploy-and-infra.md`,
  `docs/context/README.md`, `.claude/skills/ems-plan-change/checklists.md`, the `ems-deploy-checklist` skill,
  and the comments in `.pre-commit-config.yaml` and `infra/ems-gen.Dockerfile`; a decision line in 00.
- [ ] T02 (AC-010-02) `pyproject.toml`: remove `exclude-newer`; group `actors` (`restate-sdk==1.5.2`,
  `hypercorn==0.18.0`) in the default groups; `pytest-cov` and `coverage` in `dev`; `[tool.coverage.*]` and
  `[tool.ty.src] exclude = ["reference"]` (§8, §9). `uv lock` adds only the new packages. Re-read in the installed
  SDK: input deserialisation errors, `BytesSerde`, request headers, lazy state (`state_preload=False`; the SDK
  marks `enable_lazy_state` deprecated), `ctx.time()`.

## Phase 2: Failing tests without infrastructure

- [ ] T03 [test] (AC-010-02, -06, -08, -27, -28, -30, -31, P11) Static checks in `tests/unit/infra/`: compose pins
  by digest, Restate ports on 127.0.0.1, the three ingress services (topics, groups, handlers, `earliest`,
  profile, label, restart policy, no ports); `uv.lock` pins and no `exclude-newer`; settings defaults equal the
  Makefile defaults; `down` and `reset` carry the profile and the label sweep; every tier target writes its
  coverage file under `logs/`; the workflow (actions by SHA, permissions, runner, triggers, `make ci`); README
  badges present and Restate and Kafka versions equal to the compose tags.
- [ ] T04 [test] (AC-010-09, -10, -12, -17) `tests/unit/actors/test_ingress_check.py` (log, regression, partition
  change, rejected, leads, skipped, import allow-list, no clock) and `tests/property/test_ingress_check.py`
  (interleavings, regression count, distinct digests, totality on arbitrary bytes and mutated events).
- [ ] T05 [test] (AC-010-18, -22, -24, -25, -26) Spike file list and hashes
  (`tests/fixtures/spike-files.sha256`); `jobs/spike_repro.py` port check and verdict; `jobs/ingress_measure.py`
  lead summary; `SdkService` refuses a busy port.

## Phase 3: Code

- [ ] T06 (P11) `src/ems/settings.py`: `restate_ingress_url`, `restate_admin_url`, `service_url`.
- [ ] T07 (AC-010-10, -12, -17) `src/ems/actors/ingress_check.py` (§4).
- [ ] T08 (AC-010-08, -10, -20) `src/ems/actors/probe.py`, `statecost.py`, `app.py` (§5, Actor contract).
- [ ] T09 (AC-010-26) `src/ems/experiments/stack.py`, the harness (§6).

## Phase 4: Failing tests against the stack

- [ ] T10 [test] (AC-010-01, -03 … -08, -10, -11, -26) Fixtures in `tests/stack_fixtures.py`, the session check of
  port 9080 in each Restate tier's `conftest.py`; `tests/integration/test_stack_lifecycle.py`,
  `test_probe_delivery.py`, `test_probe_bad_records.py`, `test_probe_prepublish.py`.
- [ ] T11 [test] (AC-010-09) `tests/e2e/test_probe_backfill.py`.
- [ ] T12 [test] (AC-010-13 … -16) `tests/fault/test_probe_kills.py`, with the trigger and proof of each kill.

## Phase 5: Environment, jobs, CI

- [ ] T13 (AC-010-01, -02, -06, -08) `infra/docker-compose.yml` (§2) and `tests/compose.it.yml`.
- [ ] T14 (AC-010-03 … -07, -27) `Makefile` (§3) and `/logs/` in `.gitignore`.
- [ ] T15 (AC-010-22) Track the 16 spike files under `reference/restate-ems-spike/`, write the hash fixture,
  move the root copy to `local/restate-ems-spike/` (user decision 2026-11-15) and drop its `.gitignore` line (§9).
- [ ] T16 (AC-010-17 … -21, -23 … -25) `jobs/ingress_measure.py`, `jobs/spike_repro.py`, `infra/spike/ctl.sh` (§7).
- [ ] T17 (AC-010-28 … -31) `.github/workflows/ci.yml` and the README badge block (§8); check `uv.lock` with
  uv 0.13.8.

## Phase 6: Run

- [ ] T18 (SC-003) `make format`, `make ci` green.
- [ ] T19 (AC-010-01 … -16, SC-003) `make up`, `make dev`, `make register` twice; `make test-integration`,
  `make test-e2e`, `make test-fault` green; the `force: false` check for an added and a removed handler; the
  facts marked "task" in the plan confirmed on the pinned versions.
- [ ] T20 (AC-010-18 … -21, -23, -24, -27, SC-004) `make measure` in its three modes; `make spike`;
  `make report-coverage` after the e2e tier shows `src/ems/actors/probe.py` covered; `make reset` leaves no
  ingress container and no volume.

## Phase 7: Docs

- [ ] T21 (SC-002) `docs/context/02-subsystem-actors.md`, with its rows in `docs/context/README.md` and the
  `ems-context` skill.
- [ ] T22 (SC-001, SC-002) 00, 01 (Restate facts, V1–V4 and V7–V11, ownership row, stage 3 brief), 05, 08, 09,
  11, 13.
- [ ] T23 (SC-002, SC-004) `docs/thesis/spike-reproduction.md` and `docs/thesis/kafka-ingress.md` from the result
  files.
- [ ] T24 (SC-002) `README.md` ("Start the engine", `make reset`), `CLAUDE.md` (commands, tiers),
  `.claude/rules/tests.md` and `actors.md`, the `ems-actor-contract` and `ems-deploy-checklist` skills.

## Phase 8: Pull request

- [ ] T25 (P1, SC-003) Check the code against the spec, plan and tasks; `make format`; `make ci`; open the pull
  request with `ems-pr`. AC-010-29 (the coverage badge) is checked on the first workflow run on `main` after the
  merge, as the plan says.
