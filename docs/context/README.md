# Canonical project docs (`docs/context/`)

These files are the current consensus for the EMS thesis project. Claude Code reaches them through the
`ems-context` skill; people can start here.

Each doc describes its piece in four parts: **Current consensus** (decided or verified, with sources),
**What to build** (deliverables and the backlog spec that owns them), **Prerequisites**, and **Open
questions**. `OPEN` marks an undecided choice, `VERIFY` a fact that still has to be checked against an
official source. Versions were checked on 2026-10-16; "at least two weeks old" then means released on or
before 2026-10-02.

| File | Covers |
|---|---|
| [00-overview.md](00-overview.md) | Purpose, scope, status of every piece, repo map, backlog, decisions so far |
| [01-architecture.md](01-architecture.md) *(planned)* | Actors, keys, single-writer ownership, message diagram |
| [02-subsystem-actors.md](02-subsystem-actors.md) *(planned)* | Restate services and handlers, rules R1–R7, registration, Kafka ingress |
| [03-subsystem-strategies-and-evaluation.md](03-subsystem-strategies-and-evaluation.md) *(planned)* | Policy contract, shadow vs live, Evaluator, Selector |
| [04-subsystem-forecasting-and-optimization.md](04-subsystem-forecasting-and-optimization.md) *(planned)* | Forecasters, MILP/MPC, oracle, data sources |
| [05-deploy-and-infra.md](05-deploy-and-infra.md) *(planned)* | uv project and tooling, Makefile, pre-commit, Compose, kind, Helm, CI, pinned versions |
| [06-data-flows.md](06-data-flows.md) *(planned)* | Tick, live loop, replay and sim clock, failure and recovery, timers |
| [07-design-principles.md](07-design-principles.md) | The 16 invariants; the source of the Spec Kit constitution |
| [08-contracts.md](08-contracts.md) *(planned)* | Message envelope and schemas, Kafka topics, result schemas, versioning rules |
| [09-risks-and-tech-debt.md](09-risks-and-tech-debt.md) *(planned)* | Known issues and risks with fix hints |
| [10-planning-guide.md](10-planning-guide.md) | How to plan a change: `ems-plan-change`, Spec Kit setup and loop, checklists |
| [11-experiments.md](11-experiments.md) *(planned)* | Experiment protocol, metrics, statistics, reference-spike results |
| [12-domain-model.md](12-domain-model.md) *(planned)* | Time base, units, battery model, settlement and net-billing, actions, glossary |

Elsewhere: decision records in [../adr/](../adr/README.md); the frozen seed brief in
[../bootstrap/PROJECT_CONTEXT.md](../bootstrap/PROJECT_CONTEXT.md), read only to trace where something came
from; thesis notes in `../thesis/`.

## Which source wins

`CLAUDE.md` and [07-design-principles.md](07-design-principles.md) → the other files here → skills and
rules in `.claude/` → the bootstrap brief → auto memory. The Spec Kit constitution
(`.specify/memory/constitution.md`) is generated from 07 and must match it. Accepted ADRs explain
decisions, and these docs must agree with them. Real contradictions are raised, not silently resolved.

## Keeping the docs current

- A behaviour change updates the affected doc in the same change; a decision change adds an ADR.
- Adding, renaming or removing a file here also updates this index and the table in
  `.claude/skills/ems-context/SKILL.md`.
- `12-domain-model.md` was added at bootstrap: the brief's split (00–11) had no home for the domain model.

## References

- Restate: <https://docs.restate.dev> (server on Kubernetes: <https://docs.restate.dev/server/deploy/kubernetes>,
  services on Kubernetes: <https://docs.restate.dev/services/deploy/kubernetes>, operator:
  <https://github.com/restatedev/restate-operator>, licence: <https://github.com/restatedev/restate/blob/main/LICENSE>)
- Claude Code: <https://code.claude.com/docs/en/claude-directory> and the memory, skills, sub-agents, hooks,
  settings and permissions pages
- GitHub Spec Kit: <https://github.com/github/spec-kit>
- uv: <https://docs.astral.sh/uv/>; ruff: <https://docs.astral.sh/ruff/>; ty: <https://github.com/astral-sh/ty>;
  ruff pre-commit: <https://github.com/astral-sh/ruff-pre-commit>
- Helm: <https://helm.sh/docs/>; kind: <https://kind.sigs.k8s.io/>; Apache Kafka: <https://kafka.apache.org/documentation/>
- FLIP-569 (StateFun sunset): <https://cwiki.apache.org/confluence/display/FLINK/FLIP-569:+Sunset+the+Stateful+Functions+(StateFun)+Sub-project>
- Dapr actors: <https://docs.dapr.io/developing-applications/building-blocks/actors/>
- Forecasting: Chronos-2 <https://arxiv.org/abs/2510.15821>; Tan et al. (NeurIPS 2024) <https://arxiv.org/abs/2406.16964>;
  epftoolbox <https://github.com/jeslago/epftoolbox>; pvlib <https://pvlib-python.readthedocs.io>
- Data: ENTSO-E Transparency Platform <https://transparency.entsoe.eu>; Open-Meteo <https://open-meteo.com>;
  PSE market data API <https://api.raporty.pse.pl>; CityLearn <https://github.com/intelligent-environments-lab/CityLearn>
- Polish law: consolidated RES act (ustawa o OZE), Dz.U. 2026 poz. 68 <https://api.sejm.gov.pl/eli/acts/DU/2026/68/text.pdf>
