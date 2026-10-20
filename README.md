# Streaming EMS

MSc thesis project: an energy management system for residential prosumers (PV + home battery + grid,
Polish/EU market, 15-minute intervals) built on durable stateful actors (Restate Virtual Objects, Python
SDK). The same live stream feeds many battery strategies, each on its own virtual battery, so they can be
compared counterfactually while one of them drives the real battery.

- **Start here:** [docs/context/README.md](docs/context/README.md) (canonical docs, status of every piece).
- **Working with Claude Code:** [CLAUDE.md](CLAUDE.md) and the harness in `.claude/` (skills, rules,
  subagents, hooks). Changes go spec-first through the `ems-plan-change` skill and GitHub Spec Kit.
- **Status:** the Claude Code harness and the canonical docs exist. The environment, the reference-spike
  reproduction and the code are built spec by spec, starting with backlog spec 001
  (see [docs/context/00-overview.md](docs/context/00-overview.md)).

Prerequisites and pinned versions: [docs/context/05-deploy-and-infra.md](docs/context/05-deploy-and-infra.md).
