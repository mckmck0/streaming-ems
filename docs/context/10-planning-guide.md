# 10 · Planning guide (Spec Kit setup and loop)

How a change gets from idea to merged code. The entry point is the `ems-plan-change` skill; it runs the
checklists and drives Spec Kit with a human approval gate after every phase.

## Current consensus

### Spec Kit setup (done 2026-10-20)

Verified 2026-10-20 against <https://github.com/github/spec-kit> (releases and `specify init --help` of the tag):

| Item | Value |
|---|---|
| Release | **v1.1.0**, git tag 2026-10-02 (commit `f1d3a4f8337ebbd3ae22760a9c12e3352b93a175`). v1.1.1 (2026-10-06) is not yet 14 days old |
| Install | `uv tool install specify-cli --from git+https://github.com/github/spec-kit.git@v1.1.0 --exclude-newer 2026-10-02T00:00:00Z` (`uv tool install` does not inherit the project's `exclude-newer`) |
| Init in place | `specify init --here --force --non-interactive --integration claude --script sh`. The old `--ai` flag was removed in 0.10.0 |
| What init writes | `.specify/` (scripts, templates, memory) and Claude Code skills `.claude/skills/speckit-*/`, invoked with hyphens (`/speckit-specify`). `converge` is a core command. Init never touches `CLAUDE.md`; it writes hooks into `.claude/settings.json` only for extensions, and none are installed |
| Feature numbering | `sequential` (`.specify/init-options.json`): the next three-digit number after scanning `specs/`. An explicit folder can be forced with `SPECIFY_FEATURE_DIRECTORY=specs/NNN-slug` |

After an init or upgrade, check with `git diff` that `CLAUDE.md` and `.claude/settings.json` are unchanged, then
regenerate the constitution from `07-design-principles.md` with `/speckit-constitution`.

### The loop (per spec)

1. `ems-plan-change`: ground (`ems-context`), run the principle, contract and risk checklists.
2. `/speckit-specify`: goal, scope, out-of-scope, EARS acceptance criteria with ids (`AC-001-03`). **Stop.**
3. `/speckit-clarify` when the spec has open questions. **Stop.**
4. `/speckit-plan`: Constitution Check (P1–P16), Actor contract section when actors are touched, contract
   changes per `08-contracts.md`, test strategy per tier, docs and ADRs that change. Reviews by
   `architect-reviewer` (and `energy-domain-reviewer` for prices, settlement, battery, time/DST). **Stop.**
5. `/speckit-tasks`, then `/speckit-analyze`; resolve every CRITICAL finding. Test tasks come before the tasks
   they test. **Stop.**
6. The user types `/speckit-implement`. Afterwards `/speckit-converge` until converged, `make format`, `make ci`,
   docs and ADRs, PR description per `CLAUDE.md`.

### Specs are written as we go

Specs are created one piece at a time, when the work reaches that piece, rather than all up front; the backlog in
`00-overview.md` lists titles and order, and numbers are assigned by Spec Kit when a spec is created. A spec may
be revised while it is in progress (re-run `/speckit-specify`/`/speckit-plan` for the changed part and record it).

## What to build

- Nothing further for the setup. Optional extensions (agent-context, git) are deliberately not installed.

## Prerequisites

- `07-design-principles.md` (constitution source).

## Open questions

- None.
