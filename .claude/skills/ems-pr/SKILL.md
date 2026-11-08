---
name: ems-pr
description: Opens or updates the GitHub pull request for a finished spec branch. Checks the gates (all tasks checked, make format, make ci), commits what is left as Conventional Commits, pushes the branch, writes the title and description from the spec, plan, tasks and diff with the repo's PR template and the CLAUDE.md PR description contract, and creates the PR with gh. Use when every task in specs/NNN-*/tasks.md is checked (a Stop hook asks for it), at the end of ems-plan-change, or when the user asks to open, write or update a PR.
argument-hint: "[spec number or branch; default: current branch]"
---

# ems-pr: pull request for a finished spec

The user asked (2026-11-08) for a PR to be opened automatically after each spec is done. The Stop hook
`.claude/hooks/spec-pr-check.sh` blocks a stop once every task of the current spec branch is checked and
no PR is recorded for it; this skill then does the work. It never merges.

## 1. Gates (stop and report on the first failure)

1. Branch is a spec branch `NNN-slug` (not `main`) and `specs/NNN-slug/` has `spec.md`, `plan.md` and
   `tasks.md`. No branch yet: create it from the spec header ("Feature Branch") before committing.
2. Every task in `tasks.md` is `- [x]`. Otherwise list the open tasks and stop.
3. `make format`, then `make ci` is green. Record the test count (`N passed`) for the test plan.
4. If the plan names other tiers or smoke runs (integration, Kafka runs), they were run in the
   verify task; take their results from it. Do not invent results: a check that did not run is
   written as not run.

## 2. Commit and push

- Commit what is left as Conventional Commits (CLAUDE.md "Commits"): one commit per logical change, the
  usual order `spec(NNN)` → `test` / `feat` / `fix` / `refactor` → `docs`, each subject ending with its
  task ids. Every commit must leave `make ci` green, so tests go with the code they test.
- Stage explicit paths and check `git diff --cached --stat` before each commit: a file staged earlier
  (for example by `git rm`) otherwise lands in the wrong commit.
- Never amend or rewrite pushed commits and never force-push. Push with `git push -u origin <branch>`
  (the settings ask the user to confirm).

## 3. Title and description

- **Title**: `type(scope): imperative summary (spec NNN)`, at most 72 characters, from the dominant
  change (Conventional Commits 1.0.0).
- **Body**: fill `.github/pull_request_template.md` section by section (it also serves PRs opened in the
  GitHub web UI). Drop its HTML comments; write "None" instead of leaving a section out. Sources:

  | Section | Source |
  |---|---|
  | Summary | `spec.md` goal and input; one link to the spec folder |
  | Changes | `git log --format='%h %s' main..HEAD`, `git diff --stat main...HEAD`, `plan.md` design |
  | Breaking changes, Contract impact | `plan.md` contract section, `schemas/` diff, `08-contracts.md` |
  | Experiment impact | `plan.md` experiment impact; P14/P16 in the principles check |
  | Design principles touched | `plan.md` Principles Check: the touched rows only, by number |
  | Known issues in scope | `09-risks-and-tech-debt.md` rows the spec names: fixed or accepted for now |
  | Test plan | the gates above plus the verify task; name the new test files and the criteria they cover |
  | Docs | files read for the spec (`ems-context`) and docs, ADRs changed in the diff |
  | Risks and rollback | `plan.md` risks; rollback is normally "revert the merge commit" plus any data step |
  | References | spec, plan, tasks, ADRs |

- Write for a reviewer who has not followed the session: plain sentences, repo-relative paths, no
  internal chat context. Keep it as long as the change needs, no longer.
- End the body with the attribution line `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- Save it to `$(git rev-parse --git-dir)/ems-pr/<branch>.md` (inside `.git`, never committed).

## 4. Create or update the PR

```bash
repo=$(git remote get-url origin | sed -E 's#^.*[:/]([^/]+/[^/]+)$#\1#; s#\.git$##')   # owner/name
gh auth status                                            # must be logged in to github.com
gh pr view "$branch" --repo "$repo" --json url -q .url    # exists → edit, else create
gh pr edit "$branch" --repo "$repo" --title "$title" --body-file "$body"
gh pr create --repo "$repo" --base main --head "$branch" --title "$title" --body-file "$body"
```

`--repo` is needed because `origin` uses an SSH host alias. Open the PR ready for review, not as a
draft, and never merge it.

**Without `gh`** (not installed or not logged in): do not install or log in yourself. Give the user the
compare link `https://github.com/<repo>/compare/main...<branch>?expand=1`, the title and the body file
path, and say that `brew install gh` then `gh auth login` lets the skill do it next time.

## 5. Record and report

- Write the outcome to `$(git rev-parse --git-dir)/ems-pr/<branch>`: the PR URL, `manual` (no `gh`; the
  user opens it from the compare link) or `skip` (the user declined a PR for this branch). The Stop hook
  stays quiet once this file exists; run the skill again by hand to update the PR after more commits.
- Report the PR URL (or the compare link), the title, and the gates that ran with their results.
