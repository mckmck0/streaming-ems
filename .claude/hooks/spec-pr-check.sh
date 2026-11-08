#!/usr/bin/env bash
# Stop hook (wired in .claude/settings.json): once a spec is done, make sure it has a pull request.
#
# "Done" means: the current branch is a spec branch `NNN-slug`, `specs/NNN-slug/tasks.md` exists, it has
# at least one checked task and no unchecked one. If the `ems-pr` skill has not recorded a PR (or a
# skip) for the branch in `.git/ems-pr/<branch>`, the stop is blocked once with a reason that tells
# Claude to run the skill. The user asked for this automation on 2026-11-08.
#
# Never fails the session: anything unexpected means "do nothing".
set -u

input=$(cat)
# A stop that this hook already continued must be allowed through, or it would loop.
active=$(printf '%s' "$input" | jq -r '.stop_hook_active // false' 2>/dev/null) || active=false
[ "$active" = "true" ] && exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0
branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null) || exit 0
case "$branch" in
  [0-9][0-9][0-9]-*) ;;
  *) exit 0 ;;
esac

tasks="specs/$branch/tasks.md"
[ -f "$tasks" ] || exit 0
grep -qE '^[[:space:]]*- \[[xX]\]' "$tasks" || exit 0
grep -qE '^[[:space:]]*- \[ \]' "$tasks" && exit 0

git_dir=$(git rev-parse --git-dir 2>/dev/null) || exit 0
[ -f "$git_dir/ems-pr/$branch" ] && exit 0

reason="Every task in $tasks is checked and branch $branch has no pull request yet. Run the ems-pr skill now (it checks, commits, pushes and opens the PR). If the user declined a PR for this branch, record the skip as the skill describes."
jq -n --arg r "$reason" '{decision: "block", reason: $r}'
