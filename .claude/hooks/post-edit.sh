#!/usr/bin/env bash
# PostToolUse hook for Edit|Write (wired in .claude/settings.json).
#
# 1. Guard: a Restate import in src/ems/domain/ breaks ADR 0004 / principle 2. Exit 2 shows the
#    message to Claude (the edit has already happened, so Claude must move the code).
# 2. Format the edited .py file with the project's pinned ruff: `check --fix` before `format`
#    (ruff's documented order). F401 is not auto-fixed here, so an import added in one edit survives
#    until the edit that uses it; `make format` at the end of the change does the full fix.
#    Until the dev environment exists (spec 001) ruff is absent and this step is skipped.
#
# Never fails the session: missing jq, uv or ruff means "do nothing".
set -u

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty' 2>/dev/null) || exit 0
case "$file" in
  *.py | *.pyi) ;;
  *) exit 0 ;;
esac
[ -f "$file" ] || exit 0
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

status=0
case "$file" in
  */src/ems/domain/*)
    if grep -nE '^[[:space:]]*(import|from)[[:space:]]+restate([.[:space:]]|$)' "$file" >&2; then
      echo "post-edit hook: Restate import in ${file#"$PWD"/} - the domain core must stay runtime-agnostic (ADR 0004, principle 2). Move the runtime code to src/ems/actors/." >&2
      status=2
    fi
    ;;
esac

if command -v uv >/dev/null 2>&1 && uv run --no-sync ruff --version >/dev/null 2>&1; then
  uv run --no-sync ruff check --fix --unfixable F401 --force-exclude --quiet "$file" >/dev/null 2>&1
  uv run --no-sync ruff format --force-exclude --quiet "$file" >/dev/null 2>&1
fi

exit "$status"
