#!/usr/bin/env bash
# PreToolUse hook for Edit|Write, registered in the frontmatter of .claude/agents/test-engineer.md.
# The test engineer writes failing tests first and must not change production code or config:
# any file outside tests/ is blocked (exit 2), and the reason is shown to the subagent.
set -u

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty' 2>/dev/null) || exit 0
[ -n "$file" ] || exit 0

root="${CLAUDE_PROJECT_DIR:-$PWD}"
case "$file" in
  "$root"/tests/*) exit 0 ;;
esac

echo "tests-only hook: test-engineer may only write under tests/ (got ${file#"$root"/}). Report the needed production or config change to the main session instead." >&2
exit 2
