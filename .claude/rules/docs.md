---
paths:
  - "docs/**"
---

# Documentation (`docs/`)

- `docs/context/` holds the canonical docs. Each piece is described as **Current consensus**, **What to
  build**, **Prerequisites** and **Open questions**; keep that structure when editing.
- Mark undecided choices `OPEN` and facts that still need checking against an official source `VERIFY`.
  When you resolve a VERIFY item, record the value, the source URL and the date checked.
- Adding, renaming or removing a file in `docs/context/` means updating `docs/context/README.md` and the
  table in `.claude/skills/ems-context/SKILL.md` in the same change.
- A changed decision gets a new ADR in `docs/adr/NNNN-short-title.md` (template in `docs/adr/README.md`);
  accepted ADRs are not rewritten, they are superseded.
- `docs/bootstrap/PROJECT_CONTEXT.md` is a frozen seed: never edit it; correct the canonical doc instead.
- `docs/thesis/` holds notes that feed thesis chapters (for example `spike-reproduction.md`); numbers in it
  come from recorded experiment results, never from memory.
