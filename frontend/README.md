# frontend/ — reserved

This folder is a placeholder for a future web UI. It is **out of scope** until an approved spec exists;
the initial backlog does not schedule it before the thesis core is done.

- Do not scaffold, generate or modify anything here without an approved spec, plan and task list.
- Planned direction (not a decision yet): latest stable React with TypeScript, custom components built on
  the project's own design tokens, no third-party component library.
- Integration contract with the root `Makefile`: once `frontend/package.json` exists, `make install`,
  `make lint` and `make format` also run `make -C frontend install|lint|format`, so the frontend must then
  provide its own `Makefile` with those three targets.
