---
name: ems-deploy-checklist
description: Pre-flight checklist for infrastructure and tooling work in the EMS repo, covering Docker Compose (Restate, Kafka), kind, the Helm umbrella chart, CI, pre-commit, the Makefile, Python dependency pins, image tags and releases. Use before creating or changing any of these, before bumping a pinned version, and before running make up, make register, make kind-up, helm or deploy commands.
---

# ems-deploy-checklist

Read `docs/context/05-deploy-and-infra.md` first. It holds the verified versions (with source and date
checked), the current state of each piece and what is still to be built; most of the environment is
backlog spec 001, the Helm deployment is spec 011.

## Before changing anything

- [ ] An approved spec or task covers the change (or the user asked for this specific fix or bump).
- [ ] Two-week rule: every new or bumped version (PyPI package, image, chart, pre-commit rev, CLI tool)
      was released at least 14 days ago. Record version, release date and source URL in 05.
- [ ] Pins are exact. Python: `uv.lock` under `[tool.uv] exclude-newer`. Images: tag plus `@sha256:`
      digest (tags such as `restatedev/restate:1.7.12` are re-pushed by weekly OS refreshes). Helm:
      `Chart.lock`. Pre-commit: tagged `rev`, with the ruff rev equal to the ruff pin in `pyproject.toml`.
- [ ] No secrets in files; settings come from the environment through pydantic-settings.

## Before running anything

- [ ] Ports are free: `lsof -nP -iTCP -sTCP:LISTEN | grep -E ':(8080|9070|5122|9080|9081|9092)\b'`.
      The reference spike's native `restate-server` and hypercorn processes may still be running from an
      earlier session; stop them by port, not with `pkill`, and only after asking the user.
- [ ] Kubernetes: `kubectl config current-context` must be `kind-ems`. This machine's default context is
      a corporate cluster; never run `helm install|upgrade` or `kubectl apply|delete` against it
      (`make deploy-local` prompts for approval for this reason).
- [ ] Docker Desktop has enough memory for what you start (its VM has 8 GB; Restate, Kafka and kind share it).

## Compose (`infra/docker-compose.yml`)

- Restate image pinned by tag and digest; healthcheck `curl -fsS http://127.0.0.1:9070/health`; host
  ports bound to `127.0.0.1`; `extra_hosts: ["host.docker.internal:host-gateway"]` so the container can
  reach SDK services on the host (Linux engines need it); a named volume on `/restate-data` with a fixed
  node name, so state survives the `kill -9` fault experiments.
- Kafka-compatible broker: single node, KRaft, one listener for the compose network and one for the host,
  a healthcheck, a bounded JVM heap.
- `make up` runs `docker compose up -d --wait`, so it is only as good as the healthchecks.

## Helm (Helm 4, chart `apiVersion: v2`)

- Dependencies locked in `Chart.lock`; `helm dependency build` restores them exactly; `helm dependency
  update` only for a deliberate bump.
- `helm lint --strict` with each values file, then `helm template`; pin the Restate image digest in values.

## kind

- One kind version for everyone and a node image pinned by digest from that release's notes (the
  Kubernetes minor must suit the installed kubectl and Helm).

## After

- [ ] Update 05 (versions, state), 09 (new risks), and add an ADR if a decision changed.
- [ ] Put the verification commands and their output in the PR description.
