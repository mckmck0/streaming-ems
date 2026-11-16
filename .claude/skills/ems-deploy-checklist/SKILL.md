---
name: ems-deploy-checklist
description: Pre-flight checklist for infrastructure and tooling work in the EMS repo, covering Docker Compose (Restate, Kafka), kind, the Helm umbrella chart, CI, pre-commit, the Makefile, Python dependency pins, image tags and releases. Use before creating or changing any of these, before bumping a pinned version, and before running make up, make register, make kind-up, helm or deploy commands.
---

# ems-deploy-checklist

Read `docs/context/05-deploy-and-infra.md` first. It holds the verified versions (with source and date
checked), the current state of each piece and what is still to be built; most of the environment is
the data-pipeline spec 001 (Kafka) and the dev-environment spec (Restate); the Helm deployment is an optional spec, started only behind the gate in `00-overview.md` (spec 006).

## Before changing anything

- [ ] An approved spec or task covers the change (or the user asked for this specific fix or bump).
- [ ] Newest release: every new or bumped version (PyPI package, image, action, chart, pre-commit rev, CLI
      tool) is the newest release when pinned; read its release notes. Record version, release date and
      source URL in 05. There is no minimum age (decided 2026-11-15, spec 010).
- [ ] Pins are exact. Python: `uv.lock`. Actions: full commit SHA. Images: tag plus `@sha256:`
      digest (tags such as `restatedev/restate:1.7.12` are re-pushed by weekly OS refreshes). Helm:
      `Chart.lock`. Pre-commit: tagged `rev`, with the ruff rev equal to the ruff pin in `pyproject.toml`.
- [ ] No secrets in files; settings come from the environment through pydantic-settings.

## Before running anything

- [ ] Ports are free: `lsof -nP -iTCP -sTCP:LISTEN | grep -E ':(8080|9070|9080|9081|9092)\b'`.
      `make dev`, a test tier or `make spike` may hold 9080/9081; stop them by port or process group, not
      with `pkill`, and only after asking the user. `make spike` also needs 8080 and 9070 (`make down`).
- [ ] Kubernetes: `kubectl config current-context` must be `kind-ems`. This machine's default context is
      a corporate cluster; never run `helm install|upgrade` or `kubectl apply|delete` against it
      (`make deploy-local` prompts for approval for this reason).
- [ ] Docker Desktop has enough memory for what you start (its VM has 7.75 GiB: Kafka 1 GB, Restate 2 GB,
      768 MB per ingress container, and kind too if the optional Helm spec runs).

## Compose (`infra/docker-compose.yml`)

- Restate image pinned by a dated tag and digest (`restatedev/restate:1.8.1-hotfix.2-20261102@sha256:…`);
  healthcheck on the admin `/health` and the ingress `/restate/health`; host ports bound to `127.0.0.1`;
  `extra_hosts: ["host.docker.internal:host-gateway"]` so the container reaches the SDK service on the host
  (Linux engines need it); the named volume `restate-data` with the fixed node name `restate-1`, so state
  survives the `kill -9` fault experiments.
- Kafka ingress integration: one container per target handler in the `ingress` profile (only `make register`
  starts them, `make down` removes them), `KAFKA_AUTO_OFFSET_RESET=earliest`, `restart: unless-stopped`, label
  `ems.role=ingress`, healthcheck `["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9464"]` (the image's
  `/bin/sh` is dash). The image is linux/amd64 only.
- Registration: the admin API's `force` defaults to **true**; `make register` sends false. With false, an
  existing URI answers 200 and keeps its old handlers even when they changed: re-register with `FORCE=true`.
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
