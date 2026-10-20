# ems: root Makefile.
# Only the targets the repo can use today are active. The rest of the planned surface (brief §7.2) is kept
# commented out at the bottom, grouped by the backlog spec that switches it on; uncomment a block when that
# spec lands and move its target into .PHONY and `help`. The original template is in templates/Makefile.
# Keep it GNU Make 3.81-compatible (macOS default): no .ONESHELL or .SHELLFLAGS.
.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help install format lint type-check test ci clean

help:
	@echo ""
	@echo "ems: root Makefile"
	@echo ""
	@echo "  install     uv sync (all groups) + pre-commit hooks"
	@echo "  format      ruff check --fix + ruff format. Run at the end of every code change"
	@echo "  lint        uv lock --check + ruff check + ruff format --check"
	@echo "  type-check  ty check"
	@echo "  test        pytest (no tests yet counts as a pass until the test tiers exist)"
	@echo "  ci          lint + type-check + test"
	@echo "  clean       remove caches and build artefacts"
	@echo ""

install:
	uv sync --all-groups
	@if uv run --no-sync pre-commit --version >/dev/null 2>&1; then \
	  uv run --no-sync pre-commit install; \
	else \
	  echo "pre-commit is not a dev dependency yet: skipping hook install"; \
	fi

format:
	uv run ruff check --fix .
	uv run ruff format .

lint:
	uv lock --check
	uv run ruff check .
	uv run ruff format --check .

type-check:
	uv run ty check

# pytest exits 5 when it collects no tests. Accept that only while the repo has no tests; once the tiers
# below exist, an empty tier must fail again.
test:
	@uv run pytest; status=$$?; \
	if [ $$status -eq 5 ]; then echo "no tests collected yet"; exit 0; fi; \
	exit $$status

ci: lint type-check test
	@echo ""
	@echo "All CI checks passed."

clean:
	rm -rf .pytest_cache .ruff_cache .hypothesis build dist logs
	find src -name __pycache__ -type d -prune -exec rm -rf {} +

# ───────────────────────────────────────────────────────────────────────────────────────────────────
# Planned targets: commented out until their spec lands (details: docs/context/05-deploy-and-infra.md)
# ───────────────────────────────────────────────────────────────────────────────────────────────────

# ── Spec 001: test tiers with combined coverage (markers: unit, property, integration, e2e, fault) ──
# coverage 7.14+ combines implicitly when reporting; --keep-combined keeps the per-tier files.
# LOGS_DIR := logs
# test-unit:
# 	@mkdir -p $(LOGS_DIR)
# 	COVERAGE_FILE=$(LOGS_DIR)/.coverage.unit uv run pytest -m "unit or property" --cov=ems --cov-report=
# test-integration: up
# 	@mkdir -p $(LOGS_DIR)
# 	COVERAGE_FILE=$(LOGS_DIR)/.coverage.integration uv run pytest -m integration --cov=ems --cov-report=
# test-e2e: up
# 	@mkdir -p $(LOGS_DIR)
# 	COVERAGE_FILE=$(LOGS_DIR)/.coverage.e2e uv run pytest -m e2e --cov=ems --cov-report=
# test-fault: up
# 	uv run pytest -m fault
# report-coverage:
# 	uv run coverage xml --keep-combined --data-file $(LOGS_DIR)/.coverage -o $(LOGS_DIR)/coverage.xml
# 	uv run coverage report --keep-combined --data-file $(LOGS_DIR)/.coverage
# test: test-unit test-integration test-e2e report-coverage   # replaces the simple `test` above
# pre-commit:
# 	uv run pre-commit run --all-files

# ── Spec 001: local infrastructure (infra/docker-compose.yml: Restate + Kafka-compatible broker) ──
# COMPOSE       := docker compose -f infra/docker-compose.yml
# RESTATE_ADMIN ?= http://localhost:9070
# SERVICE_URL   ?= http://host.docker.internal:9080
# FORCE         ?= false   # true overwrites a deployment at the same URI: dev only
# up:
# 	$(COMPOSE) up -d --wait
# down:
# 	$(COMPOSE) down
# register:
# 	curl -fsS -X POST $(RESTATE_ADMIN)/deployments \
# 	  -H "content-type: application/json" \
# 	  -d '{"uri": "$(SERVICE_URL)", "force": $(FORCE)}'

# ── Spec 003: run the Restate SDK services locally ──
# dev:
# 	uv run hypercorn ems.actors.app:app --bind 0.0.0.0:9080 --reload

# ── Spec 010: experiments ──
# exp:
# 	@test -n "$(NAME)" || { echo "usage: make exp NAME=<experiment-id>"; exit 1; }
# 	uv run python -m ems.experiments.run experiments/configs/$(NAME).yaml

# ── Spec 011: kind + Helm (Helm 4). Never target a context other than kind-$(KIND_CLUSTER). ──
# CHART        := helm/ems
# KIND_CLUSTER ?= ems
# KUBE_CONTEXT := kind-$(KIND_CLUSTER)
# kind-up:
# 	kind create cluster --name $(KIND_CLUSTER) --config infra/kind-config.yaml
# kind-down:
# 	kind delete cluster --name $(KIND_CLUSTER)
# helm-deps:          # restore exactly what Chart.lock says; `helm dependency update` only for bumps
# 	helm dependency build $(CHART)
# helm-deps-check:
# 	@set -o pipefail; test -f $(CHART)/Chart.lock || { echo "Chart.lock missing"; exit 1; }; \
# 	helm dependency list $(CHART) | awk 'NR > 1 && NF && $$NF != "ok" && $$NF != "unpacked" { bad = 1 } END { exit bad }' \
# 	  || { echo "Helm dependencies missing or stale: run make helm-deps"; exit 1; }
# helm-lint: helm-deps
# 	helm lint --strict $(CHART) -f $(CHART)/values-local.yaml
# 	helm lint --strict $(CHART) -f $(CHART)/values-cluster.yaml
# 	helm template ems $(CHART) > /dev/null
# deploy-local: helm-deps
# 	@kind get clusters | grep -qx $(KIND_CLUSTER) || { echo "kind cluster $(KIND_CLUSTER) not found"; exit 1; }
# 	helm upgrade --install ems $(CHART) --kube-context $(KUBE_CONTEXT) -n ems --create-namespace \
# 	  -f $(CHART)/values-local.yaml
# (then add helm-deps-check to `lint`)

# ── Frontend (no spec yet): once frontend/package.json exists, delegate install/lint/format ──
# HAS_FRONTEND := $(wildcard frontend/package.json)
# ifneq ($(HAS_FRONTEND),)
# 	$(MAKE) -C frontend install|lint|format
# endif
