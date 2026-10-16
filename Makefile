# ems — root Makefile (template). Target names mirror the work-harness surface:
# install · lint · format · type-check · pre-commit · test-* · report-coverage · ci · helm-* · clean
.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help install install-uv dev lint format type-check pre-commit \
        test test-unit test-integration test-e2e test-fault report-coverage ci \
        up down register exp \
        kind-up kind-down helm-deps helm-deps-check helm-lint deploy-local \
        clean clean-coverage

LOGS_DIR      := logs
COMPOSE       := docker compose -f infra/docker-compose.yml
RESTATE_ADMIN ?= http://localhost:9070
SERVICE_URL   ?= http://host.docker.internal:9080
CHART         := helm/ems
KIND_CLUSTER  ?= ems
HAS_FRONTEND  := $(wildcard frontend/package.json)
HAS_CHART     := $(wildcard $(CHART)/Chart.yaml)

# ── Help ─────────────────────────────────────────────────
help:
	@echo ""
	@echo "ems — root Makefile"
	@echo ""
	@echo "  install          uv sync (all groups) + pre-commit hooks (+ frontend once it exists)"
	@echo "  dev              Run the Restate SDK services locally with reload (port 9080)"
	@echo "  lint             ruff check + ruff format --check (+ helm deps check, + frontend)"
	@echo "  format           ruff check --fix + ruff format (+ frontend)"
	@echo "  type-check       ty check"
	@echo "  pre-commit       Run pre-commit on all files"
	@echo "  test-unit        Unit + property tests (no infrastructure)"
	@echo "  test-integration Actor tests against a real Restate server (docker compose)"
	@echo "  test-e2e         Replay tests, bit-exact vs the offline reference"
	@echo "  test-fault       Fault injection (kill -9 SDK service / Restate server)"
	@echo "  test             unit + integration + e2e, then combined coverage"
	@echo "  report-coverage  Combine coverage data into logs/coverage.xml"
	@echo "  ci               lint + type-check + test"
	@echo "  up / down        Start / stop local infra (Restate, Kafka)"
	@echo "  register         Register the local SDK services with Restate"
	@echo "  exp NAME=<id>    Run experiment experiments/configs/<id>.yaml"
	@echo "  kind-up/down     Create / delete the local kind cluster"
	@echo "  helm-deps        Update Helm chart dependencies"
	@echo "  helm-deps-check  Fail if Helm dependencies are missing or stale"
	@echo "  helm-lint        helm lint + helm template"
	@echo "  deploy-local     helm upgrade --install into the kind cluster"
	@echo "  clean            Remove caches, build and coverage artefacts"
	@echo "  install-uv       Bootstrap uv (one-off)"
	@echo ""

# ── Dependencies ─────────────────────────────────────────
install:
	uv sync --all-groups
	uv run pre-commit install
ifneq ($(HAS_FRONTEND),)
	$(MAKE) -C frontend install
endif

dev:
	uv run hypercorn ems.actors.app:app --bind 0.0.0.0:9080 --reload

# ── Code quality ─────────────────────────────────────────
lint: helm-deps-check
	uv run ruff check .
	uv run ruff format --check .
ifneq ($(HAS_FRONTEND),)
	$(MAKE) -C frontend lint
endif

format:
	uv run ruff check --fix .
	uv run ruff format .
ifneq ($(HAS_FRONTEND),)
	$(MAKE) -C frontend format
endif

type-check:
	uv run ty check

pre-commit:
	uv run pre-commit run --all-files

# ── Tests (pytest markers: unit, property, integration, e2e, fault) ──
test-unit:
	@mkdir -p $(LOGS_DIR)
	COVERAGE_FILE=$(LOGS_DIR)/.coverage.unit uv run pytest -m "unit or property" --cov=ems --cov-report=

test-integration: up
	@mkdir -p $(LOGS_DIR)
	COVERAGE_FILE=$(LOGS_DIR)/.coverage.integration uv run pytest -m integration --cov=ems --cov-report=

test-e2e: up
	@mkdir -p $(LOGS_DIR)
	COVERAGE_FILE=$(LOGS_DIR)/.coverage.e2e uv run pytest -m e2e --cov=ems --cov-report=

test-fault: up
	uv run pytest -m fault

test: clean-coverage test-unit test-integration test-e2e report-coverage

report-coverage:
	@files=$$(ls $(LOGS_DIR)/.coverage.* 2>/dev/null); \
	if [ -z "$$files" ]; then echo "No coverage data. Run make test or a test-* target first."; exit 1; fi; \
	uv run coverage combine --keep --data-file $(LOGS_DIR)/.coverage $$files && \
	uv run coverage xml --data-file $(LOGS_DIR)/.coverage -o $(LOGS_DIR)/coverage.xml && \
	uv run coverage report --data-file $(LOGS_DIR)/.coverage

ci: lint type-check test
	@echo ""
	@echo "All CI checks passed."

# ── Local infrastructure ─────────────────────────────────
up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

# VERIFY the admin API body against the Restate docs for the pinned server version.
register:
	curl -fsS -X POST $(RESTATE_ADMIN)/deployments \
	  -H "content-type: application/json" \
	  -d '{"uri": "$(SERVICE_URL)"}'

exp:
	@test -n "$(NAME)" || { echo "usage: make exp NAME=<experiment-id>"; exit 1; }
	uv run python -m ems.experiments.run experiments/configs/$(NAME).yaml

# ── Kubernetes / Helm ────────────────────────────────────
kind-up:
	kind create cluster --name $(KIND_CLUSTER) --config infra/kind-config.yaml

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

helm-deps:
	helm dependency update $(CHART)

helm-deps-check:
ifneq ($(HAS_CHART),)
	@command -v helm >/dev/null || { echo "helm not installed, skipping deps check"; exit 0; }; \
	helm dependency list $(CHART) | awk 'NR > 1 && NF && $$NF != "ok" && $$NF != "unpacked" { bad = 1 } END { exit bad }' \
	  || { echo "Helm dependencies missing or stale: run make helm-deps"; exit 1; }
endif

helm-lint:
	helm lint $(CHART)
	helm template ems $(CHART) > /dev/null

deploy-local: helm-deps
	helm upgrade --install ems $(CHART) -n ems --create-namespace -f $(CHART)/values-local.yaml

# ── Housekeeping ─────────────────────────────────────────
clean: clean-coverage
	rm -rf .pytest_cache .ruff_cache build dist
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

clean-coverage:
	rm -rf $(LOGS_DIR)

install-uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv --version
