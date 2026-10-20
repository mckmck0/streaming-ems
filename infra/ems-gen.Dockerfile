# ems-gen: the EMS data generator (spec 001). Build context is the repo root:
#   docker build -f infra/ems-gen.Dockerfile .
# Pins (two-week rule, see docs/context/05-deploy-and-infra.md): uv 0.12.21 (2026-09-29),
# python 3.13.15-slim-trixie (pushed 2026-09-25). Debian/glibc is required: confluent-kafka has no musl wheels.
FROM ghcr.io/astral-sh/uv:0.12.21@sha256:a7aed3216253ee804de3e2d8afa5073baa1a177335345d43845cd4165e43b711 AS uv

FROM python:3.13.15-slim-trixie@sha256:7c61056e61ac89e852de05f3dc6fa51a6dd2181797bceed46aa725dd7cb2cd3b AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --group ingest --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --group ingest --no-editable

FROM python:3.13.15-slim-trixie@sha256:7c61056e61ac89e852de05f3dc6fa51a6dd2181797bceed46aa725dd7cb2cd3b
RUN useradd --uid 10001 --create-home ems
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY config ./config
COPY mappings ./mappings
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER ems
ENTRYPOINT ["ems-gen"]
CMD ["--help"]
