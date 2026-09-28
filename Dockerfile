# syntax=docker/dockerfile:1.7

# Keep the container interpreter aligned with the project's locked Python range.
FROM ghcr.io/astral-sh/uv:0.11.28 AS uv
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

COPY --from=uv /uv /uvx /usr/local/bin/

RUN groupadd --gid 10001 trpc \
    && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/trpc trpc

WORKDIR /app

# Install locked third-party dependencies before copying source so normal code
# changes retain the expensive dependency layer.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY trpc_service ./trpc_service
RUN uv sync --frozen --no-dev \
    && mkdir -p /data/workspaces \
    && chown -R trpc:trpc /app /data/workspaces

USER 10001:10001

EXPOSE 8000

ENTRYPOINT ["/app/.venv/bin/trpc-agent-service"]
