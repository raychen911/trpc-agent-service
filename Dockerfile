FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY trpc_service ./trpc_service

RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:${PATH}"

CMD ["trpc-agent-shared-serve", "--host", "0.0.0.0", "--port", "8080", "--node-id", "container-node"]
