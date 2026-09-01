FROM ghcr.io/astral-sh/uv:0.12.7 AS uv
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY trpc_service ./trpc_service
COPY alembic.ini ./
COPY migrations ./migrations
COPY data/README.md ./data/README.md
RUN uv sync --frozen --no-dev --no-editable

RUN chown -R app:app /app
USER app

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2)"
ENTRYPOINT ["trpc-agent-service"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000"]
