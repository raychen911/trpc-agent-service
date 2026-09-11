FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY trpc_service ./trpc_service
COPY migrations ./migrations
RUN uv sync --frozen --no-dev
USER 65532:65532
EXPOSE 8080
CMD ["/app/.venv/bin/trpc-service", "serve"]
