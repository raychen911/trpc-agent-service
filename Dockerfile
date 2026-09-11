# Minimal runtime image: locked dependencies only (no dev tools), started via
# the existing project CLI (gateway / worker / admin / db-migrate / import).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Locked dependency closure (requirements.lock.txt, generated from the
# verified runtime environment).  --no-deps keeps resolution EXACTLY pinned.
COPY requirements.lock.txt ./
RUN pip install --no-deps -r requirements.lock.txt

# Application, migrations and data.  No secrets: .env/.venv/.git are excluded
# via .dockerignore and are never COPYed.
COPY pyproject.toml README.md ./
COPY trpc_service ./trpc_service
COPY migrations ./migrations
COPY data ./data
COPY alembic.ini ./
RUN pip install --no-deps . \
    && useradd --create-home --uid 10001 trpc \
    && chown -R trpc:trpc /app

USER trpc

EXPOSE 8000
ENTRYPOINT ["trpc-agent-service"]
CMD ["gateway", "--host", "0.0.0.0", "--port", "8000"]
