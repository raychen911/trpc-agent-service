FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY trpc_service ./trpc_service
RUN pip install --upgrade pip==24.3.1 && pip install .
COPY alembic.ini ./
COPY alembic ./alembic
COPY docs ./docs
COPY .env.example ./
USER 10001
CMD ["python", "-m", "trpc_service._cli", "api"]
