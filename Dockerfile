FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY trpc_service ./trpc_service

COPY alembic.ini ./
COPY migrations ./migrations

RUN pip install --no-cache-dir ".[storage,production]"

EXPOSE 8000

CMD ["python", "-m", "trpc_service", "serve"]
