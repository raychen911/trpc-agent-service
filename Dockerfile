FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml ./
RUN python -c "import tomllib; from pathlib import Path; p=tomllib.loads(Path('pyproject.toml').read_text())['project']; Path('/tmp/runtime-requirements.txt').write_text('\n'.join(p['dependencies']+p['optional-dependencies']['im']))" \
    && pip install -r /tmp/runtime-requirements.txt
COPY trpc_service ./trpc_service
COPY README.md ./
COPY deploy ./deploy
RUN pip install --no-deps .

RUN useradd --create-home --uid 10001 appuser
USER appuser

ENTRYPOINT ["python", "-m", "trpc_service._cli"]

FROM base AS e2e
USER root
RUN pip install 'pytest==8.4.2' 'pytest-asyncio==1.2.0'
USER appuser
ENTRYPOINT ["python"]

FROM base AS runtime
