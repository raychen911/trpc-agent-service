# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.12-alpine3.24

FROM ${PYTHON_IMAGE} AS builder

ARG TRPC_PYPI_INDEX_URL="https://pypi.org/simple"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_INDEX_URL=${TRPC_PYPI_INDEX_URL} \
    UV_INDEX_URL=${TRPC_PYPI_INDEX_URL} \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

RUN python -m pip install --no-cache-dir "uv==0.8.13" \
    && python -m uv --version

WORKDIR /build

# Keep dependency resolution in a cacheable layer.  The lock file is required
# for production builds; a source checkout without one should fail early.
COPY pyproject.toml uv.lock ./
RUN uv venv "$VIRTUAL_ENV" \
    && uv sync --active --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --active --frozen --no-dev --no-editable


FROM ${PYTHON_IMAGE} AS runtime

# The performance acceptance binds a running candidate image to the exact
# checkout fingerprint used by the report. An empty value is allowed for
# ordinary development images; the performance override supplies this build
# argument explicitly and the gate rejects missing or malformed labels.
ARG TRPC_SOURCE_FINGERPRINT=""

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    TRPC_SERVICE_ROLE=gateway

LABEL org.opencontainers.image.revision="${TRPC_SOURCE_FINGERPRINT}" \
      io.trpc.agent-service.source-fingerprint="${TRPC_SOURCE_FINGERPRINT}"

RUN addgroup -S -g 10001 trpc \
    && adduser -S -D -u 10001 -G trpc -h /home/trpc trpc \
    && mkdir -p /app /tmp/trpc-service \
    && chown -R trpc:trpc /app /tmp/trpc-service

COPY --from=builder --chown=trpc:trpc /opt/venv /opt/venv
COPY --from=builder --chown=trpc:trpc /build /app

WORKDIR /app
USER 10001:10001

EXPOSE 8080 8081

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/live', timeout=3)"

ENTRYPOINT ["trpc-service"]
CMD ["serve", "--role", "gateway"]
