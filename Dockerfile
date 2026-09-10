FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.8.17 /uv /uvx /bin/

ARG TRPC_SOURCE_FINGERPRINT=""
ARG VCS_REF=""

RUN addgroup --system --gid 10001 trpcagent \
    && adduser --system --uid 10001 --gid 10001 --home /home/trpcagent trpcagent

WORKDIR /app
COPY pyproject.toml uv.lock ./
# BuildKit 缓存挂载：wheel 缓存留在构建缓存里而不进镜像层，
# 因此镜像体积与 --no-cache-dir 相同，但重建时无需重新下载依赖。
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev \
    && chown -R trpcagent:trpcagent /app /home/trpcagent

ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"
ENV HOME=/tmp
LABEL org.opencontainers.image.revision="${VCS_REF}" \
      io.trpc.agent-service.source-fingerprint="${TRPC_SOURCE_FINGERPRINT}"
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "trpc_service.web.app:app", "--host", "0.0.0.0", "--port", "8000"]
