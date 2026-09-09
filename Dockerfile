# ===================================================================
# Dockerfile - 生产服务镜像（K8s / kustomize 部署用）
# ===================================================================
# 说明: 多阶段构建。开发环境镜像（code-server / Node / Go 等）在
#   .ide/Dockerfile，由 .cnb.yml 维护，与本文件职责分离：
#   本镜像只含运行所需（Python + 锁定依赖 + 源码），无任何 IDE 工具链。
# 构建/推送:
#   docker build -t <registry>/trpc-agent-service:<tag> .
#   docker push <registry>/trpc-agent-service:<tag>
#   依赖以 uv.lock 精确锁定（--frozen），升级流程:
#   改 pyproject.toml → uv lock → 提交 uv.lock → 重新构建镜像
# ===================================================================

# -----------------------------------------------------------------------------
# 阶段一: 依赖安装（按 uv.lock 精确锁定）
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS deps

# uv 版本与本地开发环境一致（0.12.x），保证锁解析行为一致
RUN pip install --no-cache-dir "uv>=0.12,<0.13"

COPY pyproject.toml uv.lock /tmp/deps/
RUN cd /tmp/deps && \
    UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --frozen --no-install-project --compile-bytecode

# -----------------------------------------------------------------------------
# 阶段二: 运行时（仅源码 + 锁定依赖，非 root 运行）
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# 运行依赖: libpq 不需要（驱动为纯 Python aiomysql/asyncpg 自带实现由
# SQLAlchemy 按需加载）；仅补齐 CA 证书（模型 API / IM 回调走 HTTPS）
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=deps /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY trpc_service ./trpc_service
COPY config ./config

# 数据目录（本地运行兜底；K8s 下由 emptyDir 挂载覆盖，见 deploy/kustomize）
RUN useradd --uid 10001 --create-home teneuris && \
    mkdir -p /app/data && chown -R teneuris:teneuris /app
USER teneuris

EXPOSE 8000 8002

# 网关为默认入口；admin 经 Deployment 覆盖 command 启动。
# 配置经 ConfigMap 挂载到 /config（见 deploy/kustomize/base）
CMD ["python", "-m", "trpc_service._cli", "gateway", "--config", "/config/teneuris.yaml", "--storage", "redis", "--runner", "framework"]
