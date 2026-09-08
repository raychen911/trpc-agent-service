# tRPC Agent 多租户服务

这是一个基于 [tRPC-Agent-Python](https://github.com/trpc-group/trpc-agent-python) 的多租户 Agent 服务。项目不再复制或修改 SDK 源码，而是通过 PyPI 依赖 `trpc-agent-py` 复用 Agent、Model、Runner、Session 和 Memory 等基础能力；本仓库的业务代码统一放在 `trpc_service` 包中。

> Python 包名是 `trpc-agent-py`，安装后使用 `trpc_agent_sdk` 导入；本项目自身的发行包名是 `trpc-agent-service`，使用 `trpc_service` 导入。

## 设计概览

```text
企业微信 / 微信客服 / 钉钉 / 飞书 / QQ
                    │ Webhook
                    ▼
            Gateway + Channel Adapter
            验签、解析、幂等、快速 ACK
                    │
          ┌─────────┴─────────┐
          │ 进程内调用         │ Redis Streams
          ▼                   ▼
       TenantWorker       Worker 集群
          │                   │
          └──── Agent / Filter / Tool ────┐
                                          │
                    ┌────────────┬───────┼──────────┬─────────────┐
                    ▼            ▼       ▼          ▼             ▼
             Session/Memory   Knowledge Artifact Audit/Config OpenTelemetry
              Redis/MySQL      Qdrant   S3/Local     MySQL
```

Gateway 和 Worker 不保存租户会话状态。会话与记忆通过共享 Redis 或 MySQL 后端访问，因此 Worker 可以水平扩展而不要求 sticky session。租户配置、工具权限、预算、HITL 确认、审计和脱敏由服务层实现。

详细资料：

- [架构与模块设计](docs/enterprise/DESIGN.md)
- [最小可运行与生产部署](docs/enterprise/DEPLOYMENT.md)
- [租户接入指南](docs/enterprise/ONBOARDING.md)
- [多后端适配方案](docs/enterprise/BACKEND_ADAPTERS.md)
- [数据模型设计](docs/enterprise/DATA_MODEL.md)
- [数据同步与幂等策略](docs/enterprise/SYNC_AND_IDEMPOTENCY.md)
- [企业监控链路与指标调试](docs/enterprise/METRICS.md)
- [验收测试方案](docs/enterprise/ACCEPTANCE_TEST_PLAN.md)
- [PR7 / PR8 / PR9 架构整合与运维手册](docs/enterprise/ARCHITECTURE_EVOLUTION.md)
- [完整方案与数据模型](docs/SUBMISSION_PROPOSAL.md)

## 代码结构

```text
.
├── README.md                  # 设计、安装和使用说明
├── build.sh                   # 构建 wheel/sdist
├── clean.sh                   # 清理构建、缓存和覆盖率产物
├── coverage.sh                # 运行单测和覆盖率门禁
├── data/                      # 数据库 schema 等服务数据
├── deploy/                    # Docker Compose 与 Kubernetes 清单
├── docs/                      # 架构、部署和模块文档
├── format.sh                  # YAPF 格式化
├── lint_flake8.sh             # Flake8 静态检查
├── start.sh                   # 启动最小 Compose 环境
├── stop.sh                    # 停止 Compose 环境并保留数据卷
├── tests/service/             # 服务层测试
└── trpc_service/
    ├── _cli.py                # trpc-service 命令行入口
    ├── agent/                 # Worker、任务队列、锁、执行结果和模型降级
    ├── channels/              # IM Channel Adapter
    ├── config/                # 配置模型与加载入口
    ├── log/                   # 审计日志、持久化和敏感信息遮罩
    ├── metrics/               # 指标与 OpenTelemetry
    ├── skill/                 # 服务自定义 Skill 扩展位置
    ├── tenant/                # 多租户模型、配置管理和持久化
    ├── tool/                  # 权限、预算、HITL 和输出脱敏
    ├── version.py             # 服务版本
    ├── web/                   # Gateway、Admin API 和 FastAPI 装配
    └── workspace/             # Session/Memory/Vector/Object 后端路由与迁移
```

`trpc_agent_sdk/` 不属于本仓库源码。代码中的 `from trpc_agent_sdk...` 都来自外部 `trpc-agent-py` 依赖。

## 安装

要求 Python 3.10+。

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

开发和测试依赖：

```bash
python -m pip install -r requirements-test.txt
```

## 本地运行

准备模型密钥和租户配置：

```bash
export TRPC_SERVICE_MODEL_API_KEY='<model-api-key>'
export TRPC_SERVICE_TENANTS_CONFIG="$PWD/deploy/tenants.yaml"
export TRPC_SERVICE_ADMIN_API_KEY='<admin-api-key>'
export TRPC_SERVICE_TENANT_CONFIG_ENCRYPTION_KEY='<stable-random-secret>'
```

直接运行 Gateway：

```bash
trpc-service --host 0.0.0.0 --port 8080
```

也可以不安装命令入口：

```bash
python -m trpc_service._cli --host 0.0.0.0 --port 8080
```

主要端点：

- `GET /healthz`：健康检查；
- `POST /webhook/{tenant_id}/{channel}`：IM webhook；
- `GET /admin/ui`：管理页面；
- `/admin/*`：租户、审计和指标管理 API。

真实 webhook 必须使用相应 IM 平台的签名和消息格式。

## Docker Compose

最小环境包括 Gateway、Worker、Redis 和 MySQL：

```bash
./start.sh
curl --fail http://127.0.0.1:8080/healthz
./stop.sh
```

`stop.sh` 默认保留命名卷。如果只需要低流量单进程验证，可关闭 Redis Streams Worker：

```bash
TRPC_SERVICE_QUEUE_ENABLED=0 docker compose \
  -f deploy/docker-compose.minimal.yml \
  up --build gateway
```

生产环境推荐将 Gateway 与 Worker 分开扩缩容，并使用高可用 Redis、MySQL、Qdrant、S3 兼容对象存储、Secret 管理和 OpenTelemetry Collector；完整 Kubernetes 方案见[部署文档](docs/enterprise/DEPLOYMENT.md)。

## 开发与验证

```bash
./format.sh
./lint_flake8.sh
./coverage.sh
./build.sh
```

提交前还必须检查增量覆盖率：

```bash
diff-cover coverage.xml --fail-under=85
```

完整服务层 CI 可运行：

```bash
SERVICE_PYTHON=/path/to/python ./scripts/service_ci.sh
```

`coverage.sh` 对 `trpc_service` 执行 95% 总行覆盖率门禁；`diff-cover` 对本次变更执行至少 85% 的增量覆盖率门禁。

## 构建与依赖边界

```bash
./build.sh
python -m pip install dist/trpc_agent_service-*.whl
python -c "import trpc_agent_sdk, trpc_service; print('ok')"
```

构建产物只包含 `trpc_service`。安装 wheel 时，包管理器会根据 `pyproject.toml` 自动安装兼容版本的 `trpc-agent-py`，避免本项目长期维护一份容易与上游分叉的 SDK 副本。
