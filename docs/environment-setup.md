# 跨平台环境配置与启动

项目使用 POSIX `sh` 和 `uv` 作为统一开发入口：Linux/macOS 可使用系统 shell，Windows 使用
Git Bash 或 WSL。脚本不会依赖 PowerShell、Windows 路径或固定名称的 Conda 环境。

## 1. 安装基础工具

需要安装：

- Git；
- Python 3.10～3.13，推荐 Python 3.12；
- `uv`；
- 可选 Docker，用于 Compose 部署和 Redis 集成测试。

安装 `uv` 后确认命令可用：

```sh
git --version
python --version
uv --version
```

`uv` 的安装方式见其官方文档，或者在已有 Python 环境中运行：

```sh
python -m pip install uv
```

Windows 用户需要从 Git Bash 或 WSL 执行本文的 `sh` 命令。原生 PowerShell 仍可直接运行
`uv`、Python 和 Docker 命令，但不是本项目脚本的标准入口。

## 2. 克隆服务仓库

项目使用 PyPI 发布的 `trpc-agent-py`，不需要额外克隆或提交 `trpc-agent-python`。

```sh
git clone -b feature/lanmingang https://github.com/TimeMachineForbidden/trpc-agent-service.git
cd trpc-agent-service
```

## 3. 创建环境并安装依赖

```sh
sh bootstrap.sh
```

脚本使用 `uv.lock` 创建或更新项目自己的 `.venv`，安装开发依赖，并验证
`trpc_agent_sdk` 与 `trpc_service` 可以导入。它不会修改系统 Python 环境。

需要强制刷新缓存时：

```sh
sh bootstrap.sh --refresh
```

如果公司根证书已经安装到操作系统证书库：

```sh
sh bootstrap.sh --system-certs
```

如果公司提供 PEM 格式 CA 文件：

```sh
sh bootstrap.sh --certificate /path/to/company-ca.pem
```

使用公司 Python 镜像时，可以在当前 shell 设置：

```sh
export UV_DEFAULT_INDEX="https://公司镜像地址/simple"
sh bootstrap.sh
```

不要关闭 TLS 校验，也不要配置不安全主机。仍然失败时，应确认公司镜像、代理和 CA 配置。

## 4. 验证项目

```sh
sh test.sh
```

该脚本依次执行 pytest、Ruff lint 和 Ruff format check。未设置
`TRPC_TEST_REDIS_URL`/`TRPC_TEST_POSTGRES_URL` 时，对应的 Redis、Redis Stream、分布式锁和
PostgreSQL 集成测试会自动跳过。

其他常用入口：

```sh
sh format.sh
sh coverage.sh
sh build.sh
```

`build.sh` 会进行字节码检查并在 `.package-dist/` 生成源码包和 wheel；该目录不会提交到 Git。

## 5. 初始化数据库并启动

默认 test 环境不需要外部 Token：

```sh
sh start.sh
```

`start.sh` 会初始化 SQLite，然后以前台方式启动服务。按 `Ctrl+C` 停止。浏览器访问：

- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/ready`
- `http://127.0.0.1:8000/docs`

也可以单独运行 CLI：

```sh
uv run --frozen trpc-service init-db
uv run --frozen trpc-service show-config
uv run --frozen trpc-service serve
```

SQLite 文件默认位于 `data/trpc_agent_service.db`，该文件已被 Git 忽略。

## 6. 配置真实模型

复制非敏感配置模板：

```sh
cp .env.example .env
```

`.env` 是本地唯一配置入口，同时保存 Provider 配置、`env://` 引用目标和 IM Secret：

```dotenv
TRPC_SERVICE_APP_ENV=development
TRPC_SERVICE_MODEL_PROVIDER=openai
TRPC_SERVICE_MODEL_NAME=真实模型名称
TRPC_SERVICE_MODEL_BASE_URL=https://真实模型服务地址/v1
TRPC_SERVICE_MODEL_API_KEY_REF=env://TRPC_AGENT_API_KEY
TRPC_SERVICE_ADMIN_API_KEY_REF=env://TRPC_SERVICE_ADMIN_API_KEY
TRPC_SERVICE_SESSION_HMAC_KEY_REF=env://TRPC_SERVICE_SESSION_HMAC_KEY
TRPC_SERVICE_AGENT_TIMEOUT_SECONDS=120
TRPC_SERVICE_ARTIFACT_ROOT=./data/artifacts
TRPC_SERVICE_OTEL_CONSOLE_EXPORTER=false
TRPC_AGENT_API_KEY=真实模型密钥
TRPC_SERVICE_ADMIN_API_KEY=自行生成的管理密钥
TRPC_SERVICE_SESSION_HMAC_KEY=至少16字符的会话签名密钥
TRPC_TELEGRAM_BOT_TOKEN=真实TelegramToken
TRPC_WECOM_BOT_SECRET=真实企业微信BotSecret
```

填写后直接执行 `sh start.sh`。配置加载器读取普通设置，`SecretResolver` 使用
`python-dotenv` 解析同一文件中的 Secret 引用目标；已经由容器或 Secret 管理系统注入的环境
变量不会被 `.env` 覆盖。`.env` 已被 `.gitignore`
排除，提交前仍需运行 `git diff --cached`，确认没有真实密钥。

## 7. 启动 IM 通道

先按照 [IM 接入指南](step6-channel-setup.md)创建租户、Agent App 和 Pull Binding，并注入相应
Secret。然后在另一个 shell 中运行：

```sh
sh channels.sh
```

该前台进程维护 Telegram long polling 和企业微信 AIBot WebSocket，按 `Ctrl+C` 停止。

## 8. Docker Compose

单服务 SQLite 模式：

```sh
docker compose up --build -d
docker compose ps
curl -fsS http://127.0.0.1:8000/ready
```

停止服务：

```sh
docker compose down
```

不要添加 `-v`，否则 SQLite 和 Redis 数据卷会被删除。

独立 Gateway/Worker 模式使用 PostgreSQL 和 Redis，可启动两个 Worker：

```sh
docker compose -f compose.multi.yaml up --build -d --scale worker=2
curl -fsS http://127.0.0.1:18000/ready
```

该 Compose 文件保留一个对外 Gateway；多个 Gateway 的负载均衡由 Kubernetes Service 提供。

## 9. Kubernetes

项目包含默认 2 个 Gateway 和 2 个 Worker 的简化清单、kind 启动脚本和冒烟测试。配置与部署
步骤见 [Kubernetes 多副本部署](kubernetes-deployment.md)。Kubernetes 使用独立 Secret，不会
读取本地 `.env`，不要提交 `deploy/k8s/secret.local.yaml`。

## 10. 常见问题

### 无法安装 trpc-agent-py

确认当前网络或公司镜像能够访问 `trpc-agent-py>=1.1.20,<2`，然后运行：

```sh
sh bootstrap.sh --refresh
```

不要配置 `../../trpc-agent-python` 本地路径。

### 端口 8000 被占用

```sh
export TRPC_SERVICE_PORT="8010"
sh start.sh
```

### 配置提示需要真实 Provider 或 Secret

基础验证可临时切回 test 环境：

```sh
export TRPC_SERVICE_APP_ENV="test"
sh start.sh
```

开发、真实联调和最终演示仍必须使用真实模型与真实 IM 配置。
