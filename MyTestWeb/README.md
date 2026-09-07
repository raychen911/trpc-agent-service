# MyTestWeb

本地多租户 tRPC-Agent 完整实战验证台。前端展示租户、会话、Memory、工具治理、Trace、审计与 IM 故障场景；本地 FastAPI 后端调用 `trpc_service`，底层 SDK 来自 `trpc-agent-py` 依赖。

## 启动

先启动隔离端口上的 Redis 与 MySQL：

```bash
cd /home/peanut/myCode/trpc-agent-service
docker compose -f deploy/docker-compose.test.yml up -d redis mysql
```

再启动验证台：

```bash
cd /home/peanut/myCode/trpc-agent-service/MyTestWeb
npm install
chmod +x run-local.sh
./run-local.sh
```

若系统尚未安装 Python 依赖，可先在仓库根目录创建环境并安装：

```bash
cd /home/peanut/myCode/trpc-agent-service
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/pip install -r requirements-test.txt
```

`run-local.sh` 未找到 `.env.local` 时会自动连接本地测试 Docker 的 Redis（16379）和
MySQL（13306），并使用仅适用于本地验证的配置加密密钥；因此直接执行脚本也会持久化租户。

打开 `http://127.0.0.1:3000`。默认 Mock 模式不需要模型密钥；数据后端页可真实测试
Redis/MySQL 连接、保存租户级路由、回滚配置和运行 Session/Memory Redis→MySQL 迁移 dry-run。
Audit 固定持久化到 MySQL，不提供 Redis 后端选项；每个进程只保留最近 500 条内存审计记录。
设置 `MYSQL_URL` 后，租户当前配置与版本历史写入 MySQL，Redis 用作加密配置缓存和
跨节点变更通知；重启 MyTestWeb 后租户和配置仍存在。生产环境必须显式设置随机的
`TENANT_CONFIG_ENCRYPTION_KEY`，本验证台的默认值仅适用于可丢弃的本地数据。

使用 DeepSeek 时：

```bash
cp .env.example .env.local
# 在 .env.local 填写 DEEPSEEK_API_KEY
./run-local.sh
```

密钥只由 `server.py` 读取，不会进入前端构建产物、浏览器请求或日志。不要提交 `.env.local`。

## 本地 CI

```bash
npm run ci
```

该验证台只用于本地人工验证，不设置前端覆盖率门禁。`npm run ci` 用于 ESLint、生产构建
和已有后端回归测试；正式服务模块在 GitHub Actions 中设置 ≥95% 行覆盖率硬门禁。

## 本地接口

- `GET /healthz`：后端与 DeepSeek 配置状态
- `GET /api/bootstrap`：租户和能力清单
- `POST /api/tenants`：本地管理员注册租户（创建初始配置 v1）
- `POST /api/chat`：真实 `TenantWorker` 对话入口
- `GET/PATCH /api/tenants/{tenant_id}/storage`：读取/版本化更新 Redis/MySQL 路由
- `POST /api/tenants/{tenant_id}/storage/test`：真实 Redis PING / MySQL SELECT 1
- `POST /api/tenants/{tenant_id}/storage/migrate/dry-run`：迁移前连接与步骤检查
- `POST /api/tenants/{tenant_id}/storage/migrate`：真实迁移 Session/Memory，checksum 通过后切换路由
- `GET /api/tenants/{tenant_id}/storage/migrate/{job_id}`：查询迁移进度与复制数量
- `GET /api/tenants/{tenant_id}/channels`：四类 IM 配置状态（不返回密钥）
- `PATCH /api/tenants/{tenant_id}/channels/{channel}`：保存平台绑定配置
- `POST /api/im/simulate`：平台 fixture → Adapter → Worker → Redis/MySQL → 回复 Payload
- `POST /api/scenarios/isolation`：跨租户 Session 隔离检查
- `POST /api/scenarios/duplicate`：IM 重复投递模拟
- `GET /api/audit`：租户审计记录
- `GET /api/metrics`：租户级指标
