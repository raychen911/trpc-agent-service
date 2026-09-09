# 测试说明

所有命令都在项目根目录运行：

```bat
cd /d F:\wfy\Git\trpc-agent-service
```

本地测试使用 OfflineModel 和内存实现，适合快速验证业务逻辑，运行过程不产生模型调用费用。

## 1. Windows 临时目录权限

如果测试出现下面的错误：

```text
PermissionError: C:\Users\你的用户名\AppData\Local\Temp\pytest-of-...
```

在当前 Anaconda Prompt 执行：

```bat
mkdir "%USERPROFILE%\trpc-pytest-temp" 2>nul
set "TEMP=%USERPROFILE%\trpc-pytest-temp"
set "TMP=%USERPROFILE%\trpc-pytest-temp"
```

设置后，pytest 会把临时文件写到 `%USERPROFILE%\trpc-pytest-temp`。命令中的 `-p no:cacheprovider` 会关闭 `.pytest_cache`。这些环境变量在当前 Anaconda Prompt 中生效；临时目录会保留，后续测试可以继续使用。

## 2. 一次运行全部离线测试

```bat
python -m pytest -m "not integration and not live" -p no:cacheprovider -vv
```

测试内容：配置、多租户隔离、Session/Memory、Summary、锁、幂等、队列、Outbox、微信客服、企业微信、Artifact、Tool、审计和 Trace。

预期结果：

```text
145 passed, 22 deselected
```

`22 deselected` 表示本次命令只选择了本地测试，数据库集成测试和 Live 条件检查留给后续对应命令运行。

如果结果是 `97 passed, 5 errors`，而且错误都指向 `tmp_path` 或 `pytest-of-用户名`，说明问题仍是临时目录权限。重新执行第 1 节的命令。

## 3. 按模块运行 pytest

下面的命令使用本地组件完成验证。

| 运行命令 | 测试模块 | 预期结果 |
|---|---|---|
| `python -m pytest tests/test_config.py -p no:cacheprovider -vv` | YAML、环境变量、`.env` 和 Secret 引用 | `6 passed` |
| `python -m pytest tests/test_tenant_registry.py -p no:cacheprovider -vv` | 配置发布、版本和回滚 | `2 passed` |
| `python -m pytest tests/test_identity.py -p no:cacheprovider -vv` | 用户、单聊和群聊身份生成 | `2 passed` |
| `python -m pytest tests/test_runtime_factory.py -p no:cacheprovider -vv` | SDK Runner、Agent 和租户命名空间装配 | `1 passed` |
| `python -m pytest tests/test_worker_web.py -p no:cacheprovider -vv` | Web Chat 和 Admin 鉴权 | `2 passed` |
| `python -m pytest tests/test_guard.py -p no:cacheprovider -vv` | 同一 Session 串行锁和等待超时 | `2 passed` |
| `python -m pytest tests/test_idempotency_queue_outbox.py -p no:cacheprovider -vv` | 幂等、队列重试和 Outbox | `4 passed` |
| `python -m pytest tests/test_v3_reliability.py -p no:cacheprovider -vv` | 请求状态、修复扫描和乱序记录 | `6 passed` |
| `python -m pytest tests/test_channels.py -p no:cacheprovider -vv` | Telegram 消息解析 | 全部 `PASSED` |
| `python -m pytest tests/test_v3_channels_migration.py -p no:cacheprovider -vv` | Telegram、企业微信 Fake 和迁移状态机 | 全部 `PASSED` |
| `python -m pytest tests/test_im_visual_demo.py -p no:cacheprovider -vv` | 三类 IM 的真实形状协议、附件、故障与页面闭环 | `13 passed` |
| `python -m pytest tests/test_real_storage_migration.py -p no:cacheprovider -vv` | 迁移协议、方向校验、真实双向迁移、双写、旧目标隔离和双向回滚 | 配好数据库后 `13 passed` |
| `python -m pytest tests/test_filters_tools.py -p no:cacheprovider -vv` | Filter、Tool 白名单和确认配置 | `3 passed` |
| `python -m pytest tests/test_v3_resources_governance.py -p no:cacheprovider -vv` | Artifact、Knowledge、审批、脱敏和副作用防重 | `4 passed` |
| `python -m pytest tests/test_v3_trace.py -p no:cacheprovider -vv` | 队列中的 Trace Context 传播 | `1 passed` |
| `python -m pytest tests/test_v3_resumption.py -p no:cacheprovider -vv` | SDK 完整链路、并发、故障恢复和离线 Demo | `47 passed` |
| `python -m pytest tests/test_customer_service.py -p no:cacheprovider -vv` | 微信客服加密回调、分页、附件、接管和发送异常 | `14 passed` |
| `python -m pytest tests/test_session_recovery.py -p no:cacheprovider -vv` | Summary、Memory、群聊隔离、锁代次和幂等恢复 | `14 passed` |

只检查微信客服和会话恢复：

```bat
python -m pytest tests/test_customer_service.py tests/test_session_recovery.py -p no:cacheprovider -vv
```

预期得到 `28 passed`。

## 4. 运行 Demo 看输出

```bat
python -m trpc_service._cli demo all
```

预期看到 16 行 `[PASS]`。这些 Demo 使用内存后端和 Fake 模型，程序退出后数据消失。

| 单独运行的命令 | 测试内容 | 预期结果 |
|---|---|---|
| `python -m trpc_service._cli demo config --json` | 配置发布和回滚 | `active_version=1`，保留版本 1、2 |
| `python -m trpc_service._cli demo isolation --json` | 两个租户使用相同外部用户 | 内部用户 ID 不同 |
| `python -m trpc_service._cli demo storage --json` | 内存 Session 恢复 | `session_restored=true` |
| `python -m trpc_service._cli demo session --json` | 20 个同 Session 请求 | 最大并发为 1 |
| `python -m trpc_service._cli demo reliability --json` | 重复请求幂等 | 重复请求返回原 request ID |
| `python -m trpc_service._cli demo channels --json` | IM 统一消息协议 | 输出 `NormalizedInboundMessage` |
| `python -m trpc_service._cli demo artifacts --json` | 文件和 Knowledge 隔离 | 本租户可读，跨租户结果为 0 |
| `python -m trpc_service._cli demo governance --json` | 审批和脱敏 | 审批状态为 `used`，Secret 显示为 `***` |
| `python -m trpc_service._cli demo telemetry --json` | 指标和标签限制 | 输出请求指标，并验证标签采用低基数字段 |
| `python -m trpc_service._cli demo migration --json` | 迁移状态和 checkpoint | 最终为 `completed`，源和目标 hash 相同 |
| `python -m trpc_service._cli demo e2e --json` | 入站到 Fake 投递 | `state=succeeded`、`model_calls=1`、`delivered=1` |
| `python -m trpc_service._cli demo customer-service --json` | 微信客服重复通知到回复 | 两次通知只回复一次，cursor 为 `cursor-1` |
| `python -m trpc_service._cli demo summary --json` | 摘要和 Memory | `summary_events=1`，三轮均为 `memory_done` |
| `python -m trpc_service._cli demo group-session --json` | 群共享会话 | 第二个成员看到两轮群历史 |
| `python -m trpc_service._cli demo session-lock --json` | 20 个任务竞争锁 | 最大并发为 1，代次为 1～20 |
| `python -m trpc_service._cli demo idempotency-recovery --json` | 入队失败后的修复 | 只创建一个请求，修复一个任务 |

Demo 通过表示本地业务链路正常。真实数据库、模型和 IM 账号分别使用后续 Integration 与 Live 命令验证。

## 4.1 本地可视化验证三类 IM

```bat
python -m trpc_service._cli im-demo --config examples\config\im-demo.yaml --env-file .env
```

打开 `http://127.0.0.1:8080/im`，依次选择企业微信、微信客服和 Telegram，模型保持默认 Fake Model。发送后应看到任务变成 `succeeded`、Outbox 变成 `delivered`，并能查看脱敏原始消息、统一消息以及完整执行阶段。勾选“发送相同消息两次”时，应显示重复请求已复用。

只运行对应自动测试：

```bat
python -m pytest tests\test_channels.py tests\test_customer_service.py tests\test_v3_channels_migration.py tests\test_im_visual_demo.py -p no:cacheprovider -vv
```

这组测试使用模拟客户端和 OfflineModel，验证三类官方形状的消息字段、认证、附件下载入库、幂等、Queue、Runner、Outbox、Fake Delivery，以及限流、发送超时和人工接管。正确结果是全部 `PASSED`。账号权限和公网收发由 Live 命令继续验证。

## 4.2 统一验证真实 IM 账号

先复制 `.env.example` 为 `.env`，填写已有通道的真实测试凭据。只验证企业微信时运行：

```bat
python -m trpc_service._cli demo im-live --env-file .env --channels wecom --confirm --json
```

三个通道都具备测试账号时运行：

```bat
python -m trpc_service._cli demo im-live --env-file .env --channels all --confirm --json
```

| 通道 | 实际测试内容 | 通过标志 |
|---|---|---|
| `wecom` | 使用真实 BotID 和 BotSecret 建立长连接并等待认证 | `status=passed`、`authenticated=true` |
| `wecom-kf` | 获取真实 access token，检查客服状态并向测试客户发送消息 | `status=passed`、`delivered=true` |
| `telegram` | 调用真实 Bot API 向测试 Chat 发送消息 | `status=passed`、`delivered=true` |

命令会真实联网，微信客服和 Telegram 会发送测试消息，因此 `.env` 中填写验收环境的接收人。全部通道通过时退出码为 0；任一通道缺少凭据或请求失败时退出码为 1，同时 JSON 会保留其他通道的检查结果。该入口由 `--confirm` 显式触发，`pytest` 和 `demo all` 使用本地模拟客户端。

## 5. 测试真实 Redis 和 PostgreSQL

先启动测试数据库：

```bat
docker compose -f docker-compose.test.yml up -d
docker compose -f docker-compose.test.yml ps
```

服务显示 `healthy` 后设置地址：

```bat
set "TRPC_TEST_REDIS_URL=redis://127.0.0.1:16379/0"
set "TRPC_TEST_POSTGRES_URL=postgresql://trpc_agent:test@127.0.0.1:15432/trpc_agent_test"
```

运行：

```bat
python -m pytest -m integration -p no:cacheprovider -vv
```

| 测试文件 | 测试内容 | 环境正常时的结果 |
|---|---|---|
| `tests/test_integration_redis.py` | Redis 幂等、Streams、重试、锁、预算和旧 Consumer 清理 | `5 passed` |
| `tests/test_integration_postgres.py` | PostgreSQL 配置、Usage、审批和 Outbox | `1 passed` |
| `tests/test_native_fencing_integration.py` | 旧代次拒写、双进程 Session/Memory、崩溃恢复和 PostgreSQL 原子事务 | `7 passed` |
| `tests/test_real_storage_migration.py` | Redis 与 PostgreSQL 双向迁移、双写和回滚 | `5 passed`（另有 8 项离线测试） |

全部通过时会得到 `18 passed`。其中迁移测试验证 Redis→PostgreSQL、PostgreSQL→Redis、迁移期间双写、20 个回填期新增 Session、旧目标隔离及双向观察期回滚。这些测试使用真实 Redis/PostgreSQL 和本地模型；Docker 服务或测试 URL 缺失时显示 `skipped`。

只测试迁移模块：

```bat
python -m pytest tests/test_real_storage_migration.py -p no:cacheprovider -vv
```

预期 `13 passed`。如果提示缺少迁移表，先把 `migrations\006_real_storage_migration.sql` 和 `migrations\007_reverse_storage_migration.sql` 应用到专用测试库。

## 6. live 测试

```bat
python -m pytest -m live -p no:cacheprovider -vv
```

4 项 Live 用例分别检查模型、企业微信、微信客服和 Telegram 的环境变量。没有填写凭据时显示 `skipped`，凭据齐全时通过检查。

这里的 `passed` 表示凭据字段检查通过。真实模型或 IM 的连接与收发结果由对应 Live 命令输出，微信客服联调方法见 [customer-service.md](customer-service.md)。

## 7. 覆盖率和静态检查

```bat
python -m pytest -m "not integration and not live" -p no:cacheprovider --cov=trpc_service --cov-report=term
python -m flake8 trpc_service tests examples
python -m compileall -q trpc_service tests
```

覆盖率命令应得到 `145 passed, 22 deselected`。后两条命令成功时通常没有输出。

## 8. 结果怎么看

| 输出 | 含义 |
|---|---|
| `PASSED` | 测试通过 |
| `FAILED` | 断言不符合预期，检查业务代码 |
| `ERROR` | 测试没正常开始，通常是环境、依赖或权限问题 |
| `SKIPPED` | 当前环境缺少该测试需要的数据库或凭据 |
| `DESELECTED` | 当前 `-m` 条件没有选择该测试 |

报告问题时，提供运行命令、最后的汇总行和第一个完整错误；敏感配置使用脱敏后的字段名和错误类型。
