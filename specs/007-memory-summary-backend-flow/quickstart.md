# 第七阶段本地开发与验收 Quickstart

本文是实施完成后的目标运行方式。当前第七阶段尚未按新 plan/tasks 实现时，部分 `tests/*/data` 路径和 schema v6 命令会先失败；这正是后续严格测试先行的 RED 起点。

## 1. 进入仓库并安装依赖

```powershell
Set-Location E:\grad_files\2026trpc-agent\trpc-agent-service-submit
uv sync
```

不要把数据库密码、Redis 密码、IM Secret 或模型 Key 写入脚本、`.env`、截图、测试报告或 Git。

## 2. 先运行纯本地模型与契约测试

```powershell
uv run pytest tests/unit/data -q
uv run pytest tests/contract/data -q -m "not data_shared_backend"
```

预期覆盖：领域模型冻结、JSON 规范化和 digest、严格 Event sequence、Memory CAS、Summary watermark/digest、Artifact 发布状态、Knowledge tenant pre-filter、迁移状态机和稳定错误。

## 3. 启动真实 Redis/PostgreSQL

在当前 PowerShell 进程中安全输入两个仅用于本地测试的密码。不要复制示例占位值作为真实密码：

```powershell
function Set-ProcessSecret([string]$Name) {
    $secureValue = Read-Host "输入 $Name" -AsSecureString
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
    try {
        [Environment]::SetEnvironmentVariable(
            $Name,
            [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer),
            "Process"
        )
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

Set-ProcessSecret "TRPC_DEMO_REDIS_PASSWORD"
Set-ProcessSecret "TRPC_DEMO_POSTGRES_PASSWORD"
```

启动容器并等待健康：

```powershell
docker compose -f deploy/local-shared/compose.yaml up -d --wait
docker compose -f deploy/local-shared/compose.yaml ps
```

只在当前进程拼接本地 DSN，不要回显变量：

```powershell
$redisPassword = [Environment]::GetEnvironmentVariable("TRPC_DEMO_REDIS_PASSWORD", "Process")
$postgresPassword = [Environment]::GetEnvironmentVariable("TRPC_DEMO_POSTGRES_PASSWORD", "Process")
$env:TRPC_SHARED_REDIS_URL = "redis://:$redisPassword@127.0.0.1:6379/0"
$env:TRPC_SHARED_DATABASE_URL = "postgresql+asyncpg://trpc_agent:$postgresPassword@127.0.0.1:5432/trpc_agent"
```

## 4. 初始化/升级 schema

```powershell
uv run trpc-agent-shared-init
```

实施完成后的预期 schema version 为 6。升级测试必须从现有 v5 数据库原地执行，不能依靠删除 volume 才通过。

## 5. 运行真实共享后端契约和集成测试

```powershell
uv run pytest tests/contract/data tests/integration/data -q
uv run pytest tests/contract/data tests/integration/data -q -m data_shared_backend
```

必须看到真实 PASS，而不是全部 skipped。重点验收：

- Event、watermark、Audit 任一点故障均全事务回滚。
- 双节点并发 Event 只有连续唯一事实提交。
- Memory/Summary 在节点 A 提交后节点 B 可见。
- 同 Summary watermark 同 digest 幂等、异 digest 冲突。
- 一个 tenant/stream 迁移停写不影响另一个。
- cutover 后首笔 PostgreSQL 新写关闭回滚，之后只允许 forward repair。
- Audit 断连时无业务写、迁移或原文返回。

## 6. Artifact 与 Knowledge fixture 验证

```powershell
uv run pytest tests/contract/data/test_object_store_fake.py -q
uv run pytest tests/contract/data/test_vector_store_fake.py -q
uv run pytest tests/integration/data/test_artifact_publication.py -q
uv run pytest tests/integration/data/test_knowledge_tenant_filter.py -q
```

这些命令只证明 Adapter 契约、孤儿清理和租户预过滤，不代表真实对象存储或真实向量数据库已经接入。

## 7. 双节点纵向链路

```powershell
uv run pytest tests/e2e/data -q
```

验收链路：可信 IM/HTTP 入站 → Gateway/Governance → Worker/官方 Runner → PostgreSQL Event/Memory/Summary/Audit → 跨节点后续会话。Worker 重启后不得依靠本地状态维持上下文。

## 8. 回归、安全与性能

```powershell
uv run pytest -q
git diff --check
```

还需运行 tasks.md 指定的敏感扫描与 InMemory p95 测试，并把命令、RED/GREEN 结果、Docker `ps`、schema version 和 skip 数量持续记录到 `validation-results.md`。报告必须区分：

- 真实 PostgreSQL/Redis：可作为共享后端验收证据。
- deterministic Object/Vector fake：只能作为契约证据。
- skipped：只说明环境未提供，不算功能通过。

## 9. 停止本地服务

```powershell
docker compose -f deploy/local-shared/compose.yaml down
```

只有确认该 volume 是可丢弃的本地测试数据时，才可显式执行 `down -v`。不要对未知或生产数据库做重置。

## 10. 常见问题

- `ConnectionRefusedError`：先检查 Docker Desktop、`docker compose ... ps` 和 5432/6379 端口。
- `InvalidPasswordError`：容器 volume 可能由旧密码初始化；优先使用创建该 volume 时的密码。仅对明确可丢弃的本地测试 volume 执行重建。
- 测试全部 skipped：检查当前 PowerShell 是否设置 `TRPC_SHARED_REDIS_URL` 和 `TRPC_SHARED_DATABASE_URL`。
- `audit_unavailable`：这是 fail-closed 设计；先恢复 PostgreSQL/Audit，再重试，不得启用本地或 Redis 业务旁路。
- `sequence_gap`：补齐缺失的连续 Event 后按原 event_id/digest 重试；Repository 不缓存乱序事件。
