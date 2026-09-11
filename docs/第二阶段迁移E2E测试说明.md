# 第二阶段：数据迁移与后端切换测试

日期：2026-09-05。

## 测试数据与真实程度

使用合成业务数据，不需要提供真实客户数据或在线模型 API Key。数据经 tRPC Session/Artifact 和平台 Knowledge 公共接口写入，不直接插入伪造数据库行。

- 基础数据集：2 个租户 × 3 个 Session × 4 轮 × 2 条 Event，共 48 条正式 Event。
- 两租户故意使用相同 user/session 字面 ID；内容包括中文、emoji、嵌套 State、稳定 event/invocation ID 和 metadata。
- 使用不同历史时间戳保持确定顺序；这些历史事件可能触发上游 stale-session 重载日志。
- 应用链路使用真实 tRPC Runner、LlmAgent、FunctionTool、可信路由、Storage Resolver、Inbox、Session 和 Outbox；仅模型响应及外部通道 Adapter 使用测试实现。
- local 模式：InMemory→真实 SQLite，Qdrant 官方嵌入式存储；MinIO 测试明确跳过。
- real 模式：真实 Redis→PostgreSQL、Local→Qdrant 服务、MinIO。缺少连接配置会失败，不能自动回退为 local。

## 首轮实测（修复前，历史记录）

首轮真实后端严格模式：**13 项，8 passed、5 failed，无 skip/xfail**。以下保留缺陷发现时的证据；最新修复、接口约束与复验结果见[已知缺陷修复与验收](已知缺陷修复与验收-2026-09-05.md)。不能将单轮迁移通过等同于第二阶段全部生产能力验收通过。

全量本地回归：89 passed、1 skipped（MinIO）、5 xfailed；flake8 通过。CI 新增 `migration-regression` job，在真实后端执行并保留已知缺陷的严格 xfail；它是回归门禁，不等同于严格验收，工作流尚未在远端执行。

真实运行复用了已有 `trpc-agent-phase-two-integration` 镜像，在临时容器目录安装 pytest 8.4.2 / pytest-asyncio 1.2.0，并挂载当前工作区代码、测试和 pyproject。未执行一次全新的上游依赖镜像构建。独立进程读取也在此容器内启动子进程完成。

| 用例 | 断言 | real 结果 |
|---|---|---|
| 批量与增量迁移 | 选定租户复制；未选定租户不受影响；重复复制无重复 ID；独立客户端读回逐字段一致 | 通过 |
| 目标提交后响应丢失 | 真实落库 3 条后注入 ConnectionError；重新连接重试不漏不重 | 通过 |
| 后端回切前反向同步 | SQL 新增第 5 轮后反向复制；源端继续第 6 轮仍保持一致 | 通过 |
| 真实应用链路切换 | 模拟回调→路由→Inbox→Runner/工具→Session→Outbox；新配置选 SQL 后模型实际收到历史上下文；反向同步再切回 | 通过 |
| 独立进程读取 | 子进程经新的 SQL Service 读取，结果与迁移前逐字段一致 | 通过 |
| 派生 Memory 重建 | 从已迁移 Session 显式重建 SQL Memory，重复处理幂等、新 Session 可见、其他租户不可见 | 通过 |
| 向量增量与 tombstone | 复制、后续新增/删除、重复补拷；逐 chunk 内容/metadata/删除状态及典型查询结果一致 | 通过 |
| MinIO 源文件版本 | 双租户同名文件两版本；重建 Service 后内容/metadata 不变；跨租户访问被拒绝 | 通过 |
| MIG-01 旧源回填 | 拒绝旧源且不得修改目标端新 State | 失败：目标 turn 从 5 退回 4 后才报校验失败 |
| MIG-02 独立 State 更新 | 最后一条 Event 后的 update_session 应保留 | 失败：目标 turn 从 99 变为历史 delta 的 4 |
| VEC-01 跨租户 chunk ID | 同 chunk ID 在两个租户同时保留 | 失败：后写覆盖前写 |
| VEC-01 跨索引 chunk ID | 同 chunk ID 在两个 index version 同时保留 | 失败：旧索引数据消失 |
| VEC-02 校验内容 | 目标正文损坏、数量不变时禁止 cutover | 失败：仅比较数量仍允许切换 |

断言使用独立 `canonical_session` 比较 Event 顺序、ID、内容、State、actions、invocation 和 metadata，不能只依赖被测迁移函数自身的 hash。连接中断和内容损坏由窄范围故障包装器注入，底层仍写真实存储。

## 运行

在仓库根目录安装项目及开发依赖后运行本地回归：

```sh
python -m pip install ".[dev]"
python -m pytest tests/e2e -o addopts= -q -ra
```

首轮用 known_gap/严格 xfail 记录已确认缺陷。修复后已经移除这些用例的 known_gap 标记；默认和严格验收均要求它们真实通过，CI 也显式启用严格验收。

严格验收把这些已知缺陷当普通失败：

```sh
python -m pytest tests/e2e --strict-acceptance -o addopts= -q
```

真实后端一键入口（Docker Desktop 运行后，在 Git Bash/WSL 中执行）：

```sh
sh e2e.sh
```

脚本使用独立 Compose 项目 `trpc-agent-migration-e2e`，构建 Dockerfile 的 e2e 阶段；退出时清理该项目容器和网络，保留数据卷。修复后严格验收应返回零退出码。镜像首次构建需要访问固定 fork 和包索引。

已有环境可直接运行：

```sh
docker compose --profile test run --build --rm e2e
```

复用已有 integration 镜像、本轮实际使用的运行方式（从仓库根目录执行）：

```sh
docker compose up -d --wait postgres redis qdrant minio
docker compose --profile test run --rm --no-deps --entrypoint sh \
  -v "$(pwd)/pyproject.toml:/app/pyproject.toml:ro" integration -c \
  'python -m pip install --quiet --target /tmp/e2e-packages pytest==8.4.2 pytest-asyncio==1.2.0 && PYTHONPATH=/app:/tmp/e2e-packages python -m pytest -o addopts= -p no:cacheprovider /app/tests/e2e --backend-mode=real --strict-acceptance -ra --tb=short'
```

real 模式所需变量由 Compose 注入：`TRPC_DATABASE_URL`、`TRPC_SESSION_DATABASE_URL`、`TRPC_REDIS_URL`、`TRPC_QDRANT_URL`、`TRPC_MINIO_ENDPOINT`、`TRPC_MINIO_ACCESS_KEY`、`TRPC_MINIO_SECRET_KEY`。Session 与平台数据库必须分开，沿用现有初始化脚本。

## 数据生命周期与验收边界

平台账本使用每个用例独有的 PostgreSQL schema，结束后清理；Qdrant 测试 collection 和 MinIO 测试 bucket 也单独创建并清理。Redis/SQL Session、派生 Memory 使用带随机 run ID 的测试 namespace，保留在测试卷中方便检查，重复运行不会共用这些业务键。

本轮测试明确不证明以下能力已完成：

- CLI Worker 自动消费、HTTP/Web IM/SSE 或真实企业微信收发。配置发布与 Pipeline 调用由测试驱动，两个配置运行实例不等于两个生产 Worker。
- 自动在线迁移、CDC/双写补偿和自动切换路由。当前验证的是回填后增量补拷，以及测试显式选择后端的应用行为。
- 回滚窗口内的自动反向同步。回切用例先显式复制新数据，再切换，不能只改版本号就宣称无损回滚。
- Summary lineage/水位迁移、独立 Memory 全量迁移、嵌入模型变化后的原文重建、迁移 Coordinator 进程崩溃后 checkpoint 恢复。
- 生产容量、持续并发写入时的一致性、真实主进程崩溃接管或副作用工具 effectively-once。

五个失败用例对应的四类缺陷已修复并通过真实后端复验，当前迁移明确要求目标停写，详见修复报告。下一步为上述未覆盖边界补充独立验收。
