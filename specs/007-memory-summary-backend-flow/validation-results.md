# 第七阶段验证记录

## 验证记录格式（T002）

每个任务使用以下字段记录，不写入 DSN、密码、Token、response URL、原始用户内容或
完整对象引用：

| 日期 | 任务 | 关联 FR/NFR/SC | DEC | 环境（脱敏） | RED 命令/退出码/结果 | GREEN 命令/退出码/结果 | 证据与备注 |
|---|---|---|---|---|---|---|---|
| YYYY-MM-DD | Txxx | FR-xxx | DEC-xxx | Python/后端状态 | command; code; counts | command; code; counts | stable error/skip reason |

方法：先 RED、后 GREEN；共享 Redis/PostgreSQL 未配置时记录 skip，不伪装通过。旧骨架
测试结果只作为历史事实保留，不作为本 `tasks.md` 的 T001–T121 验收证据。

## T001 基线清单

- 日期：2026-09-10。
- 分支：`feature/luwenjie`。
- 最新提交：`5331c41 feat: complete dual IM real channel flow`。
- 工作区：已有第五阶段及第六阶段未提交工作树变更；本阶段不覆盖或重置这些变更。
- Python：`3.12.7`；项目虚拟环境可执行文件：`.venv\\Scripts\\python.exe`。
- 关键依赖：`trpc-agent-py==1.1.19`、`pydantic==2.13.5`、`sqlalchemy==2.0.52`、
  `asyncpg==0.31.0`、`redis==8.1.0`、`pytest==9.1.1`。
- 现有 Data 骨架：`trpc_service/storage/data_models.py`、`memory.py`、`sync.py`；
  既有共享边界：`contracts.py`、`shared.py`、`postgres/database.py`、
  `postgres/models.py`、`redis_*`；新增 `tests/unit/data`、`contract/data`、
  `integration/data`、`e2e/data`、`security/data` 测试包。
- 环境状态：本进程未读取或记录任何真实凭证；共享 Redis/PostgreSQL URL 未在记录中
  输出，后续由 fixture 只检查是否存在。

## T003–T005 Setup 证据

- T003：五个分层测试包及 `__init__.py` 已创建；pytest collection 通过。
- T004：`tests/data_support.py` 提供双租户、双节点、固定 UTC、稳定 trace/fence
  digest 和规范化 fixture，不读取真实 Secret。
- T005：`tests/conftest.py` 注册 `data_shared_backend` marker，提供唯一
  `data_namespace`，共享 URL 缺失时只返回明确 skip 原因且不回显 DSN。

## T006–T010 Foundational RED

- 日期：2026-09-10；命令：`.venv\\Scripts\\python.exe -m pytest -q
  tests/unit/data/test_canonical_and_models.py tests/unit/data/test_data_errors.py
  tests/contract/data/test_repository_ports.py tests/integration/data/test_schema_upgrade.py
  -p no:cacheprovider`。
- 结果：`8 failed`，退出码 1。
- 失败均为预期的新能力缺失：canonical/digest、DataScope、v6 数据模型、稳定 data
  errors、异步数据端口和 schema v6 尚未实现；没有导入错误、网络错误或凭证错误。
- T010 RED 结论：有效 RED 已固定，允许进入 T011–T015 实现；共享数据库测试尚未
  连接真实后端，后续环境缺失只能单独记录 skip。

## 当前结果

- T002–T003 RED tests cover immutable tenant-scoped values, event idempotency, monotonic
  sequence and summary watermarks.
- T004–T008 GREEN: `5 passed` across model, repository and cross-node visibility tests.
- The InMemory reference adapter is deterministic and external-backend boundaries are
  explicit; no credentials or network calls are used.

## T011–T016 Foundational GREEN

- 日期：2026-09-10；精确 RED 命令重跑结果：`8 passed`，退出码 0。
- canonical JSON/digest、UTC/immutable data models、稳定 domain error envelope、
  异步 phase-seven ports 和 v6 schema metadata 契约均通过。
- Schema 证据：`SUPPORTED_SCHEMA_VERSION=6`；`006_memory_summary.sql` 为
  forward-only/idempotent DDL，未包含 `DROP TABLE`。真实 PostgreSQL 未在本进程
  配置，因此没有把共享数据库初始化或迁移写成 PASS。
- 本地完整回归：`296 passed, 28 skipped, 0 failed`，跳过项均因共享 Redis/
  PostgreSQL URL 未配置；未回显 DSN、凭证或原始内容。

## Selected US1–US8 deterministic implementation evidence

- 日期：2026-09-10。Data foundation/tenant/event/memory/summary/fixture/migration/
  recovery/audit targeted tests were run after each implementation group.
- Latest phase-seven full local result: `347 passed, 30 skipped, 0 failed`; skips are
  limited to tests requiring `TRPC_SHARED_REDIS_URL` or `TRPC_SHARED_DATABASE_URL`.
- Deterministic ObjectStore/VectorStore fixtures passed tenant isolation, digest
  verification, idempotent delete/upsert and pre-filter-before-scoring checks. They
  are explicitly fixtures, not claims about a real vendor service.
- At this intermediate checkpoint, the migration state machine passed scoped
  pause, checkpoint, authority cutover, rollback boundary and forward-repair
  checks in memory; real Redis/PostgreSQL evidence was still pending and was
  subsequently completed in the final sections below.
- Recovery and audit tests passed stable error, generation fencing, audit fail-closed,
  metadata-only diagnostics and trace propagation checks. No Secret/DSN/raw payload
  was written to this record.
- 当前进程环境仅观察到本机 `6379/5432` 可连接，但
  `TRPC_SHARED_REDIS_URL` 与 `TRPC_SHARED_DATABASE_URL` 均未注入；因此没有执行或
  宣称真实共享后端 PASS，也没有尝试猜测凭证。

## Historical gate snapshot (subsequently resolved)

- 当时 `tasks.md` 已勾选 29/121；其余任务没有被批量伪标记。
- 当时尚未完成的高风险门禁包括真实 PostgreSQL v5→v6 原地升级、
  Redis/PostgreSQL 双节点可见性与事务故障注入、真实迁移 authority cutover、
  双 Worker/双 IM 纵向 E2E，以及最终无 skip 的 T119–T121；这些门禁均已在
  后续记录中完成。
- 这些门禁需要同一安全 PowerShell 进程提供共享 URL；本记录不保存 URL、密码、
  Secret、response URL 或原始业务内容。
- 2026-09-10 本地 `.env.local` 试运行：schema 初始化退出码 1，PostgreSQL
  返回 `InvalidPasswordError`，Redis 返回 `AuthenticationError`。这属于现有
  Docker 数据卷凭证不匹配，不能归因于第七阶段代码，也未继续宣称共享测试通过。

## Targeted GREEN details

- Event/Memory/Summary contract group: `7 passed`; Event replay/conflict and
  sequence-gap behavior, Memory CAS/size gate, and Summary watermark/replay/conflict
  behavior passed locally.
- Artifact/Knowledge fixture group: `9 passed`; temporary object tenant scope,
  digest readback, idempotent cleanup, vector pre-filter and audit gate passed.
- Migration group: `5 passed`; scoped pause, snapshot/checkpoint, cutover,
  generation CAS and rollback/forward-repair boundary passed in deterministic memory
  fixtures. No real Redis/PG authority evidence is claimed.
- Recovery/Audit/Security group: `10 passed`; fencing, recovery decision matrix,
  fail-closed raw reads, low-detail operational diagnostics and sensitive marker
  digest handling passed.
- Final deterministic additions: SDK data-object compatibility, InMemory p95,
  dual-tenant dual-channel flow and checkpoint recovery: `4 passed`.

## 2026-09-10 shared-backend and final-gate evidence

- `.env.local` was read only into the current PowerShell process. It is ignored by
  `.gitignore`; no password or DSN value was written to this file, Git, logs, or
  this record.
- Using the repository-defined Compose credentials and DSN shape
  (`trpc_agent` / `trpc_agent`), `python -m trpc_service._cli shared-init`
  completed with exit code `0`. PostgreSQL schema initialization and Redis
  connectivity were therefore verified against the running local services.
- Shared data contract/integration run:
  `tests/contract/data tests/integration/data -p no:cacheprovider` → `40 passed,
  0 failed`, exit code `0`.
- Explicit shared-backend marker run (`-m data_shared_backend`) → `2 passed,
  38 deselected`, exit code `0`. The earlier quickstart alias `shared_backend`
  is not the registered marker; the registered marker is `data_shared_backend`.
- Full repository regression (`pytest -q -p no:cacheprovider`) → `377 passed,
  0 failed`, exit code `0`; no tests were skipped in this credentialed run.
- `git diff --check` completed with exit code `0`. The literal values loaded from
  `.env.local` had `0` matches in the repository, and `.env.local` is confirmed
  ignored. Source scans found only documented environment-variable names and
  placeholder examples, not credentials, tokens, response URLs, or raw content.
- Initial Docker CLI health inspection was unavailable to the Codex process
  because the Docker named pipe returned `permission denied`. This limitation
  was subsequently resolved by running the isolated validation from the user's
  administrator PowerShell; the final evidence is recorded below.

## 2026-09-10 final isolated Docker validation (T116/T119)

- A uniquely named temporary Compose project (`trpc-agent-v7-<timestamp>`) was
  used, so the existing project volumes were not deleted. The temporary
  PostgreSQL schema was placed at a clean v5 boundary and verified as
  `schema_version=5`, with `session_events=absent`.
- `trpc-agent-shared-init` completed successfully against that v5 boundary.
  Post-upgrade verification returned `schema_version=6` and confirmed
  `session_events`, `memory_records`, and `summary_records` were present.
- The previously failing Audit/recovery/two-worker scope was rerun after the
  corrected v5 fixture construction: `17 passed, 2 warnings`, exit code `0`.
  This confirmed the earlier nine failures were caused by the incorrectly
  constructed raw-SQL v5 fixture rather than separate Audit, Agent, Redis, or
  recovery defects.
- Data contract/integration suite: `40 passed`, exit code `0`.
- Registered real-backend marker (`-m data_shared_backend`): `2 passed,
  38 deselected`, exit code `0`; these two tests were executed, not skipped.
- Phase-seven data E2E suite: `3 passed`, exit code `0`.
- Full repository regression: `377 passed, 0 failed, 0 skipped, 2 warnings` in
  `32.77s`, exit code `0`.
- Docker Server Version: `29.6.2`. Both isolated services were `healthy`:
  PostgreSQL `17.11-alpine3.24` on local port 5432 and Redis
  `7.4.11-alpine3.21` on local port 6379.
- `git diff --check` returned exit code `0`. Git printed LF-to-CRLF conversion
  notices for existing working-tree files; these were warnings, not whitespace
  errors or failed checks.
- The two pytest warnings originate from the pinned third-party
  `lark-channel-sdk`: deprecated `datetime.utcfromtimestamp()` usage and event
  loop acquisition during import. They do not change the stage-seven result and
  are retained as dependency-upgrade follow-up information.
- No password, DSN, token, response URL, original content, or complete object
  reference was copied into this evidence.
- Final task status: `121/121 completed`, `0 pending`.

## Final traceability review (T121)

- FR-001–FR-034 are covered by the data model, repository, migration, recovery,
  audit, security, and full-regression suites listed above.
- SC-001–SC-009 are evidenced by the deterministic contract/E2E suites,
  credentialed shared-backend run, performance test, sensitive-value scan, and
  the README/阶段成果记录 scope declarations.
- DEC-001–DEC-005 are reflected in the fail-closed backend behavior, tenant
  predicates, migration state machine, fixture boundary, and no-secret logging
  checks. Any real vendor IM/model/vector/object-store or Kubernetes claim
  remains out of scope for this phase.
