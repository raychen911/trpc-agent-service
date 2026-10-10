# 数据迁移与回滚

`trpc_service.storage.migration.MigrationCoordinator` 将 Redis→PostgreSQL、Local Vector→pgvector/远端
向量等迁移统一为显式逐租户状态机。Source 提供稳定排序的 canonical `MigrationRecord`，Target 提供
幂等 upsert/read 和切换 hook，CheckpointStore 每批保存 cursor、count、滚动 SHA-256 和差异。

## 生产报告门禁

`runs/multitenant/migration-live.json` 只有在真实迁移完成后才可能被 release gate 接受为生产证据。
单独执行 `prepare`、`backfill`、`shadow-read` 或仅修改 `production_gate` 都会保持 `not_run`。生产报告
必须由 `scripts.migrate_data` 在当前 checkout 生成，并在 `migration_evidence` 中提供以下不可省略的内容：

- `status=pass`、`run_id` 与顶层 `run_id`/当前 evidence 一致；
- `scope=production`，且不得标记为 `is_simulation`/`is_test`；source 和 target 都必须显式为
  `is_real=true`；
- `source` 为 `redis`、`target` 为 `postgresql`，两者使用不同的 endpoint SHA-256 身份；source 还必须
  提供不可变 snapshot、source count 和 checksum；报告禁止写入 DSN、密码或 token；
- `manifest` 固定 tenant/migration/app/config/binding revision，source kind、kinds、snapshot、count 和
  checksum 必须与 source 一致；测试、模拟、fixture 和 acceptance 租户不得通过；
- `phases` 完整包含 `prepare → backfill → shadow-read → dual-write → cutover → verify → cleanup`，每一项
  都必须是 `gate=pass`、差异为空，并提供与该阶段相符的 control state；rollback 不作为静态模拟阶段，必须由
  `control.rollback_supported=true` 证明；
- `control.complete=true`、`rollback_supported=true`、阶段数与上述执行阶段一致，并且控制工厂是生产实现；
- `operator_confirmation` 必须是 `confirmed`，使用显式 CLI/环境确认方法，包含经过 SHA-256 处理的操作者和
  变更单标识及带时区的确认时间。
- `lineage` 必须确认当前 checkout、producer 为 `scripts.migrate_data`，并与 evidence 的 source/runtime
  fingerprint 及本次真实运行的非占位 `sha256:<64 hex>` 镜像摘要一致。

这些要求由 `scripts/release_gate.py` 重新验证，不能由迁移脚本自行声明绕过。历史 fingerprint、相同源/目标
endpoint、缺失控制钩子或只有离线验收的数据均保持 `not_run`。

阶段必须依次执行：

1. `prepare`：建目标索引/权限，确认容量与 embedding dimension。
2. `backfill`：小批幂等复制；每批 checkpoint，进程重启从 cursor 续传。
3. `shadow-read`：在线仍读旧后端，同时抽样/全量比较目标，差异不阻止继续调查。
4. `dual-write`：权威写先提交，再投影新旧后端；失败进入 Outbox，不让部分成功伪装完成。
5. `cutover`：按 tenant 原子切换 immutable storage profile/revision。
6. `verify`：重新计算 count/checksum/逐记录差异；存在差异则门禁失败。
7. `cleanup`：观察窗口结束后关闭双写并清理旧投影。
8. `rollback`：任意阶段可调用，恢复旧 profile 并关闭双写；已复制数据保留到安全 TTL 后再删。

示例（后端适配器由部署代码注入，凭证使用 SecretRef）：

```python
coordinator = MigrationCoordinator(source, target, checkpoint_store, batch_size=500)
result = await coordinator.run(tenant_id, "redis-to-pg-202608", MigrationPhase.BACKFILL)
```

## Redis→PostgreSQL 实际适配器

仓库提供 `RedisMigrationSource`、`PostgresMigrationTarget` 和
`PostgresMigrationCheckpointStore`。默认读取
`trpc:projection:session:{tenant_id}:{session_id}` 会话投影和
`trpc:memory:{tenant_id}:{resource_id}` JSON/hash 记录；目标写入 `sessions`、
`session_events` 和 `memories`。会话事件没有旧系统的 inbound/turn 外键时，适配器只创建
确定性、无正文的迁移 turn，保留事件序号、ID、作者、时间、事件 JSON 与 state delta；
目标租户必须已有对应 app 配置版本和启用的 channel binding。Redis key 中的租户前缀只用于
隔离和发现，不会被误写入目标 `session_id`/`memory_id`。

真实运行入口是 `scripts/migrate_data.py`。默认只写入 `gate: not_run` 报告；只有设置
`TRPC_RUN_REAL_MIGRATION=1`，并提供 `TRPC_MIGRATION_SOURCE_REDIS_URL`、
`TRPC_MIGRATION_TARGET_DATABASE_DSN`、`TRPC_MIGRATION_TENANT_ID`、
`TRPC_MIGRATION_ID`、`TRPC_MIGRATION_APP_ID`、`TRPC_MIGRATION_APP_REVISION`、
`TRPC_MIGRATION_CONFIG_VERSION`、`TRPC_MIGRATION_BINDING_ID`、
`TRPC_MIGRATION_BINDING_REVISION` 和 `TRPC_MIGRATION_OWNER_ID` 才会连接外部服务。
可用 `--phase prepare|backfill|shadow-read|verify`
逐阶段执行；报告只包含数量、checksum、差异和错误类型，不写入源数据或凭证。默认入口只具备
真实 source/target 的 prepare、backfill 和 shadow-read；dual-write、cutover、cleanup、rollback
必须注入部署侧 `MigrationControl`，没有控制钩子时会失败关闭，绝不会把无操作切换报告成通过。

运行前先将数据库升级到 Alembic `head`，其中 `0002_migration_checkpoint_cursor` 保存每批
cursor，进程终止后可以从最近 checkpoint 继续。双写、cutover、cleanup 和 rollback 的
存储 profile 切换仍需部署侧控制面 hook；适配器不会静默修改租户配置。

每阶段结果先序列化到 `runs/multitenant/*.json`，字段包含 baseline、candidate、case_deltas、gate 和
rejection_reasons。不要在报告中写 source payload。切换前必须保存当前 config/storage profile revision；
回滚以 revision 指针恢复，而不是反向覆盖新数据。向量迁移还要固定模型/维度/归一化参数，模型变化应
作为重新索引而非 checksum 相同的数据搬运。

## 离线验收与可回滚运行手册

先运行仓库内的隔离双租户验收。它使用确定性的 Redis 形状 source 和 PostgreSQL 形状 target，
不连接外部服务、不使用生产租户，也不会清理任何真实数据：

```powershell
.venv\Scripts\python.exe scripts\migration_acceptance_gate.py `
  --output runs\multitenant\migration-acceptance.json
.venv\Scripts\python.exe -m pytest -q tests\unit\test_migration_acceptance.py
```

报告必须同时满足 `gate=pass`、`case_deltas.cross_tenant_key_collisions=0`、
`checkpoint_resume=pass`、`expected_verify_drift=pass`、`cleanup=pass` 和 `rollback=pass`。
该报告的 `production_gate` 固定为 `not_run`，不能把模拟结果升级成生产通过。

真实 Redis→PostgreSQL 迁移必须使用一次性、不可复用的迁移租户和 migration ID。迁移账号只用于
Alembic/目标初始化，运行账号必须是 tenant-scoped `trpc_runtime`（禁止使用具备 `BYPASSRLS`
的 `trpc_worker`），且不能是表 owner；source Redis、target PostgreSQL 必须是明确的独立
地址，不能把生产 Redis 当成“源备份”直接写回同一个数据库。示例（凭证应由 SecretProvider 注入，
不要写进 shell 历史）：

```powershell
$env:TRPC_RUN_REAL_MIGRATION = "1"
$env:TRPC_MIGRATION_SOURCE_REDIS_URL = "redis://<isolated-source>"
$env:TRPC_MIGRATION_TARGET_DATABASE_DSN = "postgresql+asyncpg://<runtime>@<isolated-target>/<db>"
$env:TRPC_MIGRATION_TENANT_ID = "migration-acceptance-<date>-<random>"
$env:TRPC_MIGRATION_ID = "migration-acceptance-<date>-<random>"
$env:TRPC_MIGRATION_APP_ID = "<existing-app-id>"
$env:TRPC_MIGRATION_APP_REVISION = "<existing-app-revision>"
$env:TRPC_MIGRATION_CONFIG_VERSION = "<existing-config-version>"
$env:TRPC_MIGRATION_BINDING_ID = "<existing-binding-id>"
$env:TRPC_MIGRATION_BINDING_REVISION = "<existing-binding-revision>"
$env:TRPC_MIGRATION_OWNER_ID = "migration-operator-<random>"
$env:TRPC_MIGRATION_EXPECTED_RECORDS = "1"

.venv\Scripts\python.exe scripts\migrate_data.py --phase prepare `
  --output runs\multitenant\migration-live-prepare.json
.venv\Scripts\python.exe scripts\migrate_data.py --phase backfill `
  --output runs\multitenant\migration-live-backfill.json
.venv\Scripts\python.exe scripts\migrate_data.py --phase shadow-read `
  --output runs\multitenant\migration-live-shadow-read.json
```

每一步都要检查 JSON 中的 `source_count`、`target_count`、`checksum` 和 `differences`，并把
`migration_checkpoints` 中的 `cursor`、批次数量和 checksum 保存为变更记录。发生中断时只重新运行
同一 `tenant_id`/`migration_id` 的同一阶段；不要换 ID，也不要手工删除 checkpoint。这样会从最后一次
成功批次继续，重复 upsert 仍保持幂等。需要从别的租户迁移时必须新建 tenant 和 migration ID，禁止
复用游标。

`scripts/migrate_data.py` 在未注入部署侧控制钩子时，会对 `dual-write`、`cutover`、`cleanup`
和 `rollback` 故意失败关闭。这是安全门，不是可忽略的错误。生产切换必须使用下述已实现的
`trpc_service.storage.production_migration_control:create` 控制面适配器，并按以下顺序保存审计证据：

生产完整验收必须显式确认操作者、变更单、镜像摘要和部署控制工厂；单阶段命令不会执行控制面
操作。使用同一份 source snapshot 和 migration ID 运行完整状态机：

```powershell
$env:TRPC_MIGRATION_PRODUCTION_CONFIRMATION = "I_UNDERSTAND_REAL_MIGRATION"
$env:TRPC_RELEASE_ID = "release-<candidate-id>"
$env:TRPC_RELEASE_NONCE = "<one-time-release-nonce-at-least-32-chars>"
$env:TRPC_MIGRATION_OPERATOR_ID = "<operator-id>"
$env:TRPC_MIGRATION_CHANGE_TICKET = "<change-ticket>"
$env:TRPC_MIGRATION_IMAGE_DIGEST = "sha256:<deployed-image-digest>"
$env:TRPC_MIGRATION_CONTROL_FACTORY = "trpc_service.storage.production_migration_control:create"
.venv\Scripts\python.exe scripts\migrate_data.py --production-confirm `
  --output runs\multitenant\migration-live.json
```

命令只有在 control hook 实际报告双写、原子切换、清理和回滚状态，且每阶段 count/checksum 对账
通过后才会写入 `production_gate=pass`；缺少任一 hook、租约、快照或外部后端时保持 `not_run`/`fail`。

完整验收入口 `scripts/migration_full_acceptance.py` 仅接受专用的
`migration-acceptance-*` 租户，并在写入前对 PostgreSQL 目标租户执行一次只读空表预检。
它会在两个分支（回滚分支和最终清理分支）共用同一份 Redis source snapshot；source count 或
滚动 SHA-256 在运行期间发生变化时，整个验收失败关闭。报告只记录 snapshot 元数据以及 source
和 target 的 endpoint SHA-256，不记录 URL、payload 或凭证。该入口的 `production_gate` 始终为
`not_run`，因为它是隔离测试租户验收，不是生产租户批准。

### 生产 canary scope 一次性准备

`scripts/migration_acceptance_bootstrap.py` 永远只接受
`migration-acceptance-*` 测试租户，不能用来准备正式迁移。要在隔离的真实 Redis 和
PostgreSQL 上执行最小生产闭环，使用独立的
`scripts/migration_production_canary_bootstrap.py`。它要求两个显式 opt-in、同一 release
binding、操作者和变更单确认，并且所有 tenant/migration/app/binding ID 都必须使用全新的
`production-canary-*` 前缀；任何已有 tenant、binding、Redis scope 或目标表数据都会 fail-closed。
它只写入 2 个 session + 2 个 memory 记录、source/target storage profile、两个 config
revision、一个无 secret_refs 的 Feishu binding 和最小 tenant policy；不会输出 DSN、密码、token
或 secret。目标连接角色还会在数据库内验证为 LOGIN、NOSUPERUSER、NOBYPASSRLS、非表 owner、非
`trpc_worker`/`trpc_migration`/superuser。

凭证通过 SecretProvider 注入环境，不要把真实 DSN 或密码写进脚本、报告或 shell 历史。以下
命令中的 `<...>` 必须替换成一次性随机值；`TRPC_RELEASE_NONCE` 至少 32 个字符，ID 不能复用：

```powershell
$env:TRPC_RUN_REAL_MIGRATION = "1"
$env:TRPC_MIGRATION_PROVISION = "1"
$env:TRPC_MIGRATION_PROVISION_CONFIRMATION = "I_UNDERSTAND_CREATE_NEW_PRODUCTION_CANARY"
$env:TRPC_MIGRATION_SOURCE_REDIS_URL = "redis://<isolated-source>"
$env:TRPC_MIGRATION_TARGET_DATABASE_DSN = "postgresql+asyncpg://<trpc_runtime>@<isolated-target>/<db>"
$env:TRPC_MIGRATION_TENANT_ID = "production-canary-tenant-<unique>"
$env:TRPC_MIGRATION_ID = "production-canary-migration-<unique>"
$env:TRPC_MIGRATION_APP_ID = "production-canary-app-<unique>"
$env:TRPC_MIGRATION_APP_REVISION = "1"
$env:TRPC_MIGRATION_CONFIG_VERSION = "1"
$env:TRPC_MIGRATION_BINDING_ID = "production-canary-binding-<unique>"
$env:TRPC_MIGRATION_BINDING_REVISION = "1"
$env:TRPC_MIGRATION_OPERATOR_ID = "<operator-id>"
$env:TRPC_MIGRATION_CHANGE_TICKET = "<change-ticket>"
$env:TRPC_RELEASE_ID = "release-<candidate-id>"
$env:TRPC_RELEASE_NONCE = "<one-time-release-nonce-at-least-32-chars>"
$env:TRPC_MIGRATION_IMAGE_DIGEST = "sha256:<deployed-image-digest>"
$canaryReport = "runs/multitenant/migration-production-canary-bootstrap.json"

.venv\Scripts\python.exe scripts\migration_production_canary_bootstrap.py `
  --output $canaryReport
if ($LASTEXITCODE -ne 0) { throw "production-canary provisioning failed; preserve scope and inspect report" }
$canary = Get-Content $canaryReport | ConvertFrom-Json
if ($canary.status -ne "pass" -or $canary.production_gate -ne "not_run" `
    -or $canary.credentials_emitted -ne $false -or $canary.source.source_count -ne 4 `
    -or $canary.target.target_preflight -ne "empty") {
  throw "production-canary provisioning report contract failed"
}
```

Provisioning 成功的判据是报告 `status=pass`、`production_gate=not_run`、source count 为 4、
目标 preflight 为 `empty`、source/target endpoint hash 不同、`credentials_emitted=false`，且
报告中的 scope/release binding 与本次候选一致。失败或中断后不要再次运行同一组 ID，也不要
删除或清空部分数据；先按报告身份人工处理，下一轮必须新建全新的 canary scope。

保持上述环境变量不变，随后运行唯一的完整真实迁移入口：

```powershell
$env:TRPC_MIGRATION_OWNER_ID = "migration-operator-<unique>"
$env:TRPC_MIGRATION_PRODUCTION_CONFIRMATION = "I_UNDERSTAND_REAL_MIGRATION"
$env:TRPC_MIGRATION_CONTROL_FACTORY = "trpc_service.storage.production_migration_control:create"
$migrationReport = "runs/multitenant/migration-live.json"
.venv\Scripts\python.exe scripts\migrate_data.py --production-confirm `
  --output $migrationReport
if ($LASTEXITCODE -ne 0) { throw "production migration failed; inspect migration report and keep barrier" }
$migration = Get-Content $migrationReport | ConvertFrom-Json
if ($migration.gate -ne "pass" -or $migration.production_gate -ne "pass") {
  throw "production migration did not produce a passing gate"
}
```

真实迁移成功的判据是 `migration_evidence` 的七个阶段和 rollback 全部 `pass`，manifest
tenant/migration/app/config/binding revision 与 canary 报告相同，source/target count 和
checksum 相等且 `differences=[]`，control 证明双写、原子 cutover、cleanup 和 rollback，且
报告仍只包含 endpoint hash 而没有 DSN 或凭证。这个 canary 报告本身永远不能把
`production_gate` 从 `not_run` 升级为 `pass`；只有同一 release binding 下的
`migrate_data --production-confirm` 真实运行才能产生生产迁移证据。

1. 保存旧的 immutable storage profile/config revision、tenant_id、migration_id 和当前 checkpoint。
2. 开启该租户双写；确认 PostgreSQL 权威提交和旧投影均有成功/失败可观测记录。
3. 按租户原子切换到候选 profile；只允许该 tenant 的新请求读取候选 profile。
4. 全量 verify；count 相等、checksum 相等、`differences=[]` 后才进入观察窗口。
5. 观察窗口结束后才 cleanup 旧投影；对象和向量数据按 retention TTL 清理，不能立即物理删除。

任意阶段发现差异、租户串读或新后端异常时，先暂停 cleanup，然后调用同一 migration ID 的
`ROLLBACK` 控制钩子：恢复旧 profile/config revision、关闭该租户双写、确认旧后端读写成功，最后
再次执行 source/target count/checksum 对账。回滚只切换权威指针，不反向覆盖新数据；候选数据保留
至安全 TTL，便于调查。若投递结果未知，先标记 ambiguous，禁止用重试代替迁移回滚。

完成真实 source/target、双写、切换、清理和回滚后，才可把对应报告的 `production_gate` 从
`not_run` 更新为 `pass`；在此之前保留 `not_run`，不要在 `runs/multitenant` 中填入虚构的 live
count/checksum。
