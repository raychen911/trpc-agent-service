# 第六阶段验证记录

**功能**：`006-tenant-governance-tool-policy`
**方法**：严格测试先行；每项能力先记录有效 RED，再用同一测试记录 GREEN。
**安全约束**：只记录命令、统计、稳定错误和脱敏摘要；禁止记录凭证、Secret、response URL、明文确认码或敏感参数。

## 记录模板

~~~text
任务：Txxx
日期：YYYY-MM-DD
Git commit：<commit 或 working-tree>
环境：<Python/OS/Redis/PostgreSQL；不含连接密码>
关联：USx；FR-xxx；DEC-xxx

RED 命令：
RED 结果：<passed/failed/skipped、退出码>
RED 原因：<必须是能力尚未实现；不得是导入、环境或错误断言>

GREEN 命令：
GREEN 结果：<passed/failed/skipped、退出码>
修复摘要：
证据：<测试名、稳定 reason code、脱敏 trace 或查询摘要>
~~~

## 前置一致性检查

- 日期：2026-09-10。
- `spec.md`、`plan.md`、`tasks.md` 的 FR-001—FR-034、用户故事、ACA 决策和任务覆盖完整。
- 修复 HIGH 缺口：新增 DEC-004/FR-017a，规定危险操作创建确认前完成一次最大额度预占；等待确认期间保持同一 `RESERVED` reservation；确认成功复用，过期/取消/执行前失配释放，重复确认与恢复不得重新预占或结算。
- 复核结果：88 项任务、checklist 格式错误 0、模板占位符 0、HIGH/CRITICAL 遗留 0。

## T001 验证记录模板

- 日期：2026-09-10。
- Git commit：`working-tree`。
- 结果：模板已创建，可按任务追加环境、commit、命令、RED、GREEN、通过/失败/skip、US/FR/DEC 和脱敏证据；无凭据字段。

## T002 第二/三/五阶段回归基线

- 命令：`uv run --no-cache pytest -q`（原 `uv run pytest -q` 因系统 uv cache
  目录权限拒绝，使用等价的 no-cache 运行）。
- 结果：`233 passed, 28 skipped, 3 warnings`，退出码 0。
- 28 个 skip 均为当前进程未设置 `TRPC_SHARED_REDIS_URL` 或
  `TRPC_SHARED_DATABASE_URL`；无测试失败。warnings 为上游 SDK 弃用提示和
  pytest cache 权限提示，不影响基线。

## T003/T004 测试基础

- T003 结果：治理源码包和 unit/contract/integration/e2e 测试包骨架已创建，pytest 可发现目录；未加入生产业务逻辑。
- T004 结果：`tests/governance_support.py` 已提供双租户相反策略、双节点标识、确定性用量和虚拟时钟构造器；fixture 不读取真实模型、危险工具或 IM 凭证。

## 一致性复核（speckit-analyze remediation）

- 需求覆盖：35 个 FR 标识（FR-001—FR-034 及 FR-017a）均至少关联一项任务，覆盖率 100%。
- 决策映射：DEC-001、DEC-002、DEC-003、DEC-004 均在 `clarification-decisions.md`、`plan.md` 和 `tasks.md` 中显式出现。
- 任务格式：88 项任务均符合 `- [ ] Txxx [P?] [USx?] 描述 + 文件路径`；格式错误 0。
- 占位符/严重问题：006 目录 TODO/TKTK/占位符 0；HIGH/CRITICAL 遗留 0。
- 修订：DEC-004 固化确认等待期间 reservation 保持 `RESERVED`、TTL 到期释放、确认成功复用且不得重复预占/结算。

## Phase 2 / US1 / US2 / US3 / US4 基础增量

- T005 RED：`5 failed`；T009 GREEN：`5 passed`（不可变治理模型、状态机和稳定错误）。
- T006 RED：`2 failed`；T010 GREEN：`2 passed`（五类治理 Repository Protocol 及租户/fencing 参数）。
- T007 RED：`2 failed`；T011 GREEN：schema 静态断言 `2 passed`（治理 migration v5 与 ORM row 映射）。
- T008 RED：`1 failed, 1 passed`；T012 callback 基础 GREEN：`2 passed`（官方 callback 异步入口，无旁路 Runner）。
- US1 策略 RED→GREEN：`5 passed`；US2 主体授权 RED→GREEN：`4 passed`。
- US3/US4/US5 domain 增量 RED→GREEN：确认、预算、内容测试 `5 passed`。
- 当前所有新增单元/契约测试均不访问真实 IM、真实模型或凭证。

## 当前回归复核

- 命令：`.\\.venv\\Scripts\\python.exe -m pytest -q -p no:cacheprovider`
- 结果：`258 passed, 28 skipped, 2 warnings`，退出码 0。
- 28 个 skip 仍仅由既有 shared Redis/PostgreSQL 环境变量前置条件产生；006 新增测试无 skip、无失败。
- 当前已完成 T001–T011 的可验证增量；T012 及后续用户故事仍按任务清单继续实施，不能将本记录视为 Phase 9 最终验收。

## Real IM ACA evidence (user supplied)

- Feishu single-chat accepted replies: trace IDs `40d3166c-db10-478e-aeec-7120424928f4`
  and `9cd1ef22-5a07-463b-a894-a01274128f03`.
- WeCom single/group accepted replies: trace IDs `c660412d-897e-4483-b382-562075ce803f`,
  `c8bba018-205f-480a-b774-a1487803551e`, and subsequent group confirmation evidence.
- These are non-sensitive trace identifiers only; credentials, response URLs and screenshots
  are intentionally not copied into Git.

## T012–T013 Phase 2 completion

- T012 RED：共享对象缺少治理端口；补充 `SharedGovernancePorts` 和 PostgreSQL
  governance repository wiring 后 GREEN。
- T012 GREEN：`tests/unit/test_shared_infrastructure.py` 与 SDK callback 检查通过，
  callback 为官方异步入口且 composition root 不创建第二个 Runner。
- T013 GREEN 命令：`.venv\\Scripts\\python.exe -m pytest -q tests/unit/governance
  tests/contract/governance tests/integration/governance tests/sdk_validation -p no:cacheprovider`。
  结果：`65 passed`，退出码 0。

## US1–US6 and Phase 9 completion

- Deterministic governance suite: `65 passed`, no credential/model/IM network dependency.
- Full regression command: `powershell -NoProfile -ExecutionPolicy Bypass -File
  specs/006-tenant-governance-tool-policy/scripts/run-automated-validation.ps1`。
  Results: governance `65 passed`; full suite `281 passed, 28 skipped, 2 warnings`,
  exit code 0. Skips are existing Redis/PostgreSQL environment gates only.
- T085 scan: `sensitive_scan_hits=0`, exit code 0. The scanner ignores virtualenv,
  cache and Git metadata and checks only credential-shaped assignments/URLs.
- T086 repeatable validation script created; T087 stage record and README mapping updated.
- T088 static checks: `git diff --check` passed; all T001–T088 checklist entries are
  complete. Real Feishu/WeCom credentials remain local-only ACA evidence and are not
  included in this repository.
- Shared Redis/PostgreSQL integration remains environment-gated in this process:
  `TRPC_SHARED_REDIS_URL` and `TRPC_SHARED_DATABASE_URL` were unset, so the existing
  28 tests are recorded as skipped rather than passed. Re-run the validation script in
  the configured backend terminal to close that external gate.
