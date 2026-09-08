# P0 实施任务 Checklist

> P0 = 正确性 / 体验的关键路径增强，优先于 `ENHANCEMENTS.md` 中的 P1/P2 完成。
> 本清单把三项 P0 拆成可勾选、可验收的子任务，每个任务给出「改动文件 / 子任务 /
> 验收标准 / 关联测试」。完成后逐项打勾。

**统一完成定义（DoD）**：子任务全部勾选 + 关联测试通过 + `flake8` 无告警 +
`pytest tests/service` 全绿 + `git status` 仅新增文件（零侵入 core）。

---

## P0-1 预算按日滚动（日期分桶）✅ 已完成

**目标**：`BudgetTracker` 的日预算按自然日正确滚动，多节点无重复重置问题。

**改动文件**：`trpc_service/tool/_budget.py`、`tests/service/test_budget_hitl_stream.py`

- [x] 引入 `_date_key(tenant_id, date_str)`：`_usage`/`_reserved` 的 key 由 `tenant_id` 改为 `(tenant_id, date_str)`，`date_str = YYYY-MM-DD`（UTC）
- [x] `record / reserve / release / total_tokens / cost / input_tokens / output_tokens / reserved_tokens` 全部增加 `date_str: Optional[str] = None`（默认今日）
- [x] `is_within_budget` / `reserve` 只读、只写**当日**桶
- [x] 新增 `cost_by_date(tenant_id) -> dict[str, float]` 保留历史桶供账单
- [x] 确认 `ModelBudgetFilter` 无需改动（`reserve/record` 默认走今日）

**验收标准**：传入两个不同 `date_str` 记账互不影响；`is_within_budget` 只看当日桶，跨日自动"清零"。

**关联测试**：`test_budget_daily_rollover`、`test_reserve_scoped_to_day`（已通过）。

---

## P0-2 网关 / Worker 网络分离（Redis Streams）✅ 已完成

**目标**：Gateway 只接入与路由，Worker 独立消费执行，两者可独立扩缩容。

**改动文件**：新增 `trpc_service/agent/_queue.py`、`trpc_service/web/_dispatch.py`、`trpc_service/agent/_consumer.py`、`trpc_service/agent/run_worker.py`；改 `trpc_service/web/gateway/_app.py`；compose/k8s 拆分；测试 `test_queue.py`

- [x] 定义任务消息 `TaskMessage(tenant_id, channel, inbound: dict, trace_headers: dict)`
- [x] `StreamQueue`：`enqueue`（`XADD`）、`ensure_group`（`XGROUP CREATE MKSTREAM`）、`read`（`XREADGROUP`）、`ack`（`XACK`）
- [x] Gateway：`create_gateway_app` 增加可选 `queue` 注入；`queue` 存在时改为 `enqueue`，保留 `async_dispatch` 同步/后台路径
- [x] `StreamWorker` 消费者：`run_once` 读→反序列化→`run_and_reply`→`ack`；`run` 阻塞消费循环（at-least-once）
- [x] `trpc_service/agent/run_worker.py` 独立进程入口；compose/k8s 拆分 `gateway` 与 `worker` 两个服务

**验收标准**：Gateway 进程只入队不执行；Worker 进程消费执行；ACK 后消息不再重投。

**关联测试**：`test_queue.py`（enqueue/read/ack 用 fakeredis、gateway 入队路径、StreamWorker 消费+ack，均已通过）。

---

## P0-3 企业微信原生流式 ✅ 已完成

**目标**：`send_stream` 走企业微信原生 stream 能力，单气泡流式更新，降低首字延迟。

**改动文件**：`trpc_service/channels/_wecom.py`、`tests/service/test_channel_send.py`

- [x] 确认企业微信 stream 接口契约（开流 `msgtype=stream` 经 `message/send`、追加/结束经 `message/update`，`stream.id` 以 `STREAMID` 前缀，`finish=True` 关流）
- [x] `WecomAdapter.send_stream` 改为：首块开流 → 后续块 `message/update`（按 `stream_edit_interval` 节流）→ 末尾 `finish=True` 关流
- [x] 开流失败时降级为分段多消息（`send_message` 逐块）
- [x] `reply_text` / `send_message` 保持不变作为非流式兜底

**验收标准**：流式走"开流→追加→结束"调用序列；开流失败自动降级；非流式路径不受影响。

**关联测试**：`test_channel_send.py::test_wecom_send_stream_native_sequence`、`test_wecom_send_stream_fallback_on_open_failure`（均已通过）。

---

## 顺序与依赖

```
P0-1 预算日期分桶（独立，无依赖）✅
P0-2 网关/Worker 分离（依赖幂等，已就绪）✅
P0-3 企业微信流式（独立）✅
```

三项 P0 全部完成。

## 阶段完成时的回归（历史记录）✅

- [x] `pytest tests/service`（224 passed）
- [x] `flake8 trpc_service tests/service` 零告警
- [x] `--cov=trpc_service --cov-fail-under=95` 行覆盖率 95.93%
- [x] `python examples/multi_tenant_saas/simulate.py` 正常
- [x] 核心 SDK 通过 `trpc-agent-py` 外部依赖复用，仓库只维护 `trpc_service`
