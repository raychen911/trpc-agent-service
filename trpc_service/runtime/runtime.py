# ===================================================================
# runtime.runtime - Runtime 编排器（平台层新增核心）
# ===================================================================
# 说明: PRD 0.3 运行时模型——在框架 Runner 之外补上:
#   1. 租户上下文注入: tenant_id -> 租户配置（Registry LRU）
#   2. 动态构建 Agent（runner.run 内部按租户配置构建）
#   3. 事件流 -> IM 回复（RunnerEvent 流 -> AgentResponse）
#   4. Session / Memory 读写（Storage Adapter，共享后端）
#   5. 异步收尾: 审计 / 成本累计 / 指标（PRD 0.3-6）
# 规范: Worker 无状态；同一 session 并发写经分布式锁串行化（PRD 2.3-A）。
# ===================================================================

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from ..events import AgentEvent, AgentResponse, ResponseType
from ..agent.summarizer import LlmSummarizer
from ..log.logger import bind_logger, get_logger
from ..metrics.metrics import Metrics
from ..storage.base import Storage, acquire_lock_with_retry
from ..storage.manager import StorageManager
from ..tenant.models import TenantConfig
from ..tenant.budget import BudgetTracker
from ..tenant.registry import TenantRegistry
from .events import RunnerEvent
from .runner import AgentRunner, MockAgentRunner

log = get_logger("runtime")

_WRITE_RETRY_SLEEP_S = 0.2
"""后端写首试失败后的重试间隔（与 tenant/budget.py 同款策略对齐）。"""


class Runtime:
    """一次消息的运行时编排（内嵌 Worker）。"""

    def __init__(
        self,
        registry: TenantRegistry,
        storage: Storage,
        metrics: Optional[Metrics] = None,
        runner: Optional[AgentRunner] = None,
        summarizer: Optional[LlmSummarizer] = None,
        budget_tracker: Optional[BudgetTracker] = None,
        execution_audit_enabled: bool = True,
        storage_manager: Optional[StorageManager] = None,
    ) -> None:
        self._registry = registry
        self._storage = storage
        self._storage_manager = storage_manager
        """按租户懒建 Storage 的容器（PRD 2.1）；None 时所有租户共用 `storage`。"""
        self._metrics = metrics
        self._runner = runner or MockAgentRunner()
        self._summarizer = summarizer
        """LLM 摘要器（agent.summarizer.LlmSummarizer）；None 时用确定性摘要。"""
        self._budget_tracker = budget_tracker
        """成本结算器（tenant.budget.BudgetTracker）；None 时成本仅入 metrics/审计。"""
        self._execution_audit_enabled = execution_audit_enabled
        """执行审计开关（PRD 4.4）；单测可关，默认开。"""
        self._bg_tasks: set[asyncio.Task] = set()
        """后台摘要任务引用（防 GC；测试可 await 全部任务后再断言落库）。"""

    async def _resolve_storage(self, tenant: TenantConfig) -> Storage:
        """按租户解析 Storage：有 manager 则懒建缓存，否则回落启动时单例。"""
        if self._storage_manager is not None:
            return await self._storage_manager.get(tenant)
        return self._storage

    async def handle(self, event: AgentEvent) -> AgentResponse:
        """处理一条已通过 Filter 链的 AgentEvent，返回 IM 回复。

        Args:
            event: 已含 tenant_id / session_id / user_id / content / trace_id

        Returns:
            AgentResponse: 文本 / 卡片 / 错误回复
        """
        logger = bind_logger(log, trace_id=event.trace_id, tenant_id=event.tenant_id, session_id=event.session_id)
        start = time.perf_counter()
        # 活跃 session 计数（Gauge，运维观测）；方法级 try/finally 保证成对增减
        if self._metrics is not None:
            self._metrics.active_sessions.labels(tenant_id=event.tenant_id).inc()
        try:
            return await self._handle(event, start, logger)
        finally:
            if self._metrics is not None:
                self._metrics.active_sessions.labels(tenant_id=event.tenant_id).dec()

    async def _handle(self, event: AgentEvent, start: float, logger: Any) -> AgentResponse:
        # 1. 租户上下文注入（PRD 0.3-4a）
        try:
            tenant = await self._registry.get_or_raise(event.tenant_id)
        except KeyError:
            return self._error(event, f"租户不存在: {event.tenant_id}")
        if not tenant.is_active:
            return self._error(event, f"租户不可用: {event.tenant_id}")
        # 灰度选版（PRD 5.2）: 按 user_id 哈希命中则用 canary 覆盖配置（请求级）
        from ..tenant.gray import apply_gray

        tenant = apply_gray(tenant, event.user_id)
        # 按租户解析 Storage（PRD 2.1: 不同租户可选不同后端）
        storage = await self._resolve_storage(tenant)

        # 2. Session 读取（共享后端，PRD 1.3 无状态 Worker）
        read_start = time.perf_counter()
        session = await storage.session.get_session(event.tenant_id, event.session_id)
        self._observe_session_latency(storage, "get", time.perf_counter() - read_start)
        session_state = (session or {}).get("state", {})

        # 3. Memory 检索（PRD 2.2）
        memories: list[dict[str, Any]] = []
        try:
            memories = await storage.memory.search_memory(event.tenant_id, event.user_id, event.content, top_k=3)
        except Exception as exc:  # noqa: BLE001 - 记忆检索失败不阻塞主链路
            logger.warning("memory search failed", extra={"error": str(exc)})

        # 4. 执行 Agent（runner 内部按租户配置动态构建，PRD 0.3-4b/c）
        reply_parts: list[str] = []
        tool_trace: list[str] = []
        in_tokens = 0
        out_tokens = 0
        run_error: Optional[str] = None
        # 危险工具二次确认（PRD 4.1）: 通道侧显式确认过的工具放行，
        # 其余危险工具由 tool.builder 运行时门控拦截（返回需确认，不执行）。
        confirmed_tools: frozenset[str] = frozenset()
        if event.metadata.get("tool_confirmed"):
            declared = event.metadata.get("tool_name")
            if declared:
                confirmed_tools = frozenset({declared})
        try:
            async for runner_event in self._runner.run(
                    tenant=tenant,
                    user_id=event.user_id,
                    session_id=event.session_id,
                    new_message=event.content,
                    memories=memories,
                    session_state=session_state,
                    confirmed_tools=confirmed_tools,
            ):
                in_tokens += runner_event.input_tokens
                out_tokens += runner_event.output_tokens
                self._apply_event(runner_event, tenant, event, reply_parts, tool_trace, logger)
                if runner_event.type == "error":
                    run_error = runner_event.error or "runner_error"
                    break
        except Exception as exc:  # noqa: BLE001 - Runner 异常转错误回复
            logger.error("runner failed", extra={"error": str(exc)})
            run_error = type(exc).__name__

        if run_error is not None:
            # 错误路径统一收尾（执行审计 + 成本 + 指标），不写 session/summary
            self._record_error(tenant, event, run_error)
            await self._settle_usage(storage,
                                     tenant,
                                     event,
                                     tool_trace,
                                     in_tokens,
                                     out_tokens,
                                     start,
                                     error_type=run_error)
            return self._error(event, run_error)

        reply = "\n".join(part for part in reply_parts if part)

        # 5. Session 状态持久化（PRD 2.3-A: 分布式锁 + 乐观锁版本）
        # 先快照本轮之前的对话：_save_session 会原地 append 最新一轮，
        # 摘要输入与轮数统计不能把它重复计入。
        history_before = list((session_state or {}).get("history") or [])
        await self._save_session(storage, tenant, event, session_state, reply, tool_trace)

        # 5.1 Summary 持久化（PRD 2.2，每 session 一条，失败不阻塞主链路）
        await self._save_summary(storage, tenant, event, session_state, reply, history_before, logger)

        # 6. 异步收尾: 执行审计 + 成本 + 指标（PRD 4.2/4.4/6-10）
        await self._settle_usage(storage, tenant, event, tool_trace, in_tokens, out_tokens, start, error_type=None)
        self._record_metrics(tenant, event, reply, tool_trace, start, logger)

        logger.info("runtime done",
                    extra={
                        "reply_len": len(reply),
                        "tools": tool_trace,
                        "input_tokens": in_tokens,
                        "output_tokens": out_tokens
                    })
        return AgentResponse.text(reply,
                                  session_id=event.session_id,
                                  tenant_id=event.tenant_id,
                                  channel_type=event.channel_type,
                                  trace_id=event.trace_id)

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _apply_event(
        self,
        runner_event: RunnerEvent,
        tenant: TenantConfig,
        event: AgentEvent,
        reply_parts: list[str],
        tool_trace: list[str],
        logger: Any,
    ) -> None:
        """按事件类型更新回复 / 工具记录。"""
        if runner_event.type == "content" and runner_event.content:
            reply_parts.append(runner_event.content)
        elif runner_event.type == "tool_call":
            tool_trace.append(runner_event.tool_name)
            if self._metrics is not None:
                self._metrics.tool_calls.labels(tenant_id=event.tenant_id,
                                                tool_name=runner_event.tool_name,
                                                status="ok").inc()
        elif runner_event.type == "tool_result":
            logger.info("tool result",
                        extra={
                            "tool": runner_event.tool_name,
                            "output_len": len(runner_event.tool_output or "")
                        })

    async def _save_session(
        self,
        storage: Storage,
        tenant: TenantConfig,
        event: AgentEvent,
        session_state: dict[str, Any],
        reply: str,
        tool_trace: list[str],
    ) -> None:
        """写回 Session state（无历史时初始化；PRD 2.3-A 锁内重读防并发丢更新）。

        并发一致性（09-04 联调实测修复）：读-改-写横跨整个 LLM 调用周期，
        ``handle`` 开头读到的 ``session_state`` 快照在并发下必然过期——仅给
        「写」加锁防不住丢失更新。故在锁内**重读最新 state** 为基线，append
        本轮两条消息后再落库（版本号在锁内自增，串行化成立）。
        拿锁超时（wait_seconds 预算耗尽）时尽力写入 + 告警：可用性优先，
        与平台「不阻塞主链路」哲学一致（重读后竞态窗口已极小）。
        """
        if not event.session_id:
            return
        lock_key = f"lock:session:{event.tenant_id}:{event.session_id}"
        acquired = await acquire_lock_with_retry(storage.lock, lock_key, ttl=10, wait_seconds=5.0)
        if not acquired:
            log.warning("session lock timeout, best-effort write",
                        extra={
                            "tenant": event.tenant_id,
                            "session_id": event.session_id
                        })
        try:
            # 锁内重读最新 state 为基线（并发下 handle 开头的快照已过期）；
            # InMemory 实现返回活引用，history 须显式拷贝避免原地 append。
            reread_start = time.perf_counter()
            fresh = await storage.session.get_session(event.tenant_id, event.session_id)
            self._observe_session_latency(storage, "get", time.perf_counter() - reread_start)
            base_state = (fresh or {}).get("state")
            state = dict(base_state) if base_state else dict(session_state or {})
            history = list(state.get("history") or [])
            # user 消息带 msg_id：撤回事件据此在历史中定位并标记 revoked（PRD 3.7）
            user_msg: dict[str, Any] = {"role": "user", "content": event.content}
            if event.msg_id:
                user_msg["msg_id"] = event.msg_id
            history.append(user_msg)
            history.append({"role": "assistant", "content": reply})
            state["history"] = history
            state["tools"] = tool_trace
            # 写失败带一次重试（budget.py 同款策略）；仍失败仅告警，不中断回复
            write_start = time.perf_counter()

            async def _write() -> None:
                await storage.session.update_state(event.tenant_id, event.session_id, state)

            await self._with_write_retry("session state", event.tenant_id, _write, log)
            self._observe_session_latency(storage, "update", time.perf_counter() - write_start)
        finally:
            if acquired:
                await storage.lock.release(lock_key)

    async def _save_summary(
        self,
        storage: Storage,
        tenant: TenantConfig,
        event: AgentEvent,
        session_state: dict[str, Any],
        reply: str,
        history_before: list[dict[str, Any]],
        logger: Any,
    ) -> None:
        """写回 Summary（PRD 2.2: 每 session 一条，低频更新）。

        有摘要器时投递后台任务生成 LLM 摘要，**不阻塞回复**（fire-and-forget）；
        无摘要器（mock runner / 单测）用确定性摘要。两种路径失败均仅告警，
        不阻塞主链路。
        """
        if not event.session_id:
            return
        fallback = self._deterministic_summary(history_before, event)
        if self._summarizer is None:
            await self._save_summary_content(storage, event, fallback, logger)
            return
        self._schedule_llm_summary(storage, tenant, event, reply, history_before, fallback, logger)

    @staticmethod
    def _deterministic_summary(history_before: list[dict[str, Any]], event: AgentEvent) -> str:
        """确定性摘要（回落兜底）：轮数 + 最近消息。"""
        turns = len(history_before) + 2  # 本轮 user + assistant
        return f"会话共 {turns} 条消息，最近消息: {event.content[:200]}"

    async def _save_summary_content(self, storage: Storage, event: AgentEvent, content: str, logger: Any) -> None:
        # 写失败带一次重试；仍失败仅告警，不阻塞主链路
        async def _write() -> None:
            await storage.summary.save_summary(event.tenant_id, event.session_id, content)

        await self._with_write_retry("summary", event.tenant_id, _write, logger)

    async def _with_write_retry(self, op_name: str, tenant_id: str, write: Any, logger: Any) -> None:
        """后端写重试（budget.py:61-72 同款策略）：首写失败 → sleep → 重试一次。

        两次均失败仅告警不抛——平台哲学是写路径不阻塞回复（可用性优先）。
        """
        try:
            await write()
            return
        except Exception as exc:  # noqa: BLE001 - 首写失败转重试
            logger.warning(f"{op_name} write failed, retrying", extra={"tenant_id": tenant_id, "error": str(exc)})
        await asyncio.sleep(_WRITE_RETRY_SLEEP_S)
        try:
            await write()
        except Exception as exc:  # noqa: BLE001 - 仍失败仅告警
            logger.warning(f"{op_name} write failed after retry", extra={"tenant_id": tenant_id, "error": str(exc)})

    def _observe_session_latency(self, storage: Storage, op: str, seconds: float) -> None:
        """Session 后端读写延迟打点（PRD 4.2 点名指标，backend 取实现类名）。"""
        if self._metrics is None:
            return
        backend = type(storage.session).__name__
        self._metrics.session_backend_latency.labels(op=op, backend=backend).observe(seconds)

    def _schedule_llm_summary(
        self,
        storage: Storage,
        tenant: TenantConfig,
        event: AgentEvent,
        reply: str,
        history_before: list[dict[str, Any]],
        fallback: str,
        logger: Any,
    ) -> None:
        """后台任务: LLM 生成摘要并落库。

        任务引用挂在本实例上防止被 GC；模型失败回落确定性摘要。
        """
        messages = [dict(m) for m in history_before]
        messages.append({"role": "user", "content": event.content})
        messages.append({"role": "assistant", "content": reply})

        async def _run() -> None:
            try:
                content = await self._summarizer.summarize(tenant, messages)
            except Exception as exc:  # noqa: BLE001 - 摘要失败仅告警
                logger.warning("llm summary failed, fallback to deterministic", extra={"error": str(exc)})
                content = fallback
            await self._save_summary_content(storage, event, content, logger)

        task = asyncio.create_task(_run())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    def _record_metrics(
        self,
        tenant: TenantConfig,
        event: AgentEvent,
        reply: str,
        tool_trace: list[str],
        start: float,
        logger: Any,
    ) -> None:
        if self._metrics is None:
            return
        latency = time.perf_counter() - start
        self._metrics.agent_requests.labels(tenant_id=event.tenant_id,
                                            channel=event.channel_type,
                                            agent_name=tenant.name).inc()
        self._metrics.llm_latency.labels(tenant_id=event.tenant_id, model=tenant.model.model_name,
                                         status="ok").observe(latency)

    async def _settle_usage(
        self,
        storage: Storage,
        tenant: TenantConfig,
        event: AgentEvent,
        tool_trace: list[str],
        in_tokens: int,
        out_tokens: int,
        start: float,
        *,
        error_type: Optional[str] = None,
    ) -> None:
        """执行后统一收尾: token/成本指标 + 执行审计 + 预算累计（PRD 4.2/4.4/6-10）。

        - 成功路径（error_type=None）decision=executed；失败路径 decision=
          execution_error（此时可能已消耗部分 token，同样计入成本）。
        - 网关侧 AuditFilter 记治理决策（allow/block）；本方法记**执行**审计，
          两层语义不同、互不覆盖（PRD 4.4）。
        - 成本 = 输入/输出 token × 租户模型单价；价格未配置（0）时 cost=0，
          token 照常计数入 metrics/审计。
        - budget_tracker 持久化 used_budget_usd（SQL 原子累加 + 缓存/广播失效）；
          失败不抛错，不阻塞回复。
        """
        if self._metrics is not None:
            self._metrics.llm_tokens_input.labels(tenant_id=event.tenant_id,
                                                  model=tenant.model.model_name).inc(in_tokens)
            self._metrics.llm_tokens_output.labels(tenant_id=event.tenant_id,
                                                   model=tenant.model.model_name).inc(out_tokens)

        cost_usd = self._estimate_cost_usd(tenant, in_tokens, out_tokens)
        if self._metrics is not None and cost_usd > 0:
            self._metrics.tenant_cost.labels(tenant_id=event.tenant_id, cost_type="llm").inc(cost_usd)

        # 执行审计（PRD 4.4: 与网关治理审计并列，覆盖真实执行结果）
        if self._execution_audit_enabled and tenant.audit.enabled:
            tools = list(dict.fromkeys(tool_trace))
            log_entry: dict[str, Any] = {
                "trace_id": event.trace_id,
                "channel": event.channel_type,
                "user_id": event.user_id,
                "session_id": event.session_id,
                "agent_name": tenant.name,
                # 单次执行涉及多工具时逗号拼接（保留出现顺序，去重）；无工具
                # 统一为 None（与 allow / recall 行口径一致，便于统一查询）
                "tool_name": ",".join(tools) if tools else None,
                "decision": "execution_error" if error_type else "executed",
                "latency_ms": int((time.perf_counter() - start) * 1000),
                "error_type": error_type,
                "cost": str(cost_usd),
                "payload": {
                    "input_tokens": in_tokens,
                    "output_tokens": out_tokens,
                    "tools": tools,
                    "trace_id": event.trace_id,
                },
            }

            async def _write() -> None:
                await storage.audit.write_log(event.tenant_id, log_entry)

            # 执行审计失败不阻塞回复；带一次重试降低 DB 抖动丢审计概率
            await self._with_write_retry("execution audit", event.tenant_id, _write, log)

        # 预算累计（PRD 6-10 BudgetFilter 硬限的数据来源）
        if self._budget_tracker is not None and cost_usd > 0:
            # 后台任务（BudgetTracker 内部已带一次重试），避免 DB 写阻塞回复；
            # 任务引用挂实例防 GC；关闭时经 drain_background_tasks 排空。
            task = asyncio.create_task(self._budget_tracker.record(event.tenant_id, cost_usd))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)

    async def drain_background_tasks(self, timeout_s: float = 5.0) -> None:
        """关闭前排空后台任务（摘要 / 预算落库），避免进程退出丢写入。

        受 timeout_s 保护：任务超时未完成则放弃（不无限阻塞关闭）。
        """
        tasks = list(self._bg_tasks)
        if not tasks:
            return
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout_s)
        except asyncio.TimeoutError:
            log.warning("drain background tasks timeout", extra={"pending": len(tasks)})
        finally:
            # gather 已完成的任务会被 done_callback 清掉；残留的超时任务仅告警
            self._bg_tasks.clear()

    @staticmethod
    def _estimate_cost_usd(tenant: TenantConfig, in_tokens: int, out_tokens: int) -> float:
        """按租户模型单价估算成本（USD）。

        单价 0（未配置）返回 0.0；tokens 照常单独计数。
        """
        model = tenant.model
        cost = in_tokens / 1_000_000 * model.input_price_per_1m_usd
        cost += out_tokens / 1_000_000 * model.output_price_per_1m_usd
        return round(cost, 6)

    def _record_error(self, tenant: TenantConfig, event: AgentEvent, error_type: str) -> None:
        if self._metrics is not None:
            self._metrics.agent_errors.labels(tenant_id=event.tenant_id, error_type=error_type, stage="runner").inc()

    def _error(self, event: AgentEvent, message: str) -> AgentResponse:
        return AgentResponse(
            response_type=ResponseType.ERROR,
            content=message,
            session_id=event.session_id,
            tenant_id=event.tenant_id,
            channel_type=event.channel_type,
            trace_id=event.trace_id,
        )
