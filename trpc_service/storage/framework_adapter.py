# ===================================================================
# storage.framework_adapter - 平台存储层 -> 框架 Service 适配
# ===================================================================
# 说明: 阶段二 Spec §3「Session/Memory 后端」决策——平台层实现框架的
#   Service 抽象，内部委托给既有 Storage，从而同时满足:
#     1. 框架 Runner 需要 session_service / memory_service（PRD 0.3）
#     2. 平台层保留多后端 + 分布式锁 + 幂等（PRD 2.1 / 2.3）
#   若直接用框架内置服务，既有 storage/ 层会空转，验收标准 4 失去代码支撑。
#
# 实测基线（trpc-agent-py v1.1.19，见 DEVELOPMENT_LOG「阶段二 Spec」§0）:
#   - 框架 session/memory 服务实现均为 async def，与平台层对齐，无需桥接
#   - BaseSessionService.append_event 只做内存追加；update_session 默认是
#     no-op —— 持久化必须靠重写 update_session
#   - Runner 调用顺序: get_session → append_event → update_session
#   - Session.save_key 由 utils.user_key(app_name, user_id) 生成
# 规范: 隔离以构造时传入的 tenant_id 为准（PRD 1.4）；框架的 app_name /
#   user_id 只作元数据，不参与隔离判定。
# ===================================================================

from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.knowledge import KnowledgeBase, SearchDocument, SearchResult
from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.sessions import BaseSessionService, Session, extract_state_delta
from trpc_agent_sdk.types import SearchMemoryResponse
from trpc_agent_sdk.utils import user_key

from .base import acquire_lock_with_retry
from ..log.logger import get_logger

_log = get_logger("storage.framework")

_FW_SESSION_SUFFIX = ":fw"
"""框架 Session 在平台 SessionStore 中的命名空间后缀。

平台的 session.state 由 Runtime._save_session 全量覆盖（写 history/tools），
框架 Session（含 events）若共用同一 state 会被覆盖冲掉，故分键存放。
两者同样带 tenant_id 前缀，隔离语义不受影响。
"""


class PlatformSessionService(BaseSessionService):
    """把框架 Session 读写委托给平台层 Storage（PRD 0.4 / 2.1）。"""

    def __init__(self, storage, tenant_id: str) -> None:
        super().__init__()
        self._storage = storage
        self._tenant_id = tenant_id

    def _storage_key(self, session_id: str) -> str:
        return f"{session_id}{_FW_SESSION_SUFFIX}"

    def _serialize(self, session: Session) -> dict[str, Any]:
        return {"session_id": self._storage_key(session.id), "state": {"_fw": session.model_dump(mode="json")}}

    def _deserialize(self, raw: dict[str, Any]) -> Optional[Session]:
        payload = (raw.get("state") or {}).get("_fw")
        if not payload:
            return None
        return Session.model_validate(payload)

    # ------------------------------------------------------------------
    # SessionServiceABC 实现（基类仅 4 个抽象方法，其余有默认实现）
    # ------------------------------------------------------------------

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict[str, Any]] = None,
        session_id: Optional[str] = None,
        agent_context: Any = None,
    ) -> Session:
        session = Session(
            id=session_id or uuid.uuid4().hex,
            app_name=app_name,
            user_id=user_id,
            state=dict(state or {}),
            last_update_time=time.time(),
            save_key=user_key(app_name, user_id),
        )
        await self.update_session(session)
        return session

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        agent_context: Any = None,
    ) -> Optional[Session]:
        raw = await self._storage.session.get_session(self._tenant_id, self._storage_key(session_id))
        if not raw:
            return None
        return self._deserialize(raw)

    def _fw_lock_key(self, session_id: str) -> str:
        """框架 Session 的并发写锁（与平台态锁分键，避免互相阻塞）。"""
        return f"lock:session:{self._tenant_id}:{session_id}:fw"

    async def _persist(self, session: Session) -> None:
        """无锁全量落库（仅供 _locked_persist 在锁内调用）。"""
        await self._storage.session.save_session(self._tenant_id, self._serialize(session))

    async def _locked_persist(self, session: Session) -> None:
        """锁内重读-合并-落库（并发写一致性，PRD 2.3-A；09-04 联调修复）。

        并发 Runner 各持独立的内存 Session 对象，update_session 的全量覆盖
        会互相冲掉 events（实测 6 并发丢 5 条模型回复）。故写前加锁并重读
        存储中最新 Session 为基准，按**序列化指纹**合并双方独有事件（框架
        Event 无 id 字段，实测 v1.1.19），state 以 fresh 为底、本轮 delta 优先。
        拿锁超时尽力写 + 告警（可用性优先，与 Runtime._save_session 同策略）。
        """
        lock = getattr(self._storage, "lock", None)
        lock_key = self._fw_lock_key(session.id)
        acquired = False
        if lock is not None:
            acquired = await acquire_lock_with_retry(lock, lock_key, ttl=10, wait_seconds=5.0)
            if not acquired:
                _log.warning(f"framework session lock timeout, best-effort write: session={session.id}")
        try:
            raw = await self._storage.session.get_session(self._tenant_id, self._storage_key(session.id))
            fresh = self._deserialize(raw) if raw else None
            if fresh is not None:
                # events 合并: fresh 独有 + 本会话对象独有（去重防重复落库）
                fresh_fps = {e.model_dump_json() for e in fresh.events}
                merged = list(fresh.events)
                for e in session.events:
                    fp = e.model_dump_json()
                    if fp not in fresh_fps:
                        merged.append(e)
                        fresh_fps.add(fp)
                session.events = merged
                # state 合并: 保留他人写入的 key，本轮 state delta 优先
                merged_state = dict(fresh.state)
                merged_state.update(session.state)
                session.state = merged_state
            await self._persist(session)
        finally:
            if acquired:
                await lock.release(lock_key)

    async def append_event(self, session: Session, event: Event) -> Event:
        """追加事件并落库（Runner 正常路径的持久化点）。

        基类 append_event 只做内存追加，而 Runner 仅在**异常/取消**路径调用
        update_session —— 正常路径不调。故持久化必须挂在这里，与框架
        InMemorySessionService 的做法一致；否则会话历史永远为空。
        落库经 _locked_persist（锁内重读合并），并发写不再互相冲掉。
        """
        if getattr(event, "partial", False):
            return event

        result = await super().append_event(session=session, event=event)

        actions = getattr(event, "actions", None)
        state_delta = getattr(actions, "state_delta", None) if actions else None
        if state_delta:
            session.state.update(extract_state_delta(state_delta).session_state)

        await self._locked_persist(session)
        return result

    async def update_session(self, session: Session) -> None:
        """持久化框架 Session（Runner 异常/取消路径）。

        基类此处默认是 no-op（"concrete implementations should override"）。
        同样走锁内重读合并——创建会话（create_session）与错误路径并发安全。
        """
        await self._locked_persist(session)

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        await self._storage.session.delete_session(self._tenant_id, self._storage_key(session_id))

    async def list_sessions(self, *, app_name: str, user_id: Optional[str] = None):
        # 平台 SessionStore（PRD 2.1）未定义按 app_name 列举的能力，
        # Runner 也不依赖本方法；显式报错好过静默返回空列表。
        raise NotImplementedError("平台 SessionStore 未定义按 app_name 列举接口（PRD 2.1 无此能力）")

    async def close(self) -> None:
        """Storage 生命周期由平台统一管理，此处不关闭共享后端。"""
        return None


class PlatformMemoryService(BaseMemoryService):
    """把框架 Memory 读写委托给平台层 Storage。

    框架 `store_session` 语义是「把一个 session 沉淀为长期记忆」，
    平台侧对应 `add_memory(tenant_id, user_id, memory)`。
    """

    def __init__(self, storage, tenant_id: str) -> None:
        # enabled=True: Runner 据此决定是否沉淀记忆（见 Runner 源码第 83 行）
        super().__init__(enabled=True)
        self._storage = storage
        self._tenant_id = tenant_id

    async def store_session(self, session: Session, agent_context: Any = None) -> None:
        """把 session 中的消息沉淀为记忆（按 user_id 归档，租户隔离）。"""
        texts = _extract_texts(session)
        if not texts:
            return
        await self._storage.memory.add_memory(
            self._tenant_id,
            session.user_id,
            {
                "session_id": session.id,
                "content": "\n".join(texts),
            },
        )

    async def search_memory(self, key: str, query: str, limit: int = 10, agent_context: Any = None):
        # key 形如 "{app_name}/{user_id}"（utils.user_key），取末段作 user_id
        user_id = str(key).split("/")[-1]
        hits = await self._storage.memory.search_memory(self._tenant_id, user_id, query, top_k=limit)
        return SearchMemoryResponse(memories=hits)

    async def close(self) -> None:
        """Storage 生命周期由平台统一管理。"""
        return None


class PlatformKnowledgeBase(KnowledgeBase):
    """框架 KnowledgeBase ABC 的平台适配（RAG 检索，PRD 2.1 Knowledge 域）。

    与 Session/Memory 适配同构（PRD §0.7 方案 B）: 框架定 ABC，平台出实现。
    按租户构造（tenant_id 绑定），检索委托平台 KnowledgeStore；
    生产接向量库（pgvector 等）时替换 store 即可，接口不变。
    """

    def __init__(self, knowledge_store: Any, tenant_id: str) -> None:
        """
        Args:
            knowledge_store: 平台 KnowledgeStore（InMemory 占位 / 生产向量库）
            tenant_id: 租户隔离边界
        """
        self._store = knowledge_store
        self._tenant_id = tenant_id

    async def search(self, ctx: Any, req: Any) -> SearchResult:
        query = getattr(getattr(req, "query", None), "text", None) or ""
        top_k = getattr(getattr(req, "params", None), "rank_top_k", None) or 5
        hits = await self._store.search(self._tenant_id, query, top_k=top_k)
        return SearchResult(documents=[
            SearchDocument(document=hit["content"], score=hit["score"])  # type: ignore[arg-type]
            for hit in hits
        ])


def _extract_texts(session: Session) -> list[str]:
    """从框架 Session 的 events 中抽取完整文本（跳过流式分片）。"""
    out: list[str] = []
    for event in getattr(session, "events", None) or []:
        if getattr(event, "partial", False):
            continue
        content = getattr(event, "content", None)
        for part in getattr(content, "parts", None) or []:
            text = getattr(part, "text", None)
            if text:
                out.append(text)
    return out
