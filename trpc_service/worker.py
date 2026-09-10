"""Worker 节点：独立进程消费聊天任务队列。"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Callable, Optional

QUEUE_KEY = "trpc:chat:tasks"
PROCESSING_KEY = "trpc:chat:processing"
RESULT_PREFIX = "trpc:chat:result:"
RESULT_TTL_SECONDS = 60
WORKER_TASK_TIMEOUT = float(os.getenv("WORKER_TASK_TIMEOUT", "120"))


class TaskQueue:
    """Redis 任务队列（LPUSH 入队 / BRPOPLPUSH 消费 / 结果键回传）。"""

    def __init__(self, redis_url: str):
        import redis

        self._redis = redis.Redis.from_url(
            redis_url, decode_responses=True, socket_connect_timeout=2
        )

    async def enqueue(self, task: dict) -> str:
        """入队一个聊天任务，返回 request_id（结果键的查找凭据）。"""
        request_id = uuid.uuid4().hex
        task = {**task, "request_id": request_id}
        await asyncio.to_thread(self._redis.lpush, QUEUE_KEY, json.dumps(task))
        return request_id

    async def wait_result(self, request_id: str, timeout: float = 60.0) -> Optional[dict]:
        """轮询结果键；超时返回 None（gateway 转 504）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            raw = await asyncio.to_thread(self._redis.get, RESULT_PREFIX + request_id)
            if raw:
                return json.loads(raw)
            await asyncio.sleep(0.1)
        return None

    async def consume_forever(self, handler: Callable[[dict], dict]) -> None:
        """worker 主循环：取任务 → 执行 → 写结果 → 移出 processing。"""
        while True:
            raw = await asyncio.to_thread(
                self._redis.brpoplpush, QUEUE_KEY, PROCESSING_KEY, 5
            )
            if not raw:
                continue
            try:
                task = json.loads(raw)
                result = await asyncio.wait_for(handler(task), timeout=WORKER_TASK_TIMEOUT)
            except asyncio.TimeoutError:
                result = {"reply": "", "session_id": "", "trace_id": "", "error": "worker_timeout"}
            except Exception as exc:  # noqa: BLE001  单任务失败不拖垮 worker
                from trpc_service.log import get_logger

                get_logger("worker").error("task failed err=%s", exc)
                result = {"reply": "", "session_id": "", "trace_id": "", "error": type(exc).__name__}
            request_id = task.get("request_id", "")
            if request_id:
                await asyncio.to_thread(
                    self._redis.set,
                    RESULT_PREFIX + request_id,
                    json.dumps(result),
                    ex=RESULT_TTL_SECONDS,
                )
            await asyncio.to_thread(self._redis.lrem, PROCESSING_KEY, 1, raw)


async def handle_task(task: dict, state) -> dict:
    """执行单个聊天任务（gateway 传来的 payload → execute_chat）。"""
    from trpc_service.chat import ChatBlocked, execute_chat

    tenant_config = state.config_manager.get(task.get("tenant_id", ""))
    if not tenant_config:
        return {"reply": "", "session_id": "", "trace_id": "", "error": "tenant_not_found"}
    runner = state.runners.get(task["tenant_id"])
    if not runner:
        return {"reply": "", "session_id": "", "trace_id": "", "error": "agent_not_ready"}
    try:
        return await execute_chat(
            tenant_config,
            runner,
            message=task.get("message", ""),
            user_id=task.get("user_id", "web_user"),
            session_id=task.get("session_id"),
            channel=task.get("channel", "web"),
            trace_id=task.get("trace_id"),
        )
    except ChatBlocked:
        return {
            "reply": "今日使用额度已用完，请明日再试。",
            "session_id": task.get("session_id", ""),
            "trace_id": task.get("trace_id", ""),
        }


def build_worker_state():
    """worker 侧装配：与 web lifespan 同源（配置/存储/审计/Runner）。"""
    from types import SimpleNamespace

    from trpc_service.agent.factory import AgentFactory
    from trpc_service.agent.runner import AgentRunner
    from trpc_service.config.manager import ConfigManager
    from trpc_service.metrics.setup import setup_telemetry
    from trpc_service.tenant.audit.service import audit_service
    from trpc_service.tenant.storage.database import Database, platform_db_url
    from trpc_service.tenant.storage.factory import create_storage

    setup_telemetry()
    state = SimpleNamespace(config_manager=ConfigManager(), runners={})

    db_url = platform_db_url()
    if db_url:
        from trpc_service.tenant.sql_store import SqlTenantStore

        state.database = Database(db_url)
        state.tenant_store = SqlTenantStore(state.database)
        audit_service.attach(state.database)
        for tenant_config in state.tenant_store.load_all_configs():
            if not state.config_manager.get(tenant_config.tenant_id):
                state.config_manager.register(tenant_config)

    for tenant_id in state.config_manager.all():
        tenant_config = state.config_manager.get(tenant_id)
        agent = AgentFactory.create_agent(tenant_config)
        storage = create_storage(tenant_config)
        state.runners[tenant_id] = AgentRunner(
            app_name=tenant_config.app.app_name,
            agent=agent,
            session_service=storage.session_service,
            memory_service=storage.memory_service,
        )
    return state


async def run_worker() -> None:
    """装配状态 → 常驻消费循环。"""
    state = build_worker_state()
    queue = TaskQueue(os.getenv("REDIS_URL", "redis://localhost:6379/3"))
    from trpc_service.log import get_logger

    get_logger("worker").info(
        "worker started tenants=%s queue=%s", list(state.runners), QUEUE_KEY
    )
    try:
        await queue.consume_forever(lambda task: handle_task(task, state))
    finally:
        from trpc_service.tenant.audit.service import audit_service

        await audit_service.stop()


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
