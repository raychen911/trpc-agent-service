"""worker 队列模式测试：fakeredis 模拟入队/消费/结果回传闭环。"""
import asyncio
from types import SimpleNamespace

from fakeredis import FakeRedis

from trpc_service.agent.runner import RunResult
from trpc_service.config.manager import ConfigManager
from trpc_service.config.tenant_config import TenantConfig
from trpc_service.worker import RESULT_PREFIX, TaskQueue, handle_task


class FakeRunner:
    app_name = "t1_app"

    async def run(self, user_id, session_id, message, files=None, agent_context=None):
        return RunResult(text=f"echo:{message}")


async def test_queue_roundtrip():
    """入队 → （worker 写结果）→ wait_result 取回。"""
    queue = TaskQueue("redis://unused")
    queue._redis = FakeRedis(decode_responses=True)
    request_id = await queue.enqueue(
        {"tenant_id": "t1", "message": "hi", "channel": "web", "trace_id": "tr1"}
    )
    assert queue._redis.llen("trpc:chat:tasks") == 1
    # 模拟 worker 写结果
    queue._redis.set(
        RESULT_PREFIX + request_id,
        '{"reply": "ok", "session_id": "s", "trace_id": "tr1"}',
        ex=60,
    )
    result = await queue.wait_result(request_id, timeout=1.0)
    assert result == {"reply": "ok", "session_id": "s", "trace_id": "tr1"}


async def test_wait_result_timeout_returns_none():
    queue = TaskQueue("redis://unused")
    queue._redis = FakeRedis(decode_responses=True)
    assert await queue.wait_result("no_such_id", timeout=0.2) is None


async def test_consume_forever_processes_and_replies():
    """consume 循环：取任务 → handler → 结果键 + processing 清空。"""
    queue = TaskQueue("redis://unused")
    queue._redis = FakeRedis(decode_responses=True)
    request_id = await queue.enqueue({"tenant_id": "t1", "message": "hello"})

    async def handler(task):
        assert task["message"] == "hello"
        return {"reply": "done", "session_id": "s", "trace_id": task["request_id"]}

    async def one_shot():
        task_raw = await asyncio.to_thread(
            queue._redis.brpoplpush, "trpc:chat:tasks", "trpc:chat:processing", 1
        )
        import json

        task = json.loads(task_raw)
        result = await handler(task)
        queue._redis.set(
            RESULT_PREFIX + task["request_id"], json.dumps(result), ex=60
        )
        await asyncio.to_thread(queue._redis.lrem, "trpc:chat:processing", 1, task_raw)

    await asyncio.wait_for(one_shot(), timeout=5)
    result = await queue.wait_result(request_id, timeout=1.0)
    assert result["reply"] == "done"
    assert queue._redis.llen("trpc:chat:processing") == 0


async def test_handle_task_executes_via_execute_chat():
    """handle_task：走完整 execute_chat 管线（预算/审计/指标）。"""
    from trpc_service.metrics.collector import metrics_collector

    metrics_collector.reset()
    state = SimpleNamespace(
        config_manager=ConfigManager(),
        runners={"t1": FakeRunner()},
    )
    state.config_manager.register(TenantConfig(tenant_id="t1", name="T1"))
    task = {"tenant_id": "t1", "message": "你好", "user_id": "u1", "channel": "web"}
    result = await handle_task(task, state)
    assert result["reply"] == "echo:你好"
    assert result["session_id"]
    snap = metrics_collector.snapshot()["t1"]
    assert snap["requests"] == 1 and snap["errors"] == 0
    metrics_collector.reset()


async def test_handle_task_unknown_tenant():
    state = SimpleNamespace(config_manager=ConfigManager(), runners={})
    result = await handle_task({"tenant_id": "ghost", "message": "hi"}, state)
    assert result["error"] == "tenant_not_found"
