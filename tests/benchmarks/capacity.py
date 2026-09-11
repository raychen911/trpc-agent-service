"""Real Runner/Redis-or-SQL/Inbox/Outbox burst benchmark with explicitly synthetic model usage."""

import argparse
import asyncio
import json
import math
import os
import platform
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from redis import Redis
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateSchema
from sqlalchemy.schema import DropSchema
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import LLMModel
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SqlSessionService
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part
from trpc_agent_sdk.types import UsageMetadata

from tests.conftest import build_tenant
from tests.test_real_runner_flow import FakeAdapter
from tests.test_real_runner_flow import inbound
from trpc_service.agent import RunnerRegistry
from trpc_service.persistence import Database
from trpc_service.reliability import DurableExecutionPipeline
from trpc_service.reliability import InboxRepository
from trpc_service.reliability import OutboxRepository
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SessionCoordinator
from trpc_service.telemetry.storage import percentile
from trpc_service.tenant import ChannelBindingRegistry
from trpc_service.tenant import MessageRouter
from trpc_service.tenant import SessionIdentityFactory
from trpc_service.tenant import verify_parse_and_route_callback


def distribution(values):
    return {"count": len(values), "mean": sum(values) / len(values) if values else 0,
            "p50": percentile(values, .50), "p95": percentile(values, .95), "p99": percentile(values, .99)}


def project_requirements(message_qps, mean_seconds, p95_seconds, total_tokens_per_message,
                         model_calls_per_message, concurrency, utilization=.65):
    """Token input is already total across all calls in a message; do not multiply twice."""
    return {
        "mean_inflight": message_qps * mean_seconds,
        "tail_headroom_inflight": message_qps * p95_seconds,
        "planning_workers": math.ceil(message_qps * p95_seconds / concurrency / utilization),
        "required_model_tpm": message_qps * 60 * total_tokens_per_message,
        "required_model_rpm": message_qps * 60 * model_calls_per_message,
    }


class PacedModel(LLMModel):
    def __init__(self, delay):
        super().__init__(model_name="capacity-synthetic")
        self.delay, self.calls, self.input_tokens, self.output_tokens = delay, 0, 0, 0

    @classmethod
    def supported_models(cls):
        return ["capacity-synthetic"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        await asyncio.sleep(self.delay)
        self.calls += 1
        self.input_tokens += 200
        self.output_tokens += 50
        usage = UsageMetadata(prompt_token_count=200, response_token_count=50, total_token_count=250)
        if any(part.function_response for part in request.contents[-1].parts):
            parts = [Part.from_text(text="capacity completed")]
        else:
            part = Part.from_function_call(name="lookup", args={"order_id": "synthetic-order"})
            part.function_call.id = f"call-{uuid.uuid4().hex}"
            parts = [part]
        yield LlmResponse(content=Content(role="model", parts=parts), usage_metadata=usage)


@contextmanager
def isolated_database():
    url = os.environ["TRPC_DATABASE_URL"]
    admin = create_engine(url)
    schema = f"bench_{uuid.uuid4().hex}"
    with admin.begin() as connection:
        connection.execute(CreateSchema(schema))
    database = Database(url, connect_args={"options": f"-csearch_path={schema}"}, pool_size=5, max_overflow=5)
    database.create_schema()
    try:
        yield database
    finally:
        database.close()
        try:
            with admin.begin() as connection:
                connection.execute(DropSchema(schema, cascade=True))
        finally:
            admin.dispose()


async def run_profile(concurrency, messages, delay, backend):
    with isolated_database() as database:
        service = (RedisSessionService(db_url=os.environ["TRPC_REDIS_URL"]) if backend == "redis" else
                   SqlSessionService(db_url=os.environ["TRPC_SESSION_DATABASE_URL"]))
        redis = Redis.from_url(os.environ["TRPC_REDIS_URL"], socket_timeout=3)
        suffix = uuid.uuid4().hex[:10]
        tenants = [build_tenant(f"tenant_bench_{suffix}_{i}", f"callback_{suffix}_{i}") for i in range(2)]
        registry, runners, models = RunnerRegistry(), [], []
        tool_calls = 0

        async def lookup(order_id: str) -> dict:
            """Deterministic read-only tool used to measure the real tool invocation path."""
            nonlocal tool_calls
            tool_calls += 1
            return {"order_id": order_id, "status": "ok"}

        for tenant in tenants:
            model = PacedModel(delay)
            runner = Runner(app_name=f"{tenant.tenant_id}:customer_support",
                            agent=LlmAgent(name="support_agent", model=model, tools=[FunctionTool(lookup)]),
                            session_service=service, enable_post_turn_processing=False,
                            close_session_service_on_close=False)
            registry.register(tenant.tenant_id, "customer_support", 3, runner)
            runners.append(runner)
            models.append(model)
        binding_registry = ChannelBindingRegistry(tenants)
        router = MessageRouter(binding_registry, SessionIdentityFactory(lambda _: b"capacity-test-key".ljust(32, b"!")))
        inbox, outbox = InboxRepository(database), OutboxRepository(database)
        pipeline = DurableExecutionPipeline(inbox, outbox, PostTurnRepository(database), SessionCoordinator(database))
        statements = 0
        sql_ms = []

        def before_sql(connection, cursor, statement, parameters, context, executemany):
            context.ops_benchmark_started = time.perf_counter()

        def after_sql(connection, cursor, statement, parameters, context, executemany):
            nonlocal statements
            statements += 1
            sql_ms.append((time.perf_counter() - context.ops_benchmark_started) * 1000)

        event.listen(Engine, "before_cursor_execute", before_sql)
        event.listen(Engine, "after_cursor_execute", after_sql)
        started, cpu_started = time.perf_counter(), time.process_time()
        redis_before = redis.info("stats")["total_commands_processed"]
        accept_ms, run_ms, queue_ms, end_to_end_ms, lag_ms = [], [], [], [], []
        accepted_at, failures, duplicate_count = {}, [], 0
        active, peak = 0, 0
        done = asyncio.Event()

        async def watch_loop():
            while not done.is_set():
                tick = time.perf_counter()
                await asyncio.sleep(.01)
                lag_ms.append(max(0, (time.perf_counter() - tick - .01) * 1000))

        async def consume(slot):
            nonlocal active, peak
            while True:
                work = inbox.claim(worker_id=f"bench-{slot}", lease_seconds=120)
                if work is None:
                    return
                begin = time.perf_counter()
                queue_ms.append((begin - accepted_at[work.execution_id]) * 1000)
                active += 1
                peak = max(peak, active)
                try:
                    outputs = await pipeline.execute(work, registry, worker_id=f"bench-{slot}")
                    assert outputs[-1].final and outputs[-1].text == "capacity completed"
                except Exception as error:
                    failures.append(type(error).__name__)
                finally:
                    active -= 1
                    run_ms.append((time.perf_counter() - begin) * 1000)
                    end_to_end_ms.append((time.perf_counter() - accepted_at[work.execution_id]) * 1000)

        monitor = asyncio.create_task(watch_loop())
        try:
            await asyncio.sleep(0)
            # Finite callback burst; this measures the repository/normalization path, not HTTP/IM limits.
            for index in range(messages):
                callback = f"callback_{suffix}_{index % 2}"
                message = inbound(callback, f"{suffix}-{index}", "benchmark order inquiry").model_copy(
                    update={"external_user_id": f"user-{index}"})
                begin = time.perf_counter()
                message, route = await verify_parse_and_route_callback(
                    adapter=FakeAdapter(message), raw_request=object(), channel=message.channel,
                    webhook_public_id=callback, registry=binding_registry, router=router,
                )
                receipt = pipeline.ingest(message, route, trace_id=f"trace-{suffix}-{index}")
                accepted_at[receipt.execution_id] = begin
                accept_ms.append((time.perf_counter() - begin) * 1000)
                if index % 5 == 0:
                    duplicate = pipeline.ingest(message, route, trace_id="duplicate")
                    assert duplicate.execution_id == receipt.execution_id
                    duplicate_count += 1
            admission_seconds = time.perf_counter() - started
            await asyncio.gather(*(consume(slot) for slot in range(concurrency)))
            elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu_started
            redis_commands = redis.info("stats")["total_commands_processed"] - redis_before
            event.remove(Engine, "before_cursor_execute", before_sql)
            event.remove(Engine, "after_cursor_execute", after_sql)
            with database.sessions() as session:
                succeeded = session.execute(text("SELECT count(*) FROM inbound_messages WHERE status='succeeded'"))
                assert succeeded.scalar_one() == messages - len(failures)
                outbox_pending = session.execute(text("SELECT count(*) FROM outbox_messages")).scalar_one()
            tokens = sum(model.input_tokens + model.output_tokens for model in models)
            if not failures:
                assert tool_calls == messages, "duplicate callback caused repeated tool execution"
                assert sum(model.calls for model in models) == messages * 2
            return {
                "backend": backend, "concurrency_limit": concurrency, "observed_peak_active_sessions": peak,
                "messages": messages, "duplicate_callbacks": duplicate_count, "failures": failures,
                "elapsed_seconds": elapsed, "completed_messages_per_second": (messages - len(failures)) / elapsed,
                "callback_repository_ops_per_second": (messages + duplicate_count) / admission_seconds,
                "callback_latency_ms": distribution(accept_ms), "run_latency_ms": distribution(run_ms),
                "queue_wait_ms": distribution(queue_ms), "input_to_outbox_ms": distribution(end_to_end_ms),
                "sql_statements": statements, "sql_statements_per_second": statements / elapsed,
                "sql_statement_latency_ms": distribution(sql_ms), "redis_commands": redis_commands,
                "redis_commands_per_second": redis_commands / elapsed, "event_loop_lag_ms": distribution(lag_ms),
                "cpu_seconds": cpu, "cpu_core_equivalent": cpu / elapsed,
                "synthetic_total_tokens": tokens, "synthetic_tokens_per_message": tokens / messages,
                "model_calls": sum(model.calls for model in models), "tool_calls": tool_calls,
                "initial_queue_backlog": messages, "outbox_undispatched": outbox_pending,
                "planning_example_10_msg_s": project_requirements(
                    10, distribution(run_ms)["mean"] / 1000, distribution(run_ms)["p95"] / 1000,
                    tokens / messages, sum(model.calls for model in models) / messages, concurrency),
            }
        finally:
            done.set()
            await monitor
            if event.contains(Engine, "before_cursor_execute", before_sql):
                event.remove(Engine, "before_cursor_execute", before_sql)
                event.remove(Engine, "after_cursor_execute", after_sql)
            for runner in runners:
                await runner.close()
            await service.close()
            redis.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", default="1,4,8,16")
    parser.add_argument("--messages", type=int, default=64)
    parser.add_argument("--model-delay", type=float, default=.05)
    parser.add_argument("--backend", choices=("redis", "sql"), default="redis")
    parser.add_argument("--output")
    args = parser.parse_args()
    levels = [int(value) for value in args.concurrency.split(",")]
    if args.messages < 1 or args.model_delay < 0 or any(value < 1 for value in levels):
        parser.error("messages/concurrency must be positive and delay nonnegative")
    environment = {"python": platform.python_version(), "platform": platform.platform(),
                   "logical_cpus_visible": os.cpu_count()}
    for name in ("cpu.max", "memory.max"):
        path = Path("/sys/fs/cgroup") / name
        environment[name.replace(".", "_")] = path.read_text().strip() if path.exists() else "unavailable"
    report = {"environment": environment,
              "scope": "single process, finite burst, real Runner/storage, simulated model/IM, no dispatch",
              "model_delay_per_call_seconds": args.model_delay, "token_source": "synthetic UsageMetadata",
              "qps_scope": "SQL client statements; Redis server-global command delta incl. observer/healthcheck",
              "profiles": []}
    for level in levels:
        result = await run_profile(level, args.messages, args.model_delay, args.backend)
        report["profiles"].append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
    if args.output:
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if any(profile["failures"] for profile in report["profiles"]):
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
