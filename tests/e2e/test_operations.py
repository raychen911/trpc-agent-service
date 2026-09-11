"""Operational evidence: real connection loss, admission safety, and retry boundaries."""

import asyncio
import json
import os
import subprocess
import sys
import uuid

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from trpc_agent_sdk.sessions import RedisSessionService

from tests.conftest import build_tenant
from tests.test_real_runner_flow import FakeAdapter
from tests.test_real_runner_flow import build_real_runner
from tests.test_real_runner_flow import execute_callback
from tests.test_real_runner_flow import inbound
from tests.test_real_runner_flow import SensitiveErrorModel
from tests.test_real_runner_flow import DeterministicOrderModel
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.tools import FunctionTool
from trpc_service.agent import RunnerRegistry
from trpc_service.persistence import Database
from trpc_service.reliability import InboxDisposition
from trpc_service.reliability import InboxRepository
from trpc_service.reliability import DurableExecutionPipeline
from trpc_service.reliability import OutboxRepository
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SessionCoordinator
from trpc_service.tenant import ChannelBindingRegistry
from trpc_service.tenant import MessageRouter
from trpc_service.tenant import SessionIdentityFactory
from trpc_service.tenant import verify_parse_and_route_callback
from trpc_service.web import create_app

from .conftest import required_env
from .tcp_fault import TcpFaultGate


@pytest.mark.asyncio
async def test_failed_read_only_tool_is_not_blindly_retried_or_exposed(session_backends):
    tenant = build_tenant(f"tenant_tool_{session_backends.suffix}", f"callback_{session_backends.suffix}")
    bindings = ChannelBindingRegistry([tenant])
    router = MessageRouter(bindings, SessionIdentityFactory(lambda _: b"tool-test".ljust(32, b"!")))
    calls = 0

    async def search_order(order_id: str) -> dict:
        """Read-only test tool whose upstream service is unavailable."""
        nonlocal calls
        calls += 1
        raise RuntimeError("private-upstream-password: simulated tool service failure")

    model = DeterministicOrderModel(tenant.tenant_id, 3)
    runner = Runner(app_name=f"{tenant.tenant_id}:customer_support",
                    agent=LlmAgent(name="support_agent", model=model, tools=[FunctionTool(search_order)]),
                    session_service=session_backends.source, enable_post_turn_processing=False,
                    close_session_service_on_close=False)
    registry = RunnerRegistry()
    registry.register(tenant.tenant_id, "customer_support", 3, runner)
    try:
        _, outputs = await execute_callback(
            FakeAdapter(inbound(tenant.channel_bindings[0].webhook_public_id, "tool-failure", "lookup")),
            bindings, router, registry,
        )
        assert calls == 1
        assert "private-upstream-password" not in repr(outputs)
        assert any(part.function_response for content in model.requests[-1].contents for part in content.parts)
    finally:
        await runner.close()


@pytest.mark.asyncio
async def test_model_timeout_error_must_not_be_recorded_as_success(platform_database, session_backends):
    tenant = build_tenant(f"tenant_timeout_{session_backends.suffix}", f"callback_{session_backends.suffix}")
    bindings = ChannelBindingRegistry([tenant])
    router = MessageRouter(bindings, SessionIdentityFactory(lambda _: b"timeout-only-test-identity-key!!!"))
    model = SensitiveErrorModel(model_name="sensitive-error-model")
    runner = Runner(app_name=f"{tenant.tenant_id}:customer_support",
                    agent=LlmAgent(name="support_agent", model=model), session_service=session_backends.source,
                    enable_post_turn_processing=False, close_session_service_on_close=False)
    registry = RunnerRegistry()
    registry.register(tenant.tenant_id, "customer_support", 3, runner)
    inbox = InboxRepository(platform_database)
    pipeline = DurableExecutionPipeline(inbox, OutboxRepository(platform_database),
                                        PostTurnRepository(platform_database), SessionCoordinator(platform_database))
    message = inbound(tenant.channel_bindings[0].webhook_public_id, "model-timeout", "timeout")
    try:
        message, route = await verify_parse_and_route_callback(
            adapter=FakeAdapter(message), raw_request=object(), channel=message.channel,
            webhook_public_id=message.webhook_public_id, registry=bindings, router=router,
        )
        receipt = pipeline.ingest(message, route, trace_id="timeout-trace")
        work = inbox.claim(worker_id="timeout-worker", lease_seconds=30)
        outputs = await pipeline.execute(work, registry, worker_id="timeout-worker")
        assert outputs[-1].error_type == "agent_error"
        assert "private API key" not in repr(outputs)
        with platform_database.sessions() as session:
            status = session.execute(text(
                "SELECT status FROM inbound_messages WHERE inbound_message_id=:id"
            ), {"id": receipt.inbound_message_id}).scalar_one()
        assert status == "failed_final"
        assert inbox.claim(worker_id="no-blind-retry", lease_seconds=30) is None
        assert PostTurnRepository(platform_database).claim(limit=10, worker_id="post", lease_seconds=30) == []
    finally:
        await runner.close()


@pytest.fixture
def real_services(request):
    if request.config.getoption("--backend-mode") != "real":
        pytest.skip("TCP outage tests require real Redis/PostgreSQL")


@pytest.mark.asyncio
async def test_process_crash_before_execution_is_reclaimed_with_same_execution_id(platform_database):
    url = make_url(platform_database.url)
    if url.get_backend_name() == "postgresql":
        with platform_database.sessions() as session:
            schema = session.execute(text("SELECT current_schema()")).scalar_one()
        url = url.update_query_dict({"options": f"-csearch_path={schema}"})
    repository = InboxRepository(platform_database)
    receipt = repository.accept(
        tenant_id="tenant_ops", channel_binding_id="binding_ops", external_message_id="crash-before-run",
        payload={"text": "durable"}, request_id="request-crash", trace_id="trace-crash",
        partition_key="tenant_ops:session-crash", config_version=3, storage_revision=4,
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "tests.e2e.lease_claim"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        env={**os.environ, "OPS_WORKER_DATABASE_URL": url.render_as_string(hide_password=False)},
    )
    try:
        line = await asyncio.wait_for(asyncio.to_thread(process.stdout.readline), timeout=20)
        assert line.startswith("OPS_CLAIM="), "child did not claim the durable input"
        assert json.loads(line.removeprefix("OPS_CLAIM="))["execution_id"] == receipt.execution_id
        process.kill()
        await asyncio.to_thread(process.wait, 5)
        await asyncio.sleep(0.35)
        recovered = repository.claim(worker_id="replacement-worker", lease_seconds=30)
        assert recovered.execution_id == receipt.execution_id
        assert recovered.inbound_message_id == receipt.inbound_message_id
        assert recovered.attempt == 2
    finally:
        if process.poll() is None:
            process.kill()
            await asyncio.to_thread(process.wait, 5)
        process.stdout.close()


def test_sql_disconnect_revokes_readiness_preserves_liveness_and_recovers(real_services, platform_database):
    upstream = make_url(required_env("TRPC_DATABASE_URL"))
    with platform_database.sessions() as session:
        schema = session.execute(text("SELECT current_schema()")).scalar_one()
    with TcpFaultGate(upstream.host, upstream.port or 5432) as gate:
        url = upstream.set(host="127.0.0.1", port=gate.port).update_query_dict({
            "connect_timeout": "2", "options": f"-csearch_path={schema} -cstatement_timeout=2000",
        }).render_as_string(hide_password=False)
        with TestClient(create_app(url)) as client:
            assert client.get("/health/ready").status_code == 200
            gate.disconnect()
            assert client.get("/health/ready").status_code == 503
            assert client.get("/health/live").status_code == 200
            assert client.get("/health/ready").json() == {"status": "unavailable"}
            gate.reconnect()
            assert client.get("/health/ready").status_code == 200


def test_sql_outage_does_not_ack_input_and_retry_after_recovery_deduplicates(real_services, platform_database):
    upstream = make_url(required_env("TRPC_DATABASE_URL"))
    with platform_database.sessions() as session:
        schema = session.execute(text("SELECT current_schema()")).scalar_one()
    with TcpFaultGate(upstream.host, upstream.port or 5432) as gate:
        url = upstream.set(host="127.0.0.1", port=gate.port).update_query_dict({
            "connect_timeout": "2", "options": f"-csearch_path={schema}",
        }).render_as_string(hide_password=False)
        database = Database(url)
        repository = InboxRepository(database)
        values = dict(tenant_id="tenant_ops", channel_binding_id="binding_ops", external_message_id="input-1",
                      payload={"text": "safe retry"}, request_id="request-1", trace_id="trace-1",
                      partition_key="tenant_ops:session-1", config_version=1, storage_revision=1)
        try:
            gate.disconnect()
            with pytest.raises(OperationalError):
                repository.accept(**values)
            assert InboxRepository(platform_database).claim(worker_id="other", lease_seconds=30) is None
            gate.reconnect()
            accepted = repository.accept(**values)
            duplicate = repository.accept(**{**values, "request_id": "retry-request"})
            assert accepted.disposition is InboxDisposition.ACCEPTED
            assert duplicate.disposition is InboxDisposition.DUPLICATE
            assert accepted.execution_id == duplicate.execution_id
        finally:
            database.close()


@pytest.mark.asyncio
async def test_redis_session_outage_blocks_real_runner_before_model_and_tool(real_services):
    upstream = make_url(required_env("TRPC_REDIS_URL"))
    suffix = uuid.uuid4().hex[:10]
    tenant = build_tenant(f"tenant_ops_{suffix}", f"callback_{suffix}")
    bindings = ChannelBindingRegistry([tenant])
    router = MessageRouter(bindings, SessionIdentityFactory(lambda _: b"ops-test-identity-key-32-bytes!!!"))
    calls = []
    with TcpFaultGate(upstream.host, upstream.port or 6379) as gate:
        url = upstream.set(host="127.0.0.1", port=gate.port).render_as_string(hide_password=False)
        service = RedisSessionService(db_url=url, socket_timeout=0.5, socket_connect_timeout=0.5)
        runner, model, _ = build_real_runner(tenant, service, calls)
        registry = RunnerRegistry()
        registry.register(tenant.tenant_id, "customer_support", 3, runner)
        adapter = FakeAdapter(inbound(f"callback_{suffix}", "before-outage", "remember me"))
        try:
            route, _ = await execute_callback(adapter, bindings, router, registry)
            before_model, before_tools = len(model.requests), len(calls)
            gate.disconnect()
            with pytest.raises((RedisError, ConnectionError, TimeoutError)):
                await asyncio.wait_for(execute_callback(
                    FakeAdapter(inbound(f"callback_{suffix}", "during-outage", "do not run tools")),
                    bindings, router, registry,
                ), timeout=5)
            assert len(model.requests) == before_model
            assert len(calls) == before_tools
            gate.reconnect()
            _, outputs = await execute_callback(
                FakeAdapter(inbound(f"callback_{suffix}", "after-outage", "continue")),
                bindings, router, registry,
            )
            assert outputs[-1].final
            session = await service.get_session(app_name=runner.app_name, user_id=route.internal_user_id,
                                                session_id=route.session_id)
            assert "remember me" in repr(session.events)
        finally:
            gate.reconnect()
            await runner.close()
            await service.close()
