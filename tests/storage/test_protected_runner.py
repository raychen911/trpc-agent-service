"""Public upstream Runner, actual tool execution, durable pipeline and two tenants."""

import pytest

from tests.conftest import build_tenant
from tests.test_real_runner_flow import build_real_runner, inbound
from trpc_service.agent import RunnerRegistry, tenant_app_name
from trpc_service.persistence import Database
from trpc_service.reliability import DurableExecutionPipeline, InboxRepository, OutboxRepository
from trpc_service.reliability import PostTurnRepository, SessionCoordinator
from trpc_service.storage import GuardedSessionService
from trpc_service.storage.protected_session import ProtectedSessionService
from trpc_service.tenant import ChannelBindingRegistry, MessageRouter, SessionIdentityFactory


@pytest.mark.asyncio
async def test_two_tenant_runner_tool_inbox_session_outbox_and_reopen(tmp_path):
    database = Database("sqlite:///" + (tmp_path / "platform.db").as_posix())
    database.create_schema()
    inbox, outbox = InboxRepository(database), OutboxRepository(database)
    pipeline = DurableExecutionPipeline(inbox, outbox, PostTurnRepository(database), SessionCoordinator(database))
    tenants = [build_tenant("tenant_alpha", "callback_alpha"), build_tenant("tenant_beta", "callback_beta")]
    router = MessageRouter(ChannelBindingRegistry(tenants), SessionIdentityFactory(lambda _: b"x" * 32))
    registry, services, runners, calls = RunnerRegistry(), [], [], []
    routes = []
    url = "sqlite:///" + (tmp_path / "shared.db").as_posix()
    try:
        for tenant in tenants:
            app = tenant.applications[0]
            native = ProtectedSessionService.sql(url, tenant_app_name(tenant.tenant_id, app.app_id))
            await native.initialize()
            services.append(native)
            guarded = GuardedSessionService(native, tenant.tenant_id, app.app_id)
            runner, _, _ = build_real_runner(tenant, guarded, calls)
            runners.append(runner)
            key = tenant.tenant_id, app.app_id, tenant.config_version
            registry.register(*key, runner)
            registry.protected_sessions[key] = native
            for turn in range(2):
                message = inbound(tenant.channel_bindings[0].webhook_public_id, tenant.tenant_id + str(turn),
                                  "private " + tenant.tenant_id)
                route = router.route_message(message)
                receipt = pipeline.ingest(message, route, trace_id="trace-test")
                assert pipeline.ingest(message, route, trace_id="duplicate").execution_id == receipt.execution_id
                work = inbox.claim(worker_id="worker" + str(turn), lease_seconds=30)
                output = await pipeline.execute(work, registry, worker_id="worker" + str(turn))
                assert output[-1].text.startswith(tenant.tenant_id)
                assert outbox.list_for_inbound(tenant.tenant_id, work.inbound_message_id)[-1].payload["final"]
            routes.append(route)
        assert routes[0].session_id != routes[1].session_id
        assert len(calls) == 4
        for native, route in zip(services, routes):
            reader = ProtectedSessionService.sql(url, native.app_name)
            try:
                session = await reader.get_session(app_name=native.app_name,
                                                   user_id=route.internal_user_id,
                                                   session_id=route.session_id)
                assert len(session.events) >= 8
                assert all((e.custom_metadata or {}).get("platform_execution_id") for e in session.events)
                texts = " ".join(e.get_text() for e in session.events)
                other = "tenant_beta" if route.tenant_id == "tenant_alpha" else "tenant_alpha"
                assert other not in texts
            finally:
                await reader.close()
    finally:
        for runner in runners:
            await runner.close()
        for service in services:
            await service.close()
        database.close()
