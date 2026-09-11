"""Trusted routing -> Inbox -> real Runner/tool -> Session -> Outbox across a backend change.

Configuration publication and dispatch are explicitly driven by the test. This is
an application-path integration test, not a claim that CLI Workers consume jobs.
"""

import pytest

from tests.conftest import build_tenant
from tests.test_real_runner_flow import FakeAdapter
from tests.test_real_runner_flow import build_real_runner
from tests.test_real_runner_flow import inbound
from trpc_service.agent import RunnerRegistry
from trpc_service.migration import copy_and_verify_trpc_session
from trpc_service.reliability import DurableExecutionPipeline
from trpc_service.reliability import InboxRepository
from trpc_service.reliability import OutboxRepository
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SessionCoordinator
from trpc_service.storage import BackendCapability
from trpc_service.storage import BackendProfile
from trpc_service.storage import BackendProfileRegistry
from trpc_service.storage import TenantBackendBinding
from trpc_service.storage import TenantStorageResolver
from trpc_service.tenant import BackendKind
from trpc_service.tenant import ChannelBindingRegistry
from trpc_service.tenant import MessageRouter
from trpc_service.tenant import SecretRef
from trpc_service.tenant import SessionIdentityFactory
from trpc_service.tenant import verify_parse_and_route_callback

from .seed import canonical_session


@pytest.mark.asyncio
async def test_message_pipeline_continues_after_explicit_backend_switch_and_reverse_sync(
    session_backends, platform_database, request,
):
    suffix = session_backends.suffix
    tenant_id, webhook = f"tenant_flow_{suffix}", f"callback_{suffix}"
    tenant = build_tenant(tenant_id, webhook)
    profiles = BackendProfileRegistry([
        BackendProfile(
            profile_id=name, kind=kind, provider="test-configured", endpoint="test-managed",
            secret_ref=SecretRef(uri=f"vault://e2e/{name}"), profile_revision=1,
            capabilities={BackendCapability.SESSION, BackendCapability.SUMMARY},
        ) for name, kind in (
            ("source_backend", BackendKind.REDIS if request.config.getoption("--backend-mode") == "real"
             else BackendKind.IN_MEMORY), ("target_backend", BackendKind.SQL))
    ])
    services = {"source_backend": session_backends.source, "target_backend": session_backends.target}
    resolver = TenantStorageResolver(profiles, lambda profile, resource: services[profile.profile_id])
    bundles, runners, calls = [], [], []
    inbox, outbox = InboxRepository(platform_database), OutboxRepository(platform_database)
    tasks = PostTurnRepository(platform_database)
    pipeline = DurableExecutionPipeline(inbox, outbox, tasks, SessionCoordinator(platform_database))
    identities = SessionIdentityFactory(lambda _: b"test-only-migration-identity-key!!")

    async def turn(config_version, storage_revision, profile, text):
        values = tenant.model_dump()
        values.update(config_version=config_version, storage_revision=storage_revision)
        for resource in ("session", "summary"):
            values["data_backends"][resource].update(kind=profiles.resolve(profile, 1).kind, profile_id=profile)
        config = type(tenant).model_validate(values)
        bundle = await resolver.resolve(
            tenant_id=tenant_id, agent_app_id="customer_support", config_version=config_version,
            storage_revision=storage_revision,
            bindings=[TenantBackendBinding(
                tenant_id=tenant_id, agent_app_id="customer_support", config_version=config_version,
                storage_revision=storage_revision, resource_type=resource, profile_id=profile,
                profile_revision=1, namespace=tenant_id,
            ) for resource in ("session", "summary")],
        )
        bundles.append(bundle)
        runner, model, _ = build_real_runner(config, bundle.session.scoped_service, calls)
        runners.append(runner)
        registry = RunnerRegistry()
        registry.register(tenant_id, "customer_support", config_version, runner)
        bindings = ChannelBindingRegistry([config])
        message, route = await verify_parse_and_route_callback(
            adapter=FakeAdapter(inbound(webhook, f"{suffix}-{config_version}", text)), raw_request=object(),
            channel=config.channel_bindings[0].channel, webhook_public_id=webhook, registry=bindings,
            router=MessageRouter(bindings, identities),
        )
        receipt = pipeline.ingest(message, route, trace_id=f"trace-{suffix}-{config_version}")
        work = inbox.claim(worker_id=f"worker-{config_version}", lease_seconds=60)
        assert work is not None and work.execution_id == receipt.execution_id
        outputs = await pipeline.execute(work, registry, worker_id=f"worker-{config_version}")
        assert outputs[-1].final and outputs[-1].text == f"{tenant_id} completed {message.request_id}"
        assert "private-result" not in repr(outputs)
        stored = outbox.list_for_inbound(tenant_id, receipt.inbound_message_id)
        assert [item.payload for item in stored] == [event.model_dump(mode="json") for event in outputs]
        assert [item.part_no for item in stored] == list(range(len(outputs)))
        return route, model

    try:
        route, _ = await turn(3, 4, "source_backend", "迁移前：订单咨询🙂")
        key = dict(app_name=f"{tenant_id}:customer_support",
                   user_id=route.internal_user_id, session_id=route.session_id)
        before = canonical_session(await session_backends.source.get_session(**key))
        await copy_and_verify_trpc_session(
            session_backends.source, session_backends.target, target_writes_paused=True, **key)
        assert canonical_session(await session_backends.reopen_target().get_session(**key)) == before
        switched, model = await turn(4, 5, "target_backend", "切换 SQL 后：继续查询")
        assert switched.session_id == route.session_id
        assert "迁移前：订单咨询🙂" in repr(model.requests[0].contents)
        assert canonical_session(await session_backends.source.get_session(**key)) == before
        await copy_and_verify_trpc_session(
            session_backends.target, session_backends.source, target_writes_paused=True, **key)
        rolled_back, model = await turn(5, 6, "source_backend", "反向同步后：再次查询")
        assert rolled_back.session_id == route.session_id
        assert "切换 SQL 后：继续查询" in repr(model.requests[0].contents)
        assert len(calls) == 3
        assert [call["config_version"] for call in calls] == [3, 4, 5]
    finally:
        for runner in runners:
            await runner.close()
        for bundle in bundles:
            await bundle.release()
        # Raw services are owned and closed by session_backends.
