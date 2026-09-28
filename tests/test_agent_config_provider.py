from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

from dataclasses import replace

from trpc_service.agent.contracts import AgentExecutionRequest
from trpc_service.agent.configuration import AgentConfigurationError
from trpc_service.agent.recovery import RecoveryPolicy
from trpc_service.agent.runtime import DatabaseAgentConfigProvider
from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    IncomingMessage,
    MessageKind,
)
from trpc_service.config import Settings
from trpc_service.tenant.context import TenantContext
from tests.conftest import create_test_app


def test_agent_configuration_failure_keeps_stable_error_code() -> None:
    """Operational records must identify the broken policy, not only PermissionError."""

    error = AgentConfigurationError(
        "AGENT_MODEL_PROFILE_MISSING",
        "Agent App has no governed Model Profile",
        "Agent is not executable: platform model profile is not configured",
    )

    decision = RecoveryPolicy().classify_agent(error)

    assert decision.error_code == "AGENT_MODEL_PROFILE_MISSING"


@pytest.mark.anyio
async def test_database_config_provider_applies_and_disables_tenant_model_profile(
    tmp_path: Path, ) -> None:
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'runtime-config.db'}",
        auto_create_schema=True,
        admin_bootstrap_token=SecretStr("runtime-config-admin"),
    )
    app = create_test_app(settings)
    transport = httpx.ASGITransport(app=app)
    headers = {
        "Authorization": "Bearer runtime-config-admin",
        "X-Support-Reason": "runtime configuration integration test",
    }

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers=headers,
        ) as client:
            tenant = await client.post("/api/v1/tenants", json={"name": "Runtime Config"})
            tenant_id = UUID(tenant.json()["tenant_id"])
            scaffolded = await client.post(
                f"/api/v1/tenants/{tenant_id}/agents",
                json={"name": "Scaffolded Before Model Policy"},
            )
            scaffolded_id = scaffolded.json()["agent_app_id"]
            catalog = await client.post(
                "/api/v1/admin/model-catalog",
                json={
                    "provider": "bailian",
                    "model_name": "qwen-max",
                    "display_name": "Qwen Max",
                    "default_limits": {
                        "max_output_tokens": 2048,
                        "context_window_tokens": 32768
                    },
                },
            )
            credential = await client.post(
                "/api/v1/admin/model-credentials",
                json={
                    "provider": "bailian",
                    "name": "runtime-bailian",
                    "secret_ref": "env://DASHSCOPE_API_KEY",
                },
            )
            profile = await client.post(
                f"/api/v1/tenants/{tenant_id}/model-profiles",
                json={
                    "name": "runtime-primary",
                    "model_catalog_id": catalog.json()["model_catalog_id"],
                    "credential_id": credential.json()["model_credential_id"],
                    "parameter_config": {
                        "temperature": 0.4,
                        "enable_thinking": False
                    },
                    "limits": {
                        "daily_tokens": 100000
                    },
                },
            )
            profile_id = profile.json()["model_profile_id"]
            attached = await client.get(f"/api/v1/tenants/{tenant_id}/agents/{scaffolded_id}")

            assert scaffolded.status_code == 201
            assert scaffolded.json()["model_profile_id"] is None
            assert attached.json()["model_profile_id"] == profile_id
            assert attached.json()["stable_config_version"] == 2

            agent = await client.post(
                f"/api/v1/tenants/{tenant_id}/agents",
                json={
                    "name": "Runtime Agent",
                    "model_profile_id": profile_id,
                    "application_config": {
                        "instruction": "Use tenant runtime config."
                    },
                },
            )
            agent_id = UUID(agent.json()["agent_app_id"])
            context = TenantContext(
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                config_version=1,
                request_id="runtime-config-request",
                trace_id=str(uuid4()),
            )
            request = AgentExecutionRequest(
                tenant=context,
                session_id="runtime-config-session",
                incoming=IncomingMessage(
                    external_message_id="runtime-config-message",
                    principal_id="runtime-user",
                    conversation_id="runtime-conversation",
                    kind=MessageKind.TEXT,
                    occurred_at=datetime.now(timezone.utc),
                    text="hello",
                ),
                channel=ChannelBindingConfig(
                    binding_id=uuid4(),
                    tenant_id=tenant_id,
                    agent_app_id=agent_id,
                    channel_type="wecom",
                ),
            )
            provider = DatabaseAgentConfigProvider(settings, app.state.session_factory)
            loaded = await provider.load(request)
            loaded_backends = await provider.load_backends(context)

            assert loaded.model["model_name"] == "qwen-max"
            assert loaded.model["api_key_ref"] == "env://DASHSCOPE_API_KEY"
            assert loaded.model["temperature"] == 0.4
            assert loaded.model["enable_thinking"] is False
            assert loaded.model["max_output_tokens"] == 2048
            assert loaded.model["context_window_tokens"] == 32768
            assert loaded.application["instruction"] == "Use tenant runtime config."
            assert loaded.policy["limits"] == {"daily_tokens": 100000}
            assert loaded_backends == settings.storage_profile.model_dump(mode="json")

            drafted = await client.post(
                f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions",
                json={
                    "application_config": {
                        "instruction": "Use released version two."
                    },
                    "reason": "validate exact runtime configuration loading",
                },
            )
            released = await client.post(
                f"/api/v1/tenants/{tenant_id}/agents/{agent_id}/config-versions/2/release",
                json={
                    "mode": "stable",
                    "reason": "promote validated runtime configuration",
                },
            )
            version_two = replace(
                request,
                tenant=context.model_copy(update={"config_version": 2}),
            )
            loaded_two = await provider.load(version_two)
            missing_version = replace(
                request,
                tenant=context.model_copy(update={"config_version": 3}),
            )

            assert drafted.status_code == 201
            assert released.status_code == 200
            assert loaded_two.config_version == 2
            assert loaded_two.application["instruction"] == "Use released version two."
            with pytest.raises(PermissionError, match="version is not released"):
                await provider.load(missing_version)
            with pytest.raises(PermissionError, match="version is not released"):
                await provider.load_backends(missing_version.tenant)

            inherited = await client.post(
                f"/api/v1/tenants/{tenant_id}/agents",
                json={"name": "Inherited Profile Agent"},
            )
            inherited_agent_id = UUID(inherited.json()["agent_app_id"])
            inherited_context = context.model_copy(update={"agent_app_id": inherited_agent_id})
            inherited_request = replace(
                request,
                tenant=inherited_context,
                channel=replace(request.channel, agent_app_id=inherited_agent_id),
            )
            inherited_config = await provider.load(inherited_request)

            assert inherited.status_code == 201
            assert inherited.json()["model_profile_id"] == profile_id
            assert inherited_config.model["model_name"] == "qwen-max"

            blocked_credential = await client.patch(
                f"/api/v1/admin/model-credentials/{credential.json()['model_credential_id']}",
                json={"status": "disabled"},
            )
            blocked_catalog = await client.patch(
                f"/api/v1/admin/model-catalog/{catalog.json()['model_catalog_id']}",
                json={"status": "disabled"},
            )
            disabled = await client.delete(
                f"/api/v1/tenants/{tenant_id}/model-profiles/{profile_id}")
            assert blocked_credential.status_code == 409
            assert blocked_catalog.status_code == 409
            assert disabled.status_code == 409
            assert "active Agent" in disabled.json()["error"]["message"]
            for configured_agent_id in (scaffolded_id, str(agent_id), str(inherited_agent_id)):
                response = await client.patch(
                    f"/api/v1/tenants/{tenant_id}/agents/{configured_agent_id}",
                    json={"status": "disabled"},
                )
                assert response.status_code == 200
            disabled = await client.delete(
                f"/api/v1/tenants/{tenant_id}/model-profiles/{profile_id}")
            assert disabled.status_code == 204
            with pytest.raises(PermissionError, match="Agent App is not active"):
                await provider.load(request)

            missing_agent = context.model_copy(update={"agent_app_id": uuid4()})
            with pytest.raises(PermissionError, match="Agent App is not active"):
                await provider.load_backends(missing_agent)


@pytest.mark.anyio
async def test_model_profile_rejects_secret_ref_without_runtime_resolver(tmp_path: Path, ) -> None:
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'secret-ref.db'}",
        auto_create_schema=True,
        admin_bootstrap_token=SecretStr("secret-ref-admin"),
    )
    app = create_test_app(settings)
    transport = httpx.ASGITransport(app=app)
    headers = {
        "Authorization": "Bearer secret-ref-admin",
        "X-Support-Reason": "model secret resolver validation test",
    }

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test",
                headers=headers,
        ) as client:
            rejected = await client.post(
                "/api/v1/admin/model-catalog",
                json={
                    "provider": "bailian",
                    "model_name": "qwen-max",
                    "display_name": "Qwen Max",
                    "platform_secret_ref": "vault://models/qwen-max",
                },
            )

    assert rejected.status_code == 422
