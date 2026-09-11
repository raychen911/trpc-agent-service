"""Shared tenant fixtures."""

from decimal import Decimal

import pytest


from trpc_service.channels import ChannelType
from trpc_service.tenant import AgentApplicationConfig
from trpc_service.tenant import AuditPolicy
from trpc_service.tenant import BackendKind
from trpc_service.tenant import BackendRef
from trpc_service.tenant import ChannelBindingConfig
from trpc_service.tenant import ChannelCredentialRef
from trpc_service.tenant import DataBackendConfig
from trpc_service.tenant import ModelConfig
from trpc_service.tenant import ReplyMode
from trpc_service.tenant import SecretRef
from trpc_service.tenant import TenantConfig
from trpc_service.tenant import ToolPolicy


def pytest_addoption(parser):
    parser.addoption("--backend-mode", choices=("local", "real"), default="local",
                     help="Migration E2E: embedded backends or real Compose services (no fallback).")
    parser.addoption("--strict-acceptance", action="store_true",
                     help="Treat documented E2E implementation gaps as failures, not expected failures.")


def pytest_sessionstart(session):
    from trpc_service.sdk_provenance import verify_official_sdk

    verify_official_sdk()


def pytest_collection_modifyitems(config, items):
    for item in items:
        gap = item.get_closest_marker("known_gap")
        if gap and not config.getoption("--strict-acceptance"):
            item.add_marker(pytest.mark.xfail(reason=gap.args[0], strict=True, raises=AssertionError))


def build_tenant(tenant_id: str = "tenant_acme", webhook_public_id: str = "callback_acme") -> TenantConfig:
    namespace = tenant_id
    return TenantConfig(
        tenant_id=tenant_id,
        name=f"{tenant_id} display name",
        config_version=3,
        storage_revision=4,
        applications=(
            AgentApplicationConfig(
                app_id="customer_support",
                name="Customer Support",
                agent_name="support_agent",
                model_config_id="model_primary",
                instruction="Answer customer questions and use only approved tools.",
                tool_policy=ToolPolicy(
                    allow=frozenset({"search_order", "execute_refund"}),
                    require_confirmation=frozenset({"execute_refund"}),
                ),
            ),
        ),
        models=(
            ModelConfig(
                model_config_id="model_primary",
                provider="openai-compatible",
                model_name="example-model",
                api_key_ref=SecretRef(uri=f"vault://{tenant_id}/models/primary"),
                monthly_budget=Decimal("1000.00"),
            ),
        ),
        channel_bindings=(
            ChannelBindingConfig(
                binding_id="binding_primary",
                tenant_id=tenant_id,
                agent_app_id="customer_support",
                channel=ChannelType.WECOM,
                external_account_id=f"{tenant_id}_bot",
                webhook_public_id=webhook_public_id,
                credential_refs=(
                    ChannelCredentialRef(
                        name="channel_secret",
                        secret_ref=SecretRef(uri=f"vault://{tenant_id}/channels/wecom"),
                    ),
                ),
                reply_mode=ReplyMode.STREAM,
            ),
        ),
        data_backends=DataBackendConfig(
            session=BackendRef(kind=BackendKind.REDIS, profile_id="redis_prod", namespace=namespace),
            memory=BackendRef(kind=BackendKind.SQL, profile_id="sql_prod", namespace=namespace),
            summary=BackendRef(kind=BackendKind.REDIS, profile_id="redis_prod", namespace=namespace),
            knowledge=BackendRef(kind=BackendKind.VECTOR, profile_id="vector_prod", namespace=namespace),
            artifact=BackendRef(kind=BackendKind.OBJECT, profile_id="object_prod", namespace=namespace),
            audit=BackendRef(kind=BackendKind.SQL, profile_id="sql_audit", namespace=namespace),
        ),
        audit_policy=AuditPolicy(),
    )


@pytest.fixture
def tenant() -> TenantConfig:
    return build_tenant()
