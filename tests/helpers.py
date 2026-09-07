from __future__ import annotations

import uuid

from tenant_agent.models import (
    AgentAppConfig,
    AuditPolicy,
    BackendKind,
    BackendRef,
    BudgetPolicy,
    ChannelBindingConfig,
    ChannelType,
    ChatType,
    DataBackendConfig,
    GovernanceConfig,
    InboundEnvelope,
    ModelConfig,
    RedactionPolicy,
    SecretRef,
    TenantConfig,
    ToolPermissionConfig,
    UserAccessPolicy,
    tenant_environment_prefix,
)


def make_tenant(
    tenant_id: str = "alpha",
    *,
    channel: ChannelType = ChannelType.WEB,
    binding_id: str | None = None,
    tools: frozenset[str] = frozenset({"calculator", "current_time", "memory_search"}),
    dangerous: frozenset[str] = frozenset(),
    summary_every_events: str = "2",
) -> TenantConfig:
    binding_id = binding_id or f"{channel.value}-binding-001"
    secret_prefix = tenant_environment_prefix(tenant_id).removesuffix("_")
    credentials: dict[str, SecretRef] = {}
    if channel is ChannelType.WEB:
        credentials = {"webhook_token": SecretRef(uri=f"env://{secret_prefix}_WEBHOOK_TOKEN")}
    elif channel is ChannelType.TELEGRAM:
        credentials = {
            "webhook_secret": SecretRef(uri=f"env://{secret_prefix}_TELEGRAM_SECRET"),
            "bot_token": SecretRef(uri=f"env://{secret_prefix}_TELEGRAM_TOKEN"),
        }
    elif channel is ChannelType.WECOM_BOT:
        credentials = {
            "bot_id": SecretRef(uri=f"env://{secret_prefix}_WECOM_BOT_ID"),
            "bot_secret": SecretRef(uri=f"env://{secret_prefix}_WECOM_BOT_SECRET"),
        }
    elif channel is ChannelType.WECOM:
        credentials = {
            name: SecretRef(uri=f"env://{secret_prefix}_WECOM_{name.upper()}")
            for name in (
                "callback_token",
                "encoding_aes_key",
                "corp_id",
                "corp_secret",
                "agent_id",
            )
        }
    memory_backend = BackendRef(kind=BackendKind.INMEMORY)
    return TenantConfig(
        tenant_id=tenant_id,
        display_name=f"{tenant_id.title()} Tenant",
        revision=1,
        models={"offline": ModelConfig(provider="deterministic", model_name="deterministic-v1")},
        apps={
            "assistant": AgentAppConfig(
                app_id="assistant",
                agent_name="TestAssistant",
                instruction="Be concise.",
                model_profile="offline",
                allowed_tools=tools,
            )
        },
        channels=(
            ChannelBindingConfig(
                binding_id=binding_id,
                channel=channel,
                app_id="assistant",
                external_account_id=f"{tenant_id}-account",
                credential_refs=credentials,
            ),
        ),
        data_backends=DataBackendConfig(
            session=memory_backend,
            memory=memory_backend,
            summary=memory_backend,
            artifact=memory_backend,
            knowledge=memory_backend,
            audit=memory_backend,
        ),
        governance=GovernanceConfig(
            tools=ToolPermissionConfig(allow=tools, dangerous=dangerous),
            users=UserAccessPolicy(),
            budget=BudgetPolicy(
                monthly_tokens=1_000_000,
                monthly_cost_usd=100,
                max_tokens_per_request=10_000,
                max_concurrent_sessions=100,
            ),
            redaction=RedactionPolicy(),
        ),
        audit=AuditPolicy(enabled=True, include_prompt_hash=True),
        metadata={"summary_every_events": summary_every_events},
    )


def make_envelope(
    tenant: TenantConfig,
    *,
    text: str = "hello",
    user_id: str = "external-user",
    chat_id: str = "external-chat",
    chat_type: ChatType = ChatType.DIRECT,
    message_id: str | None = None,
) -> InboundEnvelope:
    binding = tenant.channels[0]
    return InboundEnvelope(
        message_id=message_id or uuid.uuid4().hex,
        tenant_id=tenant.tenant_id,
        app_id=binding.app_id,
        binding_id=binding.binding_id,
        channel=binding.channel,
        external_account_id=binding.external_account_id,
        external_user_id=user_id,
        external_chat_id=chat_id,
        chat_type=chat_type,
        text=text,
    )
