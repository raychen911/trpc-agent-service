"""Safe, deterministic control-plane data for local console demonstrations."""

from __future__ import annotations

from trpc_service.tenant.models import (
    AgentAppSpec,
    AuditPolicy,
    ChannelSpec,
    ChannelType,
    GovernancePolicy,
    IdentityPolicy,
    ModelRoute,
    StorageSpec,
    TenantSpec,
    ToolPolicy,
)


def build_demo_tenant_spec() -> TenantSpec:
    """Return a disabled-channel demo tenant containing references, never secrets."""

    return TenantSpec(
        tenant_id="tenant-demo",
        revision=1,
        display_name="冲刺演示租户",
        apps=(
            AgentAppSpec(
                app_id="assistant",
                revision=1,
                name="assistant_agent",
                prompt=(
                    "你是该租户的专业助手。只使用经过授权的知识与工具，"  # noqa: RUF001
                    "回答应准确、可追溯，不确定时明确说明。"  # noqa: RUF001
                ),
                model=ModelRoute(
                    provider="mock",
                    model="demo-model-not-for-worker",
                    api_key_ref=None,
                    timeout_seconds=60,
                    token_ceiling=8_000,
                    temperature=0.2,
                ),
                tools=ToolPolicy(
                    allowed=frozenset({"preload_memory"}),
                    max_calls_per_turn=1,
                    max_cost_per_turn=0,
                ),
                governance=GovernancePolicy(
                    redact_sensitive_data=True,
                    max_input_chars=16_000,
                    max_output_chars=16_000,
                ),
                metadata={"purpose": "control-plane-demo", "execution_enabled": False},
            ),
        ),
        channels=(
            ChannelSpec(
                binding_id="wecom-demo",
                app_id="assistant",
                app_revision=1,
                channel=ChannelType.WECOM,
                external_account_id="unbound-wecom-account",
                callback_path="/v1/channels/wecom/wecom-demo-public/callback",
                public_callback_id="wecom-demo-public",
                secret_refs={
                    "token": "secret://env/TENANT_DEMO_WECOM_TOKEN",
                    "aes_key": "secret://env/TENANT_DEMO_WECOM_AES_KEY",
                },
                identity_policy=IdentityPolicy(default_action="deny"),
                enabled=False,
            ),
            ChannelSpec(
                binding_id="telegram-demo",
                app_id="assistant",
                app_revision=1,
                channel=ChannelType.TELEGRAM,
                external_account_id="unbound-telegram-account",
                callback_path="/v1/channels/telegram/telegram-demo-public/callback",
                public_callback_id="telegram-demo-public",
                secret_refs={
                    "webhook_secret": "secret://env/TENANT_DEMO_TELEGRAM_WEBHOOK_SECRET",
                    "bot_token": "secret://env/TENANT_DEMO_TELEGRAM_BOT_TOKEN",
                },
                identity_policy=IdentityPolicy(default_action="deny"),
                enabled=False,
            ),
        ),
        storage=StorageSpec(
            session="postgresql",
            memory="postgresql",
            summary="postgresql",
            knowledge="pgvector",
            artifact="s3",
        ),
        audit=AuditPolicy(
            scope="all_tools",
            retention_days=180,
            export="restricted",
            capture_prompt_content=False,
        ),
        budget={"monthly_cost_micros": 0},
    )
