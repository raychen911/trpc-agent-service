"""Configuration rollout and platform-policy selection shared by the service."""

import hashlib
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.models import ModelCatalogEntry, ModelProfile, ModelProviderCredential
from trpc_service.agent.models import AgentApp, AgentConfigVersion
from trpc_service.config.secret_scope import (
    validate_platform_model_secret_ref,
    validate_tenant_model_secret_ref,
)


class AgentConfigurationError(PermissionError):
    """Stable fail-closed error for an Agent configuration that cannot execute."""

    def __init__(self, error_code: str, message: str, public_detail: str) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.public_detail = public_detail


@dataclass(frozen=True, slots=True)
class ResolvedModelPolicy:
    """Validated platform model policy shared by control and runtime planes."""

    profile: ModelProfile
    catalog: ModelCatalogEntry
    secret_ref: str


async def resolve_active_model_policy(
    session: AsyncSession,
    tenant_id: UUID,
    model_profile_id: UUID | None,
) -> ResolvedModelPolicy:
    """Resolve one executable model policy or raise a stable configuration error."""

    if model_profile_id is None:
        raise AgentConfigurationError(
            "AGENT_MODEL_PROFILE_MISSING",
            "Agent App has no governed Model Profile",
            "Agent is not executable: platform model profile is not configured",
        )
    profile = await session.scalar(
        select(ModelProfile).where(
            ModelProfile.tenant_id == tenant_id,
            ModelProfile.model_profile_id == model_profile_id,
            ModelProfile.status == "active",
        ))
    if profile is None:
        raise AgentConfigurationError(
            "AGENT_MODEL_PROFILE_INACTIVE",
            "Agent Model Profile is not active",
            "Agent is not executable: platform model profile is inactive",
        )
    catalog = await session.scalar(
        select(ModelCatalogEntry).where(
            ModelCatalogEntry.model_catalog_id == profile.model_catalog_id,
            ModelCatalogEntry.status == "active",
        ))
    if catalog is None:
        raise AgentConfigurationError(
            "AGENT_MODEL_CATALOG_INACTIVE",
            "Agent model is not active in the platform catalog",
            "Agent is not executable: platform model catalog entry is inactive",
        )

    secret_ref: str | None
    if profile.model_credential_id is not None:
        credential = await session.scalar(
            select(ModelProviderCredential).where(
                ModelProviderCredential.model_credential_id == profile.model_credential_id,
                ModelProviderCredential.status == "active",
            ))
        if credential is None or credential.provider != catalog.provider:
            raise AgentConfigurationError(
                "AGENT_MODEL_CREDENTIAL_INACTIVE",
                "Agent model credential is not active for its provider",
                "Agent is not executable: platform model credential is inactive",
            )
        secret_ref = credential.secret_ref
    else:
        # Read legacy profiles during migration; current APIs always create an
        # explicit platform credential relation.
        secret_ref = (profile.secret_ref if profile.credential_mode == "tenant_managed" else
                      catalog.platform_secret_ref)
    if secret_ref is None:
        raise AgentConfigurationError(
            "AGENT_MODEL_CREDENTIAL_MISSING",
            "Agent Model Profile has no configured credential",
            "Agent is not executable: platform model credential is not configured",
        )
    try:
        if profile.model_credential_id is None and profile.credential_mode == "tenant_managed":
            validate_tenant_model_secret_ref(secret_ref, tenant_id)
        else:
            validate_platform_model_secret_ref(secret_ref)
    except ValueError as error:
        raise AgentConfigurationError(
            "AGENT_MODEL_CREDENTIAL_SCOPE_INVALID",
            "Agent model credential is outside its allowed scope",
            "Agent is not executable: platform model credential scope is invalid",
        ) from error
    return ResolvedModelPolicy(profile=profile, catalog=catalog, secret_ref=secret_ref)


async def require_agent_execution_ready(
    session: AsyncSession,
    agent: AgentApp,
) -> ResolvedModelPolicy:
    """Apply the shared readiness gate used by every current and future IM."""

    if agent.status != "active":
        raise AgentConfigurationError(
            "AGENT_INACTIVE",
            "Agent App is not active for this tenant",
            "Agent is not executable: Agent is inactive",
        )
    return await resolve_active_model_policy(session, agent.tenant_id, agent.model_profile_id)


def snapshot_agent_config(agent: AgentApp) -> dict[str, object]:
    """Copy execution fields into an immutable JSON-compatible snapshot."""

    return {
        "model_profile_id":
        (None if agent.model_profile_id is None else str(agent.model_profile_id)),
        "application_config": dict(agent.application_config),
        "model_settings": dict(agent.model_settings),
        "tool_permissions": dict(agent.tool_permissions),
        "knowledge_config": dict(agent.knowledge_config),
        "backend_config": dict(agent.backend_config),
    }


async def select_default_model_profile_id(
    session: AsyncSession,
    tenant_id: UUID,
) -> UUID | None:
    """Resolve a platform-managed default without making tenant users choose it."""

    profiles = (await session.execute(
        select(ModelProfile.model_profile_id, ModelProfile.name).where(
            ModelProfile.tenant_id == tenant_id,
            ModelProfile.status == "active",
        ).order_by(ModelProfile.created_at, ModelProfile.model_profile_id))).all()
    if len(profiles) == 1:
        return UUID(str(profiles[0].model_profile_id))
    primary = [UUID(str(row.model_profile_id)) for row in profiles if row.name == "primary"]
    return primary[0] if len(primary) == 1 else None


async def attach_unassigned_agents_to_default_profile(
    session: AsyncSession,
    tenant_id: UUID,
    *,
    actor_subject: str,
) -> int:
    """Release a new configuration for legacy Agents missing model governance."""

    profile_id = await select_default_model_profile_id(session, tenant_id)
    if profile_id is None:
        return 0
    agents = (await session.scalars(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.model_profile_id.is_(None),
        ).with_for_update())).all()
    for agent in agents:
        latest = await session.scalar(
            select(func.max(AgentConfigVersion.version)).where(
                AgentConfigVersion.tenant_id == tenant_id,
                AgentConfigVersion.agent_app_id == agent.agent_app_id,
            ))
        version = int(latest or 0) + 1
        agent.model_profile_id = profile_id
        agent.stable_config_version = version
        agent.canary_config_version = None
        agent.canary_percent = 0
        session.add(
            AgentConfigVersion(
                tenant_id=tenant_id,
                agent_app_id=agent.agent_app_id,
                version=version,
                status="released",
                snapshot=snapshot_agent_config(agent),
                created_by=actor_subject,
                reason="attached platform default Model Profile",
            ))
    return len(agents)


def select_agent_config_version(agent: AgentApp, rollout_key: str | None) -> int:
    """Select a sticky canary version without process-local state.

    Every node hashes the same tenant, Agent, and principal key, so horizontal
    scaling cannot move a user between stable and canary cohorts.
    """

    if (rollout_key is None or agent.canary_config_version is None or agent.canary_percent <= 0):
        return agent.stable_config_version
    cohort_key = f"{agent.tenant_id}:{agent.agent_app_id}:{rollout_key}".encode()
    bucket = int.from_bytes(hashlib.sha256(cohort_key).digest()[:8], "big") % 100
    if bucket < agent.canary_percent:
        return agent.canary_config_version
    return agent.stable_config_version
