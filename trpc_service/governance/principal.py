"""Trusted channel principal normalization and tenant grants."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from trpc_service.governance.errors import GovernanceUnavailable
from trpc_service.governance.models import ChannelPrincipal, PrincipalGrant, PrincipalGrantDecision


def issue_principal(*, tenant_id: str, channel: str, binding_id: str, provider_subject: str) -> ChannelPrincipal:
    if not provider_subject or provider_subject.strip() != provider_subject:
        raise ValueError("stable provider subject is required")
    return ChannelPrincipal.issue(
        tenant_id=tenant_id, channel=channel, binding_id=binding_id,
        provider_subject=provider_subject,
    )


class InMemoryPrincipalGrantRepository:
    def __init__(self) -> None:
        self._grants: dict[str, PrincipalGrant] = {}
        self._lock = asyncio.Lock()
        self.unavailable = False

    async def evaluate(self, *, principal: ChannelPrincipal, agent_name: str, binding_id: str, at: datetime) -> PrincipalGrantDecision:
        if self.unavailable:
            raise GovernanceUnavailable()
        async with self._lock:
            candidates = [grant for grant in self._grants.values() if (
                grant.tenant_id == principal.tenant_id
                and grant.channel == principal.channel
                and grant.binding_id == binding_id == principal.binding_id
                and grant.provider_subject_digest == principal.subject_digest
                and (grant.agent_name is None or grant.agent_name == agent_name)
            )]
            for grant in candidates:
                if not grant.enabled:
                    continue
                if grant.valid_from is not None and at < grant.valid_from:
                    continue
                if grant.expires_at is not None and at >= grant.expires_at:
                    continue
                if "use_agent" in grant.permissions:
                    return PrincipalGrantDecision(allowed=True, reason_code="principal_authorized")
            return PrincipalGrantDecision(allowed=False, reason_code="principal_unauthorized")

    async def put(self, grant: PrincipalGrant) -> PrincipalGrant:
        if self.unavailable:
            raise GovernanceUnavailable()
        async with self._lock:
            self._grants[grant.grant_id] = grant
            return grant

    async def disable(self, *, tenant_id: str, grant_id: str, at: datetime) -> PrincipalGrant:
        async with self._lock:
            grant = self._grants[grant_id]
            if grant.tenant_id != tenant_id:
                raise PermissionError("grant scope mismatch")
            updated = grant.model_copy(update={"enabled": False, "revoked_at": at})
            self._grants[grant_id] = updated
            return updated

    def evaluate_sync(self, principal: ChannelPrincipal, *, agent_name: str, binding_id: str) -> PrincipalGrantDecision:
        return asyncio.run(self.evaluate(principal=principal, agent_name=agent_name, binding_id=binding_id, at=datetime.now(timezone.utc)))
