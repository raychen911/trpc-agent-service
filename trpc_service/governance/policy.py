"""Tenant policy parsing and authoritative in-memory repository."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Mapping

from trpc_service.governance.errors import GovernanceUnavailable, PolicyDisabled, PolicyMissing, PolicyStale
from trpc_service.governance.models import (
    ActiveGovernancePolicy,
    GovernancePolicyVersion,
    PolicyDocument,
    PolicyScope,
    PolicyStatus,
)


def parse_policy_document(value: Mapping[str, Any] | PolicyDocument) -> PolicyDocument:
    if isinstance(value, PolicyDocument):
        return value
    return PolicyDocument.model_validate(dict(value))


class InMemoryGovernancePolicyRepository:
    """Authoritative tenant-scoped policy store used by unit/contract tests."""

    def __init__(self) -> None:
        self._versions: dict[str, GovernancePolicyVersion] = {}
        self._active: dict[str, str] = {}
        self._generation: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self.unavailable = False

    async def get_active(self, *, tenant_id: str, agent_name: str, binding_id: str) -> ActiveGovernancePolicy:
        del agent_name, binding_id
        if self.unavailable:
            raise GovernanceUnavailable()
        async with self._lock:
            policy_id = self._active.get(tenant_id)
            if policy_id is None:
                raise PolicyMissing()
            version = self._versions[policy_id]
            if version.status != PolicyStatus.ACTIVE:
                raise PolicyDisabled()
            return ActiveGovernancePolicy(
                tenant_id=tenant_id, policy_id=version.policy_id, version=version.version,
                generation=self._generation.get(tenant_id, 0), document=version.document,
            )

    async def create_version(self, *, tenant_id: str, scope: PolicyScope, document: PolicyDocument, actor_digest: str) -> GovernancePolicyVersion:
        if self.unavailable:
            raise GovernanceUnavailable()
        async with self._lock:
            next_version = max((item.version for item in self._versions.values() if item.tenant_id == tenant_id), default=0) + 1
            item = GovernancePolicyVersion(
                policy_id=str(uuid4()), tenant_id=tenant_id, scope=scope,
                version=next_version, status=PolicyStatus.DRAFT, document=parse_policy_document(document),
                created_by_digest=actor_digest, created_at=datetime.now(timezone.utc),
            )
            self._versions[item.policy_id] = item
            return item

    async def activate(self, *, tenant_id: str, policy_id: str, expected_generation: int) -> ActiveGovernancePolicy:
        if self.unavailable:
            raise GovernanceUnavailable()
        async with self._lock:
            if self._generation.get(tenant_id, 0) != expected_generation:
                raise PolicyStale()
            item = self._versions.get(policy_id)
            if item is None or item.tenant_id != tenant_id:
                raise PolicyMissing()
            if item.status not in {PolicyStatus.DRAFT, PolicyStatus.ACTIVE}:
                raise PolicyDisabled()
            previous = self._active.get(tenant_id)
            if previous and previous != policy_id:
                self._versions[previous] = self._versions[previous].model_copy(update={"status": PolicyStatus.SUPERSEDED})
            item = item.model_copy(update={"status": PolicyStatus.ACTIVE})
            self._versions[policy_id] = item
            generation = expected_generation + 1
            self._generation[tenant_id] = generation
            self._active[tenant_id] = policy_id
            return ActiveGovernancePolicy(tenant_id=tenant_id, policy_id=policy_id, version=item.version, generation=generation, document=item.document)

    async def disable(self, *, tenant_id: str, policy_id: str, expected_generation: int) -> ActiveGovernancePolicy:
        if self.unavailable:
            raise GovernanceUnavailable()
        async with self._lock:
            if self._generation.get(tenant_id, 0) != expected_generation:
                raise PolicyStale()
            item = self._versions.get(policy_id)
            if item is None or item.tenant_id != tenant_id:
                raise PolicyMissing()
            item = item.model_copy(update={"status": PolicyStatus.DISABLED})
            self._versions[policy_id] = item
            self._active.pop(tenant_id, None)
            generation = expected_generation + 1
            self._generation[tenant_id] = generation
            return ActiveGovernancePolicy(tenant_id=tenant_id, policy_id=policy_id, version=item.version, generation=generation, document=item.document)

    def create_version_sync(self, tenant_id: str, document: PolicyDocument, *, actor_digest: str) -> GovernancePolicyVersion:
        return asyncio.run(self.create_version(tenant_id=tenant_id, scope=PolicyScope.TENANT, document=document, actor_digest=actor_digest))

    def activate_sync(self, tenant_id: str, policy_id: str, *, expected_generation: int) -> ActiveGovernancePolicy:
        return asyncio.run(self.activate(tenant_id=tenant_id, policy_id=policy_id, expected_generation=expected_generation))

    def get_active_sync(self, tenant_id: str, agent_name: str, binding_id: str) -> ActiveGovernancePolicy:
        return asyncio.run(self.get_active(tenant_id=tenant_id, agent_name=agent_name, binding_id=binding_id))
