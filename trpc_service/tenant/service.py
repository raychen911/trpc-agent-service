"""Immutable tenant-config publication and rollback service."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.models import (
    AgentApp,
    ChannelBinding,
    ChannelIngressRoute,
    Tenant,
    TenantConfigRevision,
)
from trpc_service.tenant.models import TenantSpec


class TenantConfigError(RuntimeError):
    """Base error for rejected control-plane configuration changes."""


class RevisionConflictError(TenantConfigError):
    """A revision number was reused for different immutable content."""


class RevisionSequenceError(TenantConfigError):
    """A publication attempted to skip or move backwards in revision history."""


class TenantNotFoundError(TenantConfigError):
    """The requested tenant or revision does not exist."""


@dataclass(frozen=True, slots=True)
class PublishedConfig:
    """Publication outcome safe to return through the Admin API."""

    tenant_id: str
    revision: int
    content_hash: str
    idempotent: bool


class TenantConfigService:
    """Publish immutable specs and materialize their active routing view."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def publish(self, spec: TenantSpec, *, actor: str) -> PublishedConfig:
        """Publish exactly the next tenant revision, idempotently by content hash."""

        payload = _publication_payload(spec)
        content_hash = _content_hash(payload)
        async with self._session_factory() as session, session.begin():
            await _set_tenant_scope(session, spec.tenant_id)
            tenant = await session.get(Tenant, spec.tenant_id, with_for_update=True)
            existing = await session.get(
                TenantConfigRevision,
                (spec.tenant_id, spec.revision),
            )
            if existing is not None:
                if existing.content_hash != content_hash:
                    raise RevisionConflictError("revision already exists with different content")
                return PublishedConfig(spec.tenant_id, spec.revision, content_hash, True)

            latest_revision = await session.scalar(
                select(func.max(TenantConfigRevision.revision)).where(
                    TenantConfigRevision.tenant_id == spec.tenant_id
                )
            )
            expected_revision = (latest_revision or 0) + 1
            if spec.revision != expected_revision:
                raise RevisionSequenceError(
                    f"expected revision {expected_revision}, received {spec.revision}"
                )

            if tenant is None:
                tenant = Tenant(
                    tenant_id=spec.tenant_id,
                    display_name=spec.display_name,
                    status=spec.status,
                    audit_policy=spec.audit.model_dump(mode="json"),
                    budget_policy=spec.budget,
                )
                session.add(tenant)
                await session.flush()
            session.add(
                TenantConfigRevision(
                    tenant_id=spec.tenant_id,
                    revision=spec.revision,
                    schema_version=spec.schema_version,
                    status="published",
                    spec=payload,
                    content_hash=content_hash,
                    created_by=actor,
                )
            )
            await self._materialize(session, tenant, spec)
            return PublishedConfig(spec.tenant_id, spec.revision, content_hash, False)

    async def rollback(
        self,
        tenant_id: str,
        *,
        target_revision: int,
    ) -> PublishedConfig:
        """Atomically rematerialize an existing immutable revision."""

        async with self._session_factory() as session, session.begin():
            await _set_tenant_scope(session, tenant_id)
            tenant = await session.get(Tenant, tenant_id, with_for_update=True)
            revision = await session.get(
                TenantConfigRevision,
                (tenant_id, target_revision),
            )
            if tenant is None or revision is None:
                raise TenantNotFoundError("tenant revision does not exist")
            spec = TenantSpec.model_validate(revision.spec)
            await self._materialize(session, tenant, spec)
            return PublishedConfig(tenant_id, target_revision, revision.content_hash, True)

    async def load_active(self, tenant_id: str) -> TenantSpec:
        """Load and validate the immutable active spec."""

        async with self._session_factory() as session:
            await _set_tenant_scope(session, tenant_id)
            tenant = await session.get(Tenant, tenant_id)
            if tenant is None or tenant.active_config_revision is None:
                raise TenantNotFoundError("tenant has no active configuration")
            revision = await session.get(
                TenantConfigRevision,
                (tenant_id, tenant.active_config_revision),
            )
            if revision is None:
                raise TenantNotFoundError("active tenant revision is missing")
            return TenantSpec.model_validate(revision.spec)

    async def load_revision(self, tenant_id: str, revision: int) -> TenantSpec:
        """Load the exact immutable spec pinned when an Inbox was accepted."""

        if revision < 1:
            raise TenantNotFoundError("tenant revision does not exist")
        async with self._session_factory() as session:
            await _set_tenant_scope(session, tenant_id)
            stored = await session.get(
                TenantConfigRevision,
                (tenant_id, revision),
            )
            if stored is None:
                raise TenantNotFoundError("tenant revision does not exist")
            return TenantSpec.model_validate(stored.spec)

    @staticmethod
    async def _materialize(
        session: AsyncSession,
        tenant: Tenant,
        spec: TenantSpec,
    ) -> None:
        tenant.display_name = spec.display_name
        tenant.status = spec.status
        tenant.audit_policy = spec.audit.model_dump(mode="json")
        tenant.budget_policy = spec.budget
        tenant.active_config_revision = spec.revision

        for app in spec.apps:
            existing_app = await session.get(
                AgentApp,
                (spec.tenant_id, app.app_id, app.revision),
            )
            values = {
                "agent_name": app.name,
                "prompt": app.prompt,
                "model_config": app.model.model_dump(mode="json"),
                "tool_policy": app.tools.model_dump(mode="json"),
                "storage_config": spec.storage.model_dump(mode="json"),
            }
            if existing_app is None:
                session.add(
                    AgentApp(
                        tenant_id=spec.tenant_id,
                        app_id=app.app_id,
                        revision=app.revision,
                        status="published",
                        **values,
                    )
                )
            elif any(getattr(existing_app, key) != value for key, value in values.items()):
                raise RevisionConflictError("agent app revision content is immutable")
            else:
                existing_app.status = "published"

        await session.flush()

        active_binding_ids = {channel.binding_id for channel in spec.channels}
        bindings = (
            await session.scalars(
                select(ChannelBinding).where(ChannelBinding.tenant_id == spec.tenant_id)
            )
        ).all()
        for binding in bindings:
            if binding.binding_id not in active_binding_ids:
                binding.status = "disabled"
        routes = (
            await session.scalars(
                select(ChannelIngressRoute).where(ChannelIngressRoute.tenant_id == spec.tenant_id)
            )
        ).all()
        for route in routes:
            if route.binding_id not in active_binding_ids:
                route.status = "disabled"

        for channel in spec.channels:
            existing_binding = await session.get(ChannelBinding, channel.binding_id)
            if existing_binding is not None and existing_binding.tenant_id != spec.tenant_id:
                raise RevisionConflictError("binding_id is already owned by another tenant")
            binding_values: dict[str, Any] = {
                "app_id": channel.app_id,
                "app_revision": channel.app_revision,
                "config_revision": spec.revision,
                "channel_type": channel.channel.value,
                "external_account_id": channel.external_account_id,
                "callback_path": channel.callback_path,
                "public_callback_id": channel.public_callback_id,
                "route_rule": channel.route_rule,
                "secret_refs": channel.secret_refs,
                "identity_policy": channel.identity_policy.model_dump(mode="json"),
                "status": "active" if channel.enabled else "disabled",
            }
            if existing_binding is None:
                session.add(
                    ChannelBinding(
                        binding_id=channel.binding_id,
                        tenant_id=spec.tenant_id,
                        **binding_values,
                    )
                )
            else:
                for key, value in binding_values.items():
                    setattr(existing_binding, key, value)

        await session.flush()
        for channel in spec.channels:
            existing_route = await session.get(
                ChannelIngressRoute,
                channel.public_callback_id,
            )
            if existing_route is not None and existing_route.tenant_id != spec.tenant_id:
                raise RevisionConflictError("public callback id belongs to another tenant")
            route_values: dict[str, Any] = {
                "tenant_id": spec.tenant_id,
                "binding_id": channel.binding_id,
                "channel_type": channel.channel.value,
                "config_revision": spec.revision,
                "status": "active" if channel.enabled else "disabled",
            }
            if existing_route is None:
                session.add(
                    ChannelIngressRoute(
                        public_callback_id=channel.public_callback_id,
                        **route_values,
                    )
                )
            else:
                for key, value in route_values.items():
                    setattr(existing_route, key, value)


def _content_hash(payload: dict[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def _publication_payload(spec: TenantSpec) -> dict[str, Any]:
    """Normalize set-valued policies so hashes survive process hash randomization."""

    payload = spec.model_dump(mode="json")
    for app in payload["apps"]:
        tools = app["tools"]
        tools["allowed"] = sorted(tools["allowed"])
        tools["requires_approval"] = sorted(tools["requires_approval"])
    for channel in payload["channels"]:
        identity = channel["identity_policy"]
        identity["allow_principals"] = sorted(identity["allow_principals"])
        identity["deny_principals"] = sorted(identity["deny_principals"])
        identity["allowed_scopes"] = sorted(identity["allowed_scopes"])
    return payload


async def _set_tenant_scope(session: AsyncSession, tenant_id: str) -> None:
    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        await session.execute(
            text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
            {"tenant_id": tenant_id},
        )
