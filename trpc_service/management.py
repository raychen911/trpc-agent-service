"""Persistent operator control plane. Published snapshots are never rewritten."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import timedelta
from typing import Literal
from urllib.parse import urlsplit

from trpc_service.governance.admin_auth import current_actor

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError

from trpc_service.persistence.models import (
    AgentAppRow,
    AuditLogRow,
    BackendProfileRow,
    ChannelBindingRow,
    TenantBackendBindingRow,
    TenantConfigVersionRow,
    TenantRow,
    utcnow,
    OutboxMessageRow,
    TenantRolloutRow,
    TenantMaintenanceRow,
    OfflineMigrationRow,
    InboundMessageRow,
)
from trpc_service.storage.models import BackendProfile, ResourceType
from trpc_service.tenant import TenantConfig
from trpc_service.tenant import ResourcePolicy


class ProfileRegistration(BackendProfile):
    """Only reference-based connection configuration is accepted by this API."""

    @model_validator(mode="after")
    def validate_registration(self):
        endpoint = urlsplit(self.endpoint if "://" in self.endpoint else "//" + self.endpoint)
        if endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
            raise ValueError("endpoint must not contain credentials, query parameters or fragments")
        allowed = {
            "bucket", "access_key_ref", "secret_key_ref", "secure", "collection", "index_version", "embedding_id",
            "session_format"
        }
        if set(self.options) - allowed or self.region is not None or self.health.value != "healthy":
            raise ValueError("unsupported backend options")
        role_options = {
            "object": {"bucket", "access_key_ref", "secret_key_ref", "secure"},
            "vector": {"collection", "index_version", "embedding_id"},
            "sql": {"session_format"},
            "redis": {"session_format"}
        }
        if set(self.options) - role_options.get(self.kind.value, set()):
            raise ValueError("options do not apply to this backend kind")
        if "session_format" in self.options and self.options["session_format"] != "protected_v1":
            raise ValueError("unsupported Session storage format")
        for key in ("bucket", "collection", "embedding_id"):
            if key in self.options and (not isinstance(self.options[key], str) or not 1 <= len(self.options[key]) <= 128
                                        or not all(c.isalnum() or c in "._-" for c in self.options[key])):
                raise ValueError("invalid resource name")
        if "secure" in self.options and type(self.options["secure"]) is not bool:
            raise ValueError("secure must be a boolean")
        if "index_version" in self.options and (type(self.options["index_version"]) is not int
                                                or self.options["index_version"] < 1):
            raise ValueError("index_version must be positive")
        for key in ("access_key_ref", "secret_key_ref"):
            if key in self.options:
                value = self.options[key]
                if not isinstance(value, str) or not value.startswith("env://") or not value[6:].isidentifier():
                    raise ValueError("object credentials must use env references")
        if not self.secret_ref.uri.startswith("env://"):
            raise ValueError("this deployment resolves only env:// secret references")
        if not self.secret_ref.uri[6:].isidentifier():
            raise ValueError("env reference must name one environment variable")
        return self


class ConfigDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config: TenantConfig
    profile_revisions: dict[ResourceType, int]

    @model_validator(mode="after")
    def complete_revisions(self):
        if set(self.profile_revisions) != set(ResourceType) or min(self.profile_revisions.values()) < 1:
            raise ValueError("all six resources require an explicit positive profile revision")
        return self


class PublishRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_active_version: int = Field(ge=0)


class DeliveryReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_attempt: int = Field(ge=1)


class RolloutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target_version: int = Field(ge=1)
    expected_active_version: int = Field(ge=1)
    percent: int = Field(ge=0, le=100)


class MaintenanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool


class IMMemberGrant(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    active: bool
    grants: frozenset[Literal["chat", "approve"]] = frozenset()
    expected_epoch: int = Field(ge=0)


class MigrationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_version: int = Field(ge=1)
    target_version: int = Field(ge=1)
    resource_type: Literal["session", "knowledge"] = "session"


class MigrationRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    writers_stopped: bool = False
    batch_size: int = Field(default=50, ge=1, le=100)
    cutover: bool = False


class ConfigConflict(Exception):
    pass


class ConfigurationStore:

    def __init__(self, database):
        self.database = database

    @staticmethod
    def audit(session, tenant_id, actor, action, version=None, metadata=None):
        from trpc_service.telemetry.runtime import current_trace_id
        from trpc_service.telemetry.logging import current_fields
        session.add(
            AuditLogRow(audit_id=uuid.uuid4().hex,
                        tenant_id=tenant_id,
                        user_id=actor,
                        decision=action,
                        config_version=version,
                        trace_id=current_trace_id(),
                        request_id=current_fields().get("request_id"),
                        redacted=True,
                        metadata_json=metadata or {}))

    @staticmethod
    def profile(row):
        if row is None:
            raise ValueError("backend profile revision does not exist")
        return BackendProfile(profile_id=row.profile_id,
                              profile_revision=row.profile_revision,
                              kind=row.kind,
                              provider=row.provider,
                              endpoint=row.endpoint,
                              secret_ref={"uri": row.secret_ref},
                              capabilities=row.capabilities_json,
                              options=row.options_json,
                              enabled=row.status == "enabled")

    def register_profile(self, profile, actor):
        with self.database.sessions.begin() as session:
            session.add(
                BackendProfileRow(profile_id=profile.profile_id,
                                  profile_revision=profile.profile_revision,
                                  kind=profile.kind.value,
                                  provider=profile.provider,
                                  endpoint=profile.endpoint,
                                  secret_ref=profile.secret_ref.uri,
                                  options_json=profile.options,
                                  capabilities_json=sorted(item.value for item in profile.capabilities),
                                  status="enabled" if profile.enabled else "disabled"))
            self.audit(session,
                       "platform",
                       actor,
                       "backend_profile_registered",
                       metadata={
                           "profile_id": profile.profile_id,
                           "profile_revision": profile.profile_revision
                       })

    def validate(self, session, config, revisions):
        profiles = {}
        for role in ResourceType:
            ref = getattr(config.data_backends, role.value)
            profile = self.profile(session.get(BackendProfileRow, (ref.profile_id, revisions[role])))
            profiles[role.value] = profile
            if not profile.enabled or role.value not in {item.value for item in profile.capabilities}:
                raise ValueError("profile disabled or missing resource capability")
            if profile.kind != ref.kind or profile.kind.value == "in_memory":
                raise ValueError("resource kind mismatch or non-shared backend")
        if revisions[ResourceType.SESSION] != revisions[ResourceType.SUMMARY]:
            raise ValueError("Session and Summary must use the same profile revision")
        if os.environ.get("TRPC_RUNTIME_MODE") == "protected":
            from trpc_service.agent.runtime_policy import validate_protected
            validate_protected(config, profiles)

    def save_draft(self, draft, actor):
        config = draft.config
        with self.database.sessions.begin() as session:
            self.validate(session, config, draft.profile_revisions)
            if session.get(TenantRow, config.tenant_id) is None:
                session.add(
                    TenantRow(tenant_id=config.tenant_id, name=config.name, status="disabled", active_config_version=0))
            payload = config.model_dump(mode="json")
            session.add(
                TenantConfigVersionRow(tenant_id=config.tenant_id,
                                       config_version=config.config_version,
                                       status="draft",
                                       config_json=payload,
                                       content_hash=hashlib.sha256(
                                           json.dumps(draft.model_dump(mode="json"),
                                                      sort_keys=True,
                                                      separators=(",", ":")).encode()).hexdigest(),
                                       created_by=actor))
            for role, revision in draft.profile_revisions.items():
                ref = getattr(config.data_backends, role.value)
                session.add(
                    TenantBackendBindingRow(tenant_id=config.tenant_id,
                                            config_version=config.config_version,
                                            resource_type=role.value,
                                            profile_id=ref.profile_id,
                                            profile_revision=revision,
                                            namespace=ref.namespace,
                                            storage_revision=config.storage_revision))
            for app in config.applications:
                session.add(
                    AgentAppRow(tenant_id=config.tenant_id,
                                app_id=app.app_id,
                                config_version=config.config_version,
                                enabled=app.enabled,
                                config_json=app.model_dump(mode="json")))
            self.audit(session, config.tenant_id, actor, "config_drafted", config.config_version)

    @staticmethod
    def bindings(session, tenant_id, version):
        return list(
            session.scalars(
                select(TenantBackendBindingRow).where(TenantBackendBindingRow.tenant_id == tenant_id,
                                                      TenantBackendBindingRow.config_version == version)))

    def publish(self, tenant_id, version, expected, actor):
        with self.database.sessions.begin() as session:
            session.get(TenantRow, tenant_id, with_for_update=True)
            maintenance = session.get(TenantMaintenanceRow, tenant_id)
            if maintenance and maintenance.enabled:
                raise ConfigConflict("tenant is in maintenance")
            row = session.get(TenantConfigVersionRow, (tenant_id, version))
            if row is None:
                raise LookupError("configuration not found")
            config = TenantConfig.model_validate(row.config_json)
            if os.environ.get("TRPC_RUNTIME_MODE") == "simulation":
                from trpc_service.service_runtime import validate_simulation
                validate_simulation(config)
            bindings = self.bindings(session, tenant_id, version)
            revisions = {ResourceType(item.resource_type): item.profile_revision for item in bindings}
            self.validate(session, config, revisions)
            # A config rollback is safe only while data placement stays unchanged.
            if expected:
                old = self.bindings(session, tenant_id, expected)

                def placement(items):
                    return sorted((b.resource_type, b.profile_id, b.profile_revision, b.namespace, b.storage_revision)
                                  for b in items)

                if placement(old) != placement(bindings):
                    raise ConfigConflict("storage placement changes require the migration workflow")
            result = session.execute(
                update(TenantRow).where(
                    TenantRow.tenant_id == tenant_id,
                    TenantRow.active_config_version == expected).values(active_config_version=version,
                                                                        name=config.name,
                                                                        status=config.status.value))
            if result.rowcount != 1 or expected == version:
                raise ConfigConflict("active version changed; reload before publishing")
            session.execute(delete(TenantRolloutRow).where(TenantRolloutRow.tenant_id == tenant_id))
            session.execute(delete(ChannelBindingRow).where(ChannelBindingRow.tenant_id == tenant_id))
            session.flush()
            for binding in config.channel_bindings:
                from trpc_service.channels.persistence import IMAccounts
                IMAccounts.register_in_session(session, binding)
                session.add(
                    ChannelBindingRow(tenant_id=tenant_id,
                                      binding_id=binding.binding_id,
                                      channel=binding.channel.value,
                                      webhook_public_id=binding.webhook_public_id,
                                      external_account_id=binding.external_account_id,
                                      agent_app_id=binding.agent_app_id,
                                      credential_ref=json.dumps(
                                          [item.model_dump(mode="json") for item in binding.credential_refs]),
                                      enabled=binding.enabled))
            row.status = "published"
            if row.published_at is None:
                row.published_at = utcnow()
            self.audit(session,
                       tenant_id,
                       actor,
                       "config_published",
                       version,
                       metadata={"previous_active_version": expected})

    def active(self):
        with self.database.sessions() as session:
            query = select(TenantConfigVersionRow).join(
                TenantRow, (TenantRow.tenant_id == TenantConfigVersionRow.tenant_id) &
                (TenantRow.active_config_version == TenantConfigVersionRow.config_version)).where(
                    TenantRow.status == "active")
            rows = session.scalars(query)
            return tuple(TenantConfig.model_validate(row.config_json) for row in rows)

    def rollout(self, tenant_id, target_version, expected, percent, actor):
        with self.database.sessions.begin() as session:
            tenant = session.get(TenantRow, tenant_id, with_for_update=True)
            maintenance = session.get(TenantMaintenanceRow, tenant_id)
            if maintenance and maintenance.enabled:
                raise ConfigConflict("tenant is in maintenance")
            if tenant is None or tenant.active_config_version != expected:
                raise ConfigConflict("active version changed")
            base = session.get(TenantConfigVersionRow, (tenant_id, expected))
            target = session.get(TenantConfigVersionRow, (tenant_id, target_version))
            if target is None or target_version == expected:
                raise ValueError("distinct target version required")
            base_config = TenantConfig.model_validate(base.config_json)
            target_config = TenantConfig.model_validate(target.config_json)
            if (base_config.channel_bindings != target_config.channel_bindings
                    or base_config.status != target_config.status
                    or base_config.data_backends != target_config.data_backends
                    or base_config.storage_revision != target_config.storage_revision):
                raise ConfigConflict("canary must preserve routing and storage")
            if {
                    a.app_id: a.enabled
                    for a in base_config.applications
            } != {
                    a.app_id: a.enabled
                    for a in target_config.applications
            }:
                raise ConfigConflict("canary must preserve enabled applications")
            old_bindings = self.bindings(session, tenant_id, expected)
            bindings = self.bindings(session, tenant_id, target_version)
            if {
                    b.resource_type: b.profile_revision
                    for b in old_bindings
            } != {
                    b.resource_type: b.profile_revision
                    for b in bindings
            }:
                raise ConfigConflict("canary must preserve backend revisions")
            self.validate(session, target_config, {ResourceType(b.resource_type): b.profile_revision for b in bindings})
            if os.environ.get("TRPC_RUNTIME_MODE") == "simulation":
                from trpc_service.service_runtime import validate_simulation
                validate_simulation(target_config)
            row = session.get(TenantRolloutRow, tenant_id)
            if row is None:
                row = TenantRolloutRow(tenant_id=tenant_id)
                session.add(row)
            row.base_version, row.target_version, row.percent = expected, target_version, percent
            target.status, target.published_at = "published", target.published_at or utcnow()
            self.audit(session, tenant_id, actor, "config_rollout", target_version, {"percent": percent})

    def route_message(self, message):
        from trpc_service.service_runtime import message_router
        route = message_router(self.active()).route_message(message)
        from trpc_service.migration.offline import require_available
        require_available(self.database, route.tenant_id)
        with self.database.sessions() as session:
            rollout = session.get(TenantRolloutRow, route.tenant_id)
            if rollout is None or rollout.base_version != route.config_version:
                return route
            bucket = int(hashlib.sha256(route.session_id.encode()).hexdigest()[:8], 16) % 100
            if bucket < rollout.percent:
                return route.model_copy(update={"config_version": rollout.target_version})
            return route

    def published(self, tenant_id, version):
        with self.database.sessions() as session:
            row = session.get(TenantConfigVersionRow, (tenant_id, version))
            if row is None or row.status != "published":
                raise LookupError("pinned published configuration not found")
            return TenantConfig.model_validate(row.config_json)

    def connection(self, tenant_id, version, resource):
        profile = self.resource_profile(tenant_id, version, resource)
        return os.environ[profile.secret_ref.uri.removeprefix("env://")]

    def resource_profile(self, tenant_id, version, resource):
        with self.database.sessions() as session:
            binding = session.get(TenantBackendBindingRow, (tenant_id, version, resource))
            if binding is None:
                raise LookupError("pinned resource binding missing")
            profile = self.profile(session.get(BackendProfileRow, (binding.profile_id, binding.profile_revision)))
            return profile


def management_router(database):
    token = os.environ.get("TRPC_ADMIN_TOKEN")
    if token is None and not os.environ.get("TRPC_ADMIN_PRINCIPALS"):
        return APIRouter()
    if token is not None and len(token) < 32:
        raise ValueError("admin token must contain at least 32 characters")
    actor = os.environ.get("TRPC_ADMIN_ACTOR", "platform_operator")
    if not 1 <= len(actor) <= 128:
        raise ValueError("admin actor must contain 1 to 128 characters")

    from trpc_service.governance.admin_auth import authenticate, principal, authorize_config, identities
    list(identities())

    async def authorize(request: Request,
                        authorization: HTTPAuthorizationCredentials | None = Depends(HTTPBearer(auto_error=False))):
        from trpc_service.telemetry.runtime import operation
        with operation("management.request", attributes={"request.id": uuid.uuid4().hex}):
            context = principal.set(authenticate(request, authorization, token, actor))
            try:
                yield
            finally:
                principal.reset(context)

    router = APIRouter(prefix="/admin", tags=["operator management"], dependencies=[Depends(authorize)])
    from trpc_service.governance.management import budget_router
    router.include_router(budget_router(database, actor))
    from trpc_service.governance.action_management import action_router
    router.include_router(action_router(database, actor))
    from trpc_service.storage.knowledge_management import knowledge_router
    router.include_router(knowledge_router(database))
    store = ConfigurationStore(database)

    def mutate(operation):
        from trpc_service.channels.persistence import AccountOwnershipError
        try:
            operation()
        except (IntegrityError, ConfigConflict, AccountOwnershipError):
            raise HTTPException(409, "version, callback identity or storage placement conflict") from None
        except LookupError:
            raise HTTPException(404, "configuration not found") from None
        except ValueError:
            raise HTTPException(422, "configuration or backend capabilities invalid") from None
        return {"status": "ok"}

    @router.post("/backend-profiles", status_code=201)
    def register(profile: ProfileRegistration):
        return mutate(lambda: store.register_profile(profile, current_actor(actor)))

    @router.get("/tenants/{tenant_id}/im/events")
    def im_events(tenant_id: str,
                  binding_id: str | None = None,
                  limit: int = Query(50, ge=1, le=200),
                  offset: int = Query(0, ge=0)):
        from trpc_service.persistence.models import IMEventReceiptRow
        with database.sessions() as session:
            query = select(IMEventReceiptRow).where(IMEventReceiptRow.tenant_id == tenant_id)
            if binding_id:
                query = query.where(IMEventReceiptRow.binding_id == binding_id)
            return [
                dict(binding_id=row.binding_id,
                     event_id=row.external_event_id,
                     kind=row.event_kind,
                     disposition=row.disposition,
                     reason=row.reason,
                     actor_id=row.actor_id,
                     inbound_id=row.inbound_id,
                     created_at=row.created_at) for row in session.scalars(
                         query.order_by(IMEventReceiptRow.created_at.desc(), IMEventReceiptRow.external_event_id).limit(
                             limit).offset(offset))
            ]

    @router.get("/tenants/{tenant_id}/im/members")
    def im_members(tenant_id: str, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
        from trpc_service.persistence.models import IMMembershipRow
        with database.sessions() as session:
            return [
                dict(binding_id=row.binding_id,
                     actor_id=row.actor_id,
                     active=row.active,
                     grants=row.grants_json,
                     authz_epoch=row.authz_epoch) for row in session.scalars(
                         select(IMMembershipRow).where(IMMembershipRow.tenant_id == tenant_id).order_by(
                             IMMembershipRow.binding_id, IMMembershipRow.actor_id).limit(limit).offset(offset))
            ]

    @router.put("/tenants/{tenant_id}/im/bindings/{binding_id}/members/{actor_id}")
    def im_member(tenant_id: str, binding_id: str, actor_id: str, body: IMMemberGrant):
        import re
        from trpc_service.persistence.models import IMMembershipRow, IMEventReceiptRow
        if not re.fullmatch(r"usr_[0-9a-f]{32}", actor_id):
            raise HTTPException(422, "invalid IM actor identity")

        def change():
            with database.sessions.begin() as session:
                session.get(TenantRow, tenant_id, with_for_update=True)
                if session.get(ChannelBindingRow, (tenant_id, binding_id)) is None:
                    raise LookupError("binding not found")
                # A displayed receipt proves this opaque identity belongs to this binding.
                known = session.scalar(
                    select(IMEventReceiptRow.external_event_id).where(IMEventReceiptRow.tenant_id == tenant_id,
                                                                      IMEventReceiptRow.binding_id == binding_id,
                                                                      IMEventReceiptRow.actor_id == actor_id).limit(1))
                if known is None:
                    raise LookupError("actor not observed on this binding")
                row = session.get(IMMembershipRow, (tenant_id, binding_id, actor_id), with_for_update=True)
                if (row.authz_epoch if row else 0) != body.expected_epoch:
                    raise ConfigConflict("membership changed")
                if row is None:
                    row = IMMembershipRow(tenant_id=tenant_id, binding_id=binding_id, actor_id=actor_id, authz_epoch=0)
                    session.add(row)
                row.active, row.grants_json = body.active, sorted(body.grants)
                row.authz_epoch += 1
                store.audit(session,
                            tenant_id,
                            current_actor(actor),
                            "im_membership_changed",
                            None,
                            metadata={
                                "binding_id": binding_id,
                                "actor_id": actor_id,
                                "authz_epoch": row.authz_epoch
                            })

        return mutate(change)

    @router.get("/backend-profiles")
    def profiles(limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            rows = session.scalars(
                select(BackendProfileRow).order_by(BackendProfileRow.profile_id,
                                                   BackendProfileRow.profile_revision).offset(offset).limit(limit))
            return [store.profile(row).model_dump(mode="json", exclude={"secret_ref"}) for row in rows]

    @router.get("/tenants")
    def tenants(limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            return [
                dict(tenant_id=row.tenant_id,
                     name=row.name,
                     status=row.status,
                     active_config_version=row.active_config_version)
                for row in session.scalars(select(TenantRow).order_by(TenantRow.tenant_id).offset(offset).limit(limit))
            ]

    @router.post("/tenants/{tenant_id}/configs", status_code=201)
    def draft(tenant_id: str, body: ConfigDraft):
        if tenant_id != body.config.tenant_id:
            raise HTTPException(422, "tenant identity mismatch")
        authorize_config(body.config)
        return mutate(lambda: store.save_draft(body, current_actor(actor)))

    @router.get("/tenants/{tenant_id}/configs")
    def versions(tenant_id: str, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            return [
                dict(config_version=row.config_version, status=row.status, content_hash=row.content_hash)
                for row in session.scalars(
                    select(TenantConfigVersionRow).where(TenantConfigVersionRow.tenant_id == tenant_id).order_by(
                        TenantConfigVersionRow.config_version.desc()).offset(offset).limit(limit))
            ]

    @router.get("/tenants/{tenant_id}/configs/{version}")
    def configuration(tenant_id: str, version: int):
        with database.sessions() as session:
            row = session.get(TenantConfigVersionRow, (tenant_id, version))
            if row is None:
                raise HTTPException(404, "configuration not found")
            return {
                "config": row.config_json,
                "status": row.status,
                "profile_revisions": {
                    b.resource_type: b.profile_revision
                    for b in store.bindings(session, tenant_id, version)
                }
            }

    @router.get("/tenants/{tenant_id}/resources")
    def resources(tenant_id: str):
        """Expose the effective admission policy and live queue pressure."""
        from sqlalchemy import func
        with database.sessions() as session:
            tenant = session.get(TenantRow, tenant_id)
            if tenant is None:
                raise HTTPException(404, "tenant not found")
            config_row = session.get(TenantConfigVersionRow, (tenant_id, tenant.active_config_version))
            policy = ResourcePolicy()
            if config_row is not None:
                try:
                    policy = TenantConfig.model_validate(config_row.config_json).resource_policy
                except ValueError:
                    pass
            statuses = ("pending", "retry", "processing")
            queued = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
                InboundMessageRow.tenant_id == tenant_id, InboundMessageRow.status.in_(statuses))) or 0
            processing = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
                InboundMessageRow.tenant_id == tenant_id, InboundMessageRow.status == "processing")) or 0
            minute_ago = utcnow() - timedelta(minutes=1)
            recent = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
                InboundMessageRow.tenant_id == tenant_id, InboundMessageRow.created_at >= minute_ago)) or 0
            return {
                "tenant_id": tenant_id,
                "config_version": tenant.active_config_version,
                "resource_policy": policy.model_dump(mode="json"),
                "usage": {"queued": queued, "processing": processing, "requests_last_minute": recent},
            }

    @router.post("/tenants/{tenant_id}/configs/{version}/publish")
    def publish(tenant_id: str, version: int, body: PublishRequest):
        return mutate(lambda: store.publish(tenant_id, version, body.expected_active_version, current_actor(actor)))

    @router.post("/tenants/{tenant_id}/rollout")
    def rollout(tenant_id: str, body: RolloutRequest):
        return mutate(lambda: store.rollout(tenant_id, body.target_version, body.expected_active_version, body.percent,
                                            current_actor(actor)))

    @router.get("/tenants/{tenant_id}/rollout")
    def rollout_status(tenant_id: str):
        with database.sessions() as session:
            row = session.get(TenantRolloutRow, tenant_id)
            return {
                "active": row is not None,
                "base_version": row.base_version if row else None,
                "target_version": row.target_version if row else None,
                "percent": row.percent if row else 0
            }

    from trpc_service.governance.audit_management import audit_router
    router.include_router(audit_router(database))
    from trpc_service.governance.audit_archive_management import archive_router
    router.include_router(archive_router(database))

    @router.get("/tenants/{tenant_id}/deliveries")
    def deliveries(tenant_id: str, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            return [
                dict(delivery_id=row.outbox_message_id,
                     status=row.status,
                     attempt=row.attempt,
                     error_type=row.error_type,
                     next_retry_at=row.next_retry_at)
                for row in session.scalars(
                    select(OutboxMessageRow).where(OutboxMessageRow.tenant_id == tenant_id).order_by(
                        OutboxMessageRow.created_at, OutboxMessageRow.outbox_message_id).offset(offset).limit(limit))
            ]

    @router.post("/tenants/{tenant_id}/deliveries/{delivery_id}/retry")
    def replay(tenant_id: str, delivery_id: str, body: DeliveryReplayRequest):

        def requeue():
            with database.sessions.begin() as session:
                result = session.execute(
                    update(OutboxMessageRow).where(
                        OutboxMessageRow.tenant_id == tenant_id, OutboxMessageRow.outbox_message_id == delivery_id,
                        OutboxMessageRow.status == "dead_letter",
                        OutboxMessageRow.attempt == body.expected_attempt).values(status="retry",
                                                                                  attempt=0,
                                                                                  error_type=None,
                                                                                  next_retry_at=None,
                                                                                  lease_owner=None,
                                                                                  lease_expires_at=None))
                if result.rowcount != 1:
                    raise ConfigConflict("delivery state changed")
                store.audit(session,
                            tenant_id,
                            current_actor(actor),
                            "delivery_requeued",
                            metadata={
                                "delivery_id": delivery_id,
                                "previous_attempt": body.expected_attempt
                            })

        return mutate(requeue)

    from trpc_service.migration.offline import OfflineMigrations
    migrations = OfflineMigrations(database)

    @router.post("/tenants/{tenant_id}/maintenance")
    def maintenance(tenant_id: str, body: MaintenanceRequest):
        return mutate(lambda: migrations.maintenance(tenant_id, body.enabled, current_actor(actor)))

    @router.get("/tenants/{tenant_id}/maintenance")
    def maintenance_status(tenant_id: str):
        with database.sessions() as session:
            row = session.get(TenantMaintenanceRow, tenant_id)
            return {"enabled": bool(row and row.enabled)}

    @router.post("/tenants/{tenant_id}/migrations", status_code=201)
    def migration_create(tenant_id: str, body: MigrationRequest):
        result = {}

        def create():
            result["job_id"] = migrations.create(tenant_id,
                                                 body.source_version,
                                                 body.target_version,
                                                 current_actor(actor),
                                                 resource_type=body.resource_type)

        mutate(create)
        return result

    @router.get("/tenants/{tenant_id}/migrations")
    def migration_list(tenant_id: str, limit: int = Query(100, ge=1, le=500)):
        with database.sessions() as session:
            return [{
                "job_id": row.job_id,
                "source_version": row.source_version,
                "resource_type": row.resource_type,
                "target_version": row.target_version,
                "status": row.status,
                "copied": row.copied,
                "total": len(row.items_json)
            } for row in session.scalars(
                select(OfflineMigrationRow).where(OfflineMigrationRow.tenant_id == tenant_id).order_by(
                    OfflineMigrationRow.created_at.desc()).limit(limit))]

    @router.post("/tenants/{tenant_id}/migrations/{job_id}/run")
    async def migration_run(tenant_id: str, job_id: str, body: MigrationRunRequest):
        import asyncio
        from trpc_service.storage import ConflictError
        try:
            return await asyncio.to_thread(
                lambda: asyncio.run(migrations.run(tenant_id, job_id, **body.model_dump(), actor=current_actor(actor))))
        except LookupError:
            raise HTTPException(404, "migration or native session not found") from None
        except (ConfigConflict, ConflictError):
            raise HTTPException(409, "migration precondition or verification conflict; no cutover committed") from None

    @router.post("/tenants/{tenant_id}/migrations/{job_id}/abort")
    def migration_abort(tenant_id: str, job_id: str):
        return mutate(lambda: migrations.abort(tenant_id, job_id, current_actor(actor)))

    @router.get("/tenants/{tenant_id}/post-turn")
    def post_turn_tasks(tenant_id: str, limit: int = Query(100, ge=1, le=500)):
        from trpc_service.persistence.models import PostTurnTaskRow
        with database.sessions() as session:
            return [{
                "task_id": row.task_id,
                "session_id": row.session_id,
                "task_type": row.task_type,
                "status": row.status,
                "attempt": row.attempt,
                "error_type": row.error_type
            } for row in session.scalars(
                select(PostTurnTaskRow).where(
                    PostTurnTaskRow.tenant_id == tenant_id).order_by(PostTurnTaskRow.created_at.desc()).limit(limit))]

    @router.post("/tenants/{tenant_id}/post-turn/{task_id}/retry")
    def retry_post_turn(tenant_id: str, task_id: str, body: DeliveryReplayRequest):
        from trpc_service.persistence.models import PostTurnTaskRow, SessionEventRow, ModelAttemptRow

        def retry():
            with database.sessions.begin() as session:
                session.get(TenantRow, tenant_id, with_for_update=True)
                task = session.get(PostTurnTaskRow, task_id, with_for_update=True)
                if task is None or task.tenant_id != tenant_id:
                    raise LookupError("post-turn task not found")
                if task.status != "failed" or task.attempt != body.expected_attempt:
                    raise ConfigConflict("post-turn state changed")
                event = session.scalar(
                    select(SessionEventRow).where(SessionEventRow.tenant_id == tenant_id,
                                                  SessionEventRow.session_id == task.session_id,
                                                  SessionEventRow.seq_no == task.source_event_seq))
                if event is None:
                    raise ConfigConflict("post-turn source is unavailable")
                if task.task_type == "summary" and session.scalar(
                        select(ModelAttemptRow.attempt_id).where(
                            ModelAttemptRow.tenant_id == tenant_id, ModelAttemptRow.execution_id == event.execution_id,
                            ModelAttemptRow.status.in_(("in_flight", "pending_reconciliation"))).limit(1)):
                    raise ConfigConflict("model cost must be reconciled before summary retry")
                task.status, task.attempt = "pending", 0
                task.next_retry_at = task.lease_owner = task.lease_expires_at = task.error_type = None
                store.audit(session,
                            tenant_id,
                            current_actor(actor),
                            "post_turn_requeued",
                            metadata={
                                "task_id": task_id,
                                "previous_attempt": body.expected_attempt
                            })

        return mutate(retry)

    return router
