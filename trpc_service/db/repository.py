"""Tenant-required repositories and a deterministic local implementation.

``InMemoryRepository`` is deliberately a real transactional model rather than a
mock with global dictionaries.  It is used by the zero-credential demo and
exercises the same acceptance, fencing, immutable-release, and hard-budget
invariants as the PostgreSQL repository.  Production deployments use the SQL
repository in :mod:`trpc_service.db.sql_repository` plus RLS.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .context import TenantContext, require_tenant


def _now() -> datetime:
    return datetime.now(UTC)


def deterministic_id(*parts: object, prefix: str) -> str:
    """Return a stable opaque identifier safe to persist across retries."""

    encoded = json.dumps(parts, separators=(",", ":"), ensure_ascii=False, default=str)
    return f"{prefix}_{hashlib.sha256(encoded.encode()).hexdigest()[:32]}"


class RepositoryError(RuntimeError):
    """Base error emitted by the tenant-required repository."""


class NotFoundError(RepositoryError):
    """Requested tenant-scoped record does not exist."""


class ConflictError(RepositoryError):
    """A unique or state transition constraint rejected the mutation."""


class BudgetExceededError(RepositoryError):
    """A conditional hard-budget reservation could not be made."""


class FenceLostError(RepositoryError):
    """A worker no longer owns the claimed execution fence."""


class ExecutionDivergenceError(RepositoryError):
    """A deterministic execution step changed its semantic inputs."""


class SecurityEnvelopeError(RepositoryError):
    """The live security envelope rejected the operation."""


class StorageMigrationError(RepositoryError):
    """A storage migration state-machine invariant was violated."""


@dataclass(frozen=True, slots=True)
class BindingResolution:
    tenant_id: str
    binding_id: str
    provider: str


@dataclass(frozen=True, slots=True)
class AcceptanceResult:
    inbox: dict[str, Any]
    outbox: dict[str, Any]
    duplicate: bool


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    tenant_id: str
    inbox_id: str
    execution_id: str
    attempt_no: int
    session_id: str
    worker_id: str
    lease_fence: int
    routing_epoch: int
    security_epoch: int
    session_version: int
    lease_expires_at: datetime


_ROUTE_TRANSITIONS = {
    "preparing": {"backfilling", "failed", "retired"},
    "backfilling": {"catching_up", "failed", "retired"},
    "catching_up": {"draining", "failed", "retired"},
    "draining": {"verifying", "catching_up", "failed", "retired"},
    "verifying": {"active", "catching_up", "failed", "retired"},
    "active": {"draining", "readonly", "retired", "failed"},
    "readonly": {"retired", "draining", "failed"},
    "retired": set(),
    "failed": {"preparing", "retired"},
}


class InMemoryRepository:
    """Concurrency-safe local persistence implementation.

    Each mutation holds one lock, which models a serializable SQL transaction
    for the small local/demo deployment.  It does *not* claim to emulate RLS;
    its API requires a TenantContext and deliberately offers no cross-tenant
    enumeration method.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tenants: dict[str, dict[str, Any]] = {}
        self._locators: dict[str, BindingResolution] = {}

    @staticmethod
    def _clone(value: Any) -> Any:
        return copy.deepcopy(value)

    def _store(self, context: TenantContext) -> dict[str, Any]:
        context = require_tenant(context)
        try:
            return self._tenants[context.tenant_id]
        except KeyError as exc:
            raise NotFoundError(f"tenant {context.tenant_id!r} does not exist") from exc

    @staticmethod
    def _new_store(
        tenant: dict[str, Any], runtime: dict[str, Any], route: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "tenant": tenant,
            "runtime": runtime,
            "routes": {route["routing_epoch"]: route},
            "agents": {},
            "releases": {},
            "bindings": {},
            "identities": {},
            "sessions": {},
            "events": {},
            "summaries": {},
            "memories": {},
            "memory_projections": {},
            "knowledge_documents": {},
            "artifacts": {},
            "inbox": {},
            "inbox_by_idempotency": {},
            "outbox": {},
            "outbox_by_idempotency": {},
            "execution_attempts": {},
            "budget_accounts": {},
            "budget_reservations": {},
            "reservation_by_execution": {},
            "tool_executions": {},
            "tool_by_step": {},
            "delivery_attempts": {},
            "audit_logs": [],
        }

    # -- tenant/control plane -------------------------------------------------

    async def create_tenant(
        self,
        context: TenantContext,
        *,
        display_name: str,
        audit_policy: Mapping[str, Any] | None = None,
        budget_policy: Mapping[str, Any] | None = None,
        initial_storage_profile: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = require_tenant(context)
        async with self._lock:
            if context.tenant_id in self._tenants:
                raise ConflictError("tenant already exists")
            timestamp = _now()
            tenant = {
                "tenant_id": context.tenant_id,
                "display_name": display_name,
                "status": "active",
                "audit_policy": dict(audit_policy or {}),
                "budget_policy": dict(budget_policy or {}),
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            runtime = {
                "tenant_id": context.tenant_id,
                "routing_epoch": 1,
                "security_epoch": 1,
                "credential_revocation_epoch": 1,
                "execution_mode": "normal",
                "tool_denylist": [],
                "updated_at": timestamp,
            }
            route = {
                "tenant_id": context.tenant_id,
                "routing_epoch": 1,
                "profile": dict(initial_storage_profile or {"kind": "in_memory"}),
                "route_status": "active",
                "source_watermark": None,
                "target_watermark": None,
                "created_at": timestamp,
                "activated_at": timestamp,
            }
            self._tenants[context.tenant_id] = self._new_store(tenant, runtime, route)
            return self._clone(tenant)

    async def get_tenant(self, context: TenantContext) -> dict[str, Any]:
        async with self._lock:
            return self._clone(self._store(context)["tenant"])

    async def get_runtime_state(self, context: TenantContext) -> dict[str, Any]:
        async with self._lock:
            return self._clone(self._store(context)["runtime"])

    async def update_security_envelope(
        self,
        context: TenantContext,
        *,
        execution_mode: str | None = None,
        tool_denylist: Iterable[str] | None = None,
        revoke_credentials: bool = False,
    ) -> dict[str, Any]:
        if execution_mode is not None and execution_mode not in {
            "normal",
            "draining",
            "suspended",
            "emergency_stop",
        }:
            raise ValueError("invalid execution mode")
        async with self._lock:
            state = self._store(context)["runtime"]
            changed = False
            if execution_mode is not None and state["execution_mode"] != execution_mode:
                state["execution_mode"] = execution_mode
                changed = True
            if tool_denylist is not None:
                state["tool_denylist"] = sorted(set(tool_denylist))
                changed = True
            if changed:
                state["security_epoch"] += 1
            if revoke_credentials:
                state["credential_revocation_epoch"] += 1
                state["security_epoch"] += 1
            state["updated_at"] = _now()
            return self._clone(state)

    async def create_storage_route(
        self, context: TenantContext, *, profile: Mapping[str, Any]
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            epoch = max(store["routes"], default=0) + 1
            route = {
                "tenant_id": context.tenant_id,
                "routing_epoch": epoch,
                "profile": dict(profile),
                "route_status": "preparing",
                "source_watermark": None,
                "target_watermark": None,
                "created_at": _now(),
                "activated_at": None,
            }
            store["routes"][epoch] = route
            return self._clone(route)

    async def advance_storage_route(
        self,
        context: TenantContext,
        *,
        routing_epoch: int,
        next_status: str,
        source_watermark: str | None = None,
        target_watermark: str | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            route = self._store(context)["routes"].get(routing_epoch)
            if route is None:
                raise NotFoundError("storage route does not exist")
            if next_status not in _ROUTE_TRANSITIONS[route["route_status"]]:
                raise StorageMigrationError(
                    f"cannot transition {route['route_status']} to {next_status}"
                )
            route["route_status"] = next_status
            if source_watermark is not None:
                route["source_watermark"] = source_watermark
            if target_watermark is not None:
                route["target_watermark"] = target_watermark
            return self._clone(route)

    async def cut_over_storage_route(
        self, context: TenantContext, *, routing_epoch: int
    ) -> dict[str, Any]:
        """Atomically select a verified route and invalidate all stale claims."""

        async with self._lock:
            store = self._store(context)
            target = store["routes"].get(routing_epoch)
            if target is None or target["route_status"] != "verifying":
                raise StorageMigrationError("only a verified route can become active")
            state = store["runtime"]
            if state["execution_mode"] not in {"draining", "normal"}:
                raise StorageMigrationError("tenant is not available for a route cutover")
            for route in store["routes"].values():
                if route["route_status"] == "active":
                    route["route_status"] = "readonly"
            target["route_status"] = "active"
            target["activated_at"] = _now()
            state["routing_epoch"] = routing_epoch
            state["execution_mode"] = "normal"
            state["updated_at"] = _now()
            return self._clone(target)

    async def list_storage_routes(self, context: TenantContext) -> list[dict[str, Any]]:
        async with self._lock:
            return self._clone(list(self._store(context)["routes"].values()))

    # -- immutable application configuration and bindings ---------------------

    async def create_agent(
        self, context: TenantContext, *, agent_id: str, name: str, status: str = "draft"
    ) -> dict[str, Any]:
        if status not in {"draft", "active", "disabled"}:
            raise ValueError("invalid agent status")
        async with self._lock:
            store = self._store(context)
            if agent_id in store["agents"]:
                raise ConflictError("agent already exists")
            timestamp = _now()
            row = {
                "tenant_id": context.tenant_id,
                "agent_id": agent_id,
                "name": name,
                "status": status,
                "active_config_version": None,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            store["agents"][agent_id] = row
            return self._clone(row)

    async def create_agent_release(
        self,
        context: TenantContext,
        *,
        agent_id: str,
        config_version: int,
        created_by: str,
        change_reason: str,
        app_config: Mapping[str, Any] | None = None,
        model_config: Mapping[str, Any] | None = None,
        tool_policy: Mapping[str, Any] | None = None,
        knowledge_config: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            if agent_id not in store["agents"]:
                raise NotFoundError("agent does not exist")
            key = (agent_id, config_version)
            proposed = {
                "tenant_id": context.tenant_id,
                "agent_id": agent_id,
                "config_version": config_version,
                "release_status": "staged",
                "app_config": dict(app_config or {}),
                "model_config": dict(model_config or {}),
                "tool_policy": dict(tool_policy or {}),
                "knowledge_config": dict(knowledge_config or {}),
                "created_by": created_by,
                "change_reason": change_reason,
                "created_at": _now(),
            }
            existing = store["releases"].get(key)
            if existing is not None:
                if {k: v for k, v in existing.items() if k != "release_status"} != {
                    k: v for k, v in proposed.items() if k != "release_status"
                }:
                    raise ConflictError("agent releases are immutable")
                return self._clone(existing)
            store["releases"][key] = proposed
            return self._clone(proposed)

    async def activate_agent_release(
        self, context: TenantContext, *, agent_id: str, config_version: int
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            app = store["agents"].get(agent_id)
            release = store["releases"].get((agent_id, config_version))
            if app is None or release is None:
                raise NotFoundError("agent or release does not exist")
            prior_version = app["active_config_version"]
            if prior_version is not None:
                store["releases"][(agent_id, prior_version)]["release_status"] = "retired"
            release["release_status"] = "active"
            app["active_config_version"] = config_version
            app["status"] = "active"
            app["updated_at"] = _now()
            return self._clone(release)

    async def get_agent_release(
        self, context: TenantContext, *, agent_id: str, config_version: int | None = None
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            if config_version is None:
                app = store["agents"].get(agent_id)
                config_version = app and app["active_config_version"]
            row = store["releases"].get((agent_id, config_version))
            if row is None:
                raise NotFoundError("agent release does not exist")
            return self._clone(row)

    async def create_channel_binding(
        self,
        context: TenantContext,
        *,
        binding_id: str,
        agent_id: str,
        provider: str,
        external_account_id: str,
        webhook_key_hash: str,
        secret_ref: str,
        capabilities: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            if agent_id not in store["agents"]:
                raise NotFoundError("agent does not exist")
            if binding_id in store["bindings"] or webhook_key_hash in self._locators:
                raise ConflictError("binding identifier or webhook key is already registered")
            timestamp = _now()
            row = {
                "tenant_id": context.tenant_id,
                "binding_id": binding_id,
                "agent_id": agent_id,
                "provider": provider,
                "external_account_id": external_account_id,
                "webhook_key_hash": webhook_key_hash,
                "secret_ref": secret_ref,
                "capabilities": dict(capabilities or {}),
                "status": "active",
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            store["bindings"][binding_id] = row
            self._locators[webhook_key_hash] = BindingResolution(
                context.tenant_id, binding_id, provider
            )
            return self._clone(row)

    async def set_channel_binding_status(
        self, context: TenantContext, *, binding_id: str, status: str
    ) -> dict[str, Any]:
        if status not in {"active", "rotating", "disabled"}:
            raise ValueError("invalid channel binding status")
        async with self._lock:
            row = self._store(context)["bindings"].get(binding_id)
            if row is None:
                raise NotFoundError("channel binding does not exist")
            row["status"] = status
            row["updated_at"] = _now()
            if status == "disabled":
                self._locators.pop(row["webhook_key_hash"], None)
            else:
                self._locators[row["webhook_key_hash"]] = BindingResolution(
                    context.tenant_id, binding_id, row["provider"]
                )
            return self._clone(row)

    async def resolve_binding(
        self, *, webhook_key_hash: str, provider: str
    ) -> BindingResolution | None:
        """Protected exact-match locator; this intentionally has no tenant input."""

        async with self._lock:
            result = self._locators.get(webhook_key_hash)
            if result is None or result.provider != provider:
                return None
            return result

    async def get_channel_binding(
        self, context: TenantContext, *, binding_id: str
    ) -> dict[str, Any]:
        async with self._lock:
            row = self._store(context)["bindings"].get(binding_id)
            if row is None:
                raise NotFoundError("channel binding does not exist")
            return self._clone(row)

    async def upsert_identity_mapping(
        self,
        context: TenantContext,
        *,
        provider: str,
        external_account_id: str,
        external_user_id: str,
        subject_id: str,
        attributes: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            key = (provider, external_account_id, external_user_id)
            timestamp = _now()
            existing = store["identities"].get(key)
            if existing is not None and existing["subject_id"] != subject_id:
                raise ConflictError(
                    "external identity cannot be remapped without an explicit migration"
                )
            row = {
                "tenant_id": context.tenant_id,
                "provider": provider,
                "external_account_id": external_account_id,
                "external_user_id": external_user_id,
                "subject_id": subject_id,
                "attributes": dict(attributes or {}),
                "created_at": existing["created_at"] if existing else timestamp,
                "updated_at": timestamp,
            }
            store["identities"][key] = row
            return self._clone(row)

    # -- sessions, durable acceptance, and execution fences -------------------

    async def get_or_create_session(
        self,
        context: TenantContext,
        *,
        session_id: str,
        agent_id: str,
        channel_binding_id: str,
        conversation_type: str,
        conversation_key_hash: str,
        config_version: int | None = None,
    ) -> dict[str, Any]:
        if conversation_type not in {"direct", "group"}:
            raise ValueError("conversation_type must be direct or group")
        async with self._lock:
            store = self._store(context)
            existing = store["sessions"].get(session_id)
            if existing is not None:
                return self._clone(existing)
            app = store["agents"].get(agent_id)
            if app is None:
                raise NotFoundError("agent does not exist")
            selected_version = (
                config_version if config_version is not None else app["active_config_version"]
            )
            if (agent_id, selected_version) not in store["releases"]:
                raise NotFoundError("session requires an existing immutable agent release")
            if channel_binding_id not in store["bindings"]:
                raise NotFoundError("channel binding does not exist")
            timestamp = _now()
            row = {
                "tenant_id": context.tenant_id,
                "session_id": session_id,
                "agent_id": agent_id,
                "channel_binding_id": channel_binding_id,
                "conversation_type": conversation_type,
                "conversation_key_hash": conversation_key_hash,
                "id_rule_version": 1,
                "config_version": selected_version,
                "state": {},
                "version": 0,
                "last_event_seq": 0,
                "active_inbox_id": None,
                "lease_owner": None,
                "lease_expires_at": None,
                "lease_fence": 0,
                "status": "active",
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            store["sessions"][session_id] = row
            return self._clone(row)

    async def accept_inbound(
        self,
        context: TenantContext,
        *,
        inbox_id: str,
        channel_binding_id: str,
        idempotency_key: str,
        external_message_id: str | None,
        session_id: str,
        request_id: str,
        trace_id: str,
        payload_hash: str,
        dispatch_payload: Mapping[str, Any],
    ) -> AcceptanceResult:
        """Durably insert Inbox and inbound Outbox as one atomic operation."""

        async with self._lock:
            store = self._store(context)
            existing_id = store["inbox_by_idempotency"].get(idempotency_key)
            if existing_id is not None:
                existing = store["inbox"][existing_id]
                outbox = next(
                    row
                    for row in store["outbox"].values()
                    if row["event_type"] == "inbound.dispatch" and row["inbox_id"] == existing_id
                )
                return AcceptanceResult(self._clone(existing), self._clone(outbox), True)
            if channel_binding_id not in store["bindings"]:
                raise NotFoundError("channel binding does not exist")
            if session_id not in store["sessions"]:
                raise NotFoundError("session does not exist")
            timestamp = _now()
            inbox = {
                "tenant_id": context.tenant_id,
                "inbox_id": inbox_id,
                "channel_binding_id": channel_binding_id,
                "idempotency_key": idempotency_key,
                "external_message_id": external_message_id,
                "session_id": session_id,
                "status": "queued",
                "execution_id": None,
                "execution_attempt": 0,
                "claimed_lease_fence": None,
                "claimed_routing_epoch": None,
                "claimed_security_epoch": None,
                "request_id": request_id,
                "trace_id": trace_id,
                "payload_hash": payload_hash,
                "received_at": timestamp,
                "updated_at": timestamp,
            }
            outbox_id = deterministic_id(
                context.tenant_id, inbox_id, "inbound.dispatch", prefix="out"
            )
            outbox = {
                "tenant_id": context.tenant_id,
                "outbox_id": outbox_id,
                "aggregate_type": "inbox",
                "aggregate_id": inbox_id,
                "inbox_id": inbox_id,
                "event_type": "inbound.dispatch",
                "payload": dict(dispatch_payload),
                "idempotency_key": f"inbound.dispatch:{inbox_id}",
                "trace_id": trace_id,
                "status": "pending",
                "attempts": 0,
                "lease_owner": None,
                "lease_expires_at": None,
                "available_at": timestamp,
                "created_at": timestamp,
                "published_at": None,
                "delivered_at": None,
            }
            # Assignment occurs only after every deterministic validation above,
            # equivalent to the INSERT Inbox + INSERT Outbox SQL transaction.
            store["inbox"][inbox_id] = inbox
            store["inbox_by_idempotency"][idempotency_key] = inbox_id
            store["outbox"][outbox_id] = outbox
            store["outbox_by_idempotency"][outbox["idempotency_key"]] = outbox_id
            return AcceptanceResult(self._clone(inbox), self._clone(outbox), False)

    async def claim_outbox(
        self,
        context: TenantContext,
        *,
        worker_id: str,
        limit: int = 100,
        lease_seconds: int = 30,
    ) -> list[dict[str, Any]]:
        """Claim pending records with SKIP-LOCKED-equivalent lease semantics."""

        async with self._lock:
            store = self._store(context)
            timestamp = _now()
            rows: list[dict[str, Any]] = []
            for row in sorted(
                store["outbox"].values(),
                key=lambda item: (item["available_at"], item["created_at"]),
            ):
                expired = (
                    row["lease_expires_at"] is not None and row["lease_expires_at"] <= timestamp
                )
                available = row["available_at"] <= timestamp
                if (
                    len(rows) >= limit
                    or not available
                    or row["status"] not in {"pending", "processing"}
                ):
                    continue
                if row["status"] == "processing" and not expired:
                    continue
                row["status"] = "processing"
                row["lease_owner"] = worker_id
                row["lease_expires_at"] = timestamp + timedelta(seconds=lease_seconds)
                row["attempts"] += 1
                rows.append(self._clone(row))
            return rows

    async def mark_outbox_published(
        self, context: TenantContext, *, outbox_id: str, worker_id: str
    ) -> dict[str, Any]:
        async with self._lock:
            row = self._store(context)["outbox"].get(outbox_id)
            if row is None:
                raise NotFoundError("outbox does not exist")
            if row["status"] != "processing" or row["lease_owner"] != worker_id:
                raise FenceLostError("outbox lease is no longer owned")
            row["status"] = "published"
            row["published_at"] = _now()
            row["lease_owner"] = None
            row["lease_expires_at"] = None
            return self._clone(row)

    async def claim_execution(
        self,
        context: TenantContext,
        *,
        inbox_id: str,
        worker_id: str,
        lease_seconds: int = 60,
    ) -> ExecutionClaim | None:
        """Claim one Inbox + Session and record an incrementing durable fence."""

        async with self._lock:
            store = self._store(context)
            state = store["runtime"]
            if state["execution_mode"] != "normal":
                raise SecurityEnvelopeError(
                    f"new executions are disabled ({state['execution_mode']})"
                )
            inbox = store["inbox"].get(inbox_id)
            if inbox is None:
                raise NotFoundError("inbox does not exist")
            if inbox["status"] in {"committed", "reply_pending", "delivered"}:
                return None
            session = store["sessions"].get(inbox["session_id"])
            if session is None:
                raise NotFoundError("session does not exist")
            timestamp = _now()
            current_owner = session["lease_owner"]
            valid_lease = (
                session["lease_expires_at"] is not None and session["lease_expires_at"] > timestamp
            )
            if current_owner is not None and valid_lease:
                return None
            execution_id = inbox["execution_id"] or deterministic_id(
                context.tenant_id, inbox_id, prefix="exe"
            )
            if inbox["execution_id"] is not None and inbox["execution_id"] != execution_id:
                raise ConflictError("inbox execution id is immutable")
            inbox["execution_id"] = execution_id
            inbox["execution_attempt"] += 1
            fence = session["lease_fence"] + 1
            deadline = timestamp + timedelta(seconds=lease_seconds)
            session.update(
                active_inbox_id=inbox_id,
                lease_owner=worker_id,
                lease_expires_at=deadline,
                lease_fence=fence,
                updated_at=timestamp,
            )
            inbox.update(
                status="claimed",
                claimed_lease_fence=fence,
                claimed_routing_epoch=state["routing_epoch"],
                claimed_security_epoch=state["security_epoch"],
                updated_at=timestamp,
            )
            attempt_key = (execution_id, inbox["execution_attempt"])
            store["execution_attempts"][attempt_key] = {
                "tenant_id": context.tenant_id,
                "execution_id": execution_id,
                "attempt_no": inbox["execution_attempt"],
                "inbox_id": inbox_id,
                "session_id": session["session_id"],
                "worker_id": worker_id,
                "lease_fence": fence,
                "routing_epoch": state["routing_epoch"],
                "security_epoch": state["security_epoch"],
                "status": "claimed",
                "lease_expires_at": deadline,
                "claimed_at": timestamp,
                "finished_at": None,
            }
            return ExecutionClaim(
                context.tenant_id,
                inbox_id,
                execution_id,
                inbox["execution_attempt"],
                session["session_id"],
                worker_id,
                fence,
                state["routing_epoch"],
                state["security_epoch"],
                session["version"],
                deadline,
            )

    def _assert_claim_locked(
        self, store: dict[str, Any], claim: ExecutionClaim, *, allow_draining: bool = False
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        inbox = store["inbox"].get(claim.inbox_id)
        session = store["sessions"].get(claim.session_id)
        state = store["runtime"]
        if inbox is None or session is None:
            raise FenceLostError("inbox or session was removed")
        if (not allow_draining and state["execution_mode"] != "normal") or state[
            "execution_mode"
        ] in {"suspended", "emergency_stop"}:
            raise SecurityEnvelopeError("live execution mode rejects this operation")
        expected = (
            inbox["execution_id"] == claim.execution_id
            and inbox["status"] == "claimed"
            and inbox["claimed_lease_fence"] == claim.lease_fence
            and inbox["claimed_routing_epoch"] == state["routing_epoch"] == claim.routing_epoch
            and inbox["claimed_security_epoch"] == state["security_epoch"] == claim.security_epoch
            and session["active_inbox_id"] == claim.inbox_id
            and session["lease_owner"] == claim.worker_id
            and session["lease_fence"] == claim.lease_fence
            and session["lease_expires_at"] is not None
            and session["lease_expires_at"] > _now()
        )
        if not expected:
            raise FenceLostError("execution fence or live epochs no longer match")
        return inbox, session

    async def renew_execution_lease(
        self, context: TenantContext, *, claim: ExecutionClaim, lease_seconds: int = 60
    ) -> ExecutionClaim:
        async with self._lock:
            store = self._store(context)
            _, session = self._assert_claim_locked(store, claim)
            deadline = _now() + timedelta(seconds=lease_seconds)
            session["lease_expires_at"] = deadline
            attempt = store["execution_attempts"][(claim.execution_id, claim.attempt_no)]
            attempt["lease_expires_at"] = deadline
            return ExecutionClaim(
                claim.tenant_id,
                claim.inbox_id,
                claim.execution_id,
                claim.attempt_no,
                claim.session_id,
                claim.worker_id,
                claim.lease_fence,
                claim.routing_epoch,
                claim.security_epoch,
                claim.session_version,
                deadline,
            )

    async def assert_execution_fence(
        self, context: TenantContext, *, claim: ExecutionClaim
    ) -> None:
        async with self._lock:
            self._assert_claim_locked(self._store(context), claim)

    async def commit_execution(
        self,
        context: TenantContext,
        *,
        claim: ExecutionClaim,
        expected_session_version: int,
        events: Iterable[Mapping[str, Any]],
        new_state: Mapping[str, Any],
        reply_payload: Mapping[str, Any] | None = None,
        memory_intents: Iterable[Mapping[str, Any]] = (),
        reservation_id: str | None = None,
        actual_units: int | None = None,
    ) -> dict[str, Any]:
        """Commit fenced Session facts, projection jobs, reply Outbox, and budget."""

        async with self._lock:
            store = self._store(context)
            inbox, session = self._assert_claim_locked(store, claim)
            if session["version"] != expected_session_version:
                raise FenceLostError("session version CAS failed")
            timestamp = _now()
            next_seq = session["last_event_seq"]
            event_rows: list[dict[str, Any]] = []
            for raw in events:
                next_seq += 1
                event_id = str(
                    raw.get("event_id")
                    or deterministic_id(claim.execution_id, next_seq, prefix="evt")
                )
                if any(row["event_id"] == event_id for row in store["events"].values()):
                    raise ConflictError("session event id already exists")
                event_rows.append(
                    {
                        "tenant_id": context.tenant_id,
                        "session_id": claim.session_id,
                        "seq": next_seq,
                        "event_id": event_id,
                        "event_type": raw["event_type"],
                        "role": raw.get("role"),
                        "subject_id": raw.get("subject_id"),
                        "external_message_id": raw.get("external_message_id"),
                        "payload": dict(raw.get("payload", {})),
                        "trace_id": raw.get("trace_id", inbox["trace_id"]),
                        "occurred_at": raw.get("occurred_at", timestamp),
                        "created_at": timestamp,
                    }
                )
            for event in event_rows:
                store["events"][(claim.session_id, event["seq"])] = event
            for intent in memory_intents:
                memory_id = str(
                    intent.get("memory_id")
                    or deterministic_id(
                        claim.execution_id, intent.get("content_hash", ""), prefix="mem"
                    )
                )
                existing = store["memories"].get(memory_id)
                version = 1 if existing is None else existing["version"] + 1
                content = intent.get("content")
                encrypted_ref = intent.get("encrypted_content_ref")
                if (content is None) == (encrypted_ref is None):
                    raise ValueError(
                        "memory intent needs exactly one of content or encrypted_content_ref"
                    )
                row = {
                    "tenant_id": context.tenant_id,
                    "memory_id": memory_id,
                    "session_id": intent.get("session_id", claim.session_id),
                    "subject_id": intent.get("subject_id"),
                    "memory_type": intent.get("memory_type", "fact"),
                    "content": content,
                    "encrypted_content_ref": encrypted_ref,
                    "content_hash": intent["content_hash"],
                    "acl": dict(intent.get("acl", {})),
                    "source_event_id": intent.get("source_event_id"),
                    "version": version,
                    "expires_at": intent.get("expires_at"),
                    "created_at": existing["created_at"] if existing else timestamp,
                    "updated_at": timestamp,
                }
                store["memories"][memory_id] = row
                target_id = str(intent.get("target_id", "local"))
                store["memory_projections"][(memory_id, target_id)] = {
                    "tenant_id": context.tenant_id,
                    "memory_id": memory_id,
                    "target_id": target_id,
                    "requested_version": version,
                    "projected_version": None,
                    "status": "pending",
                    "last_error_code": None,
                    "updated_at": timestamp,
                }
                self._insert_outbox_locked(
                    store,
                    tenant_id=context.tenant_id,
                    aggregate_type="memory",
                    aggregate_id=memory_id,
                    event_type="memory.project.requested",
                    payload={"memory_id": memory_id, "version": version, "target_id": target_id},
                    trace_id=inbox["trace_id"],
                )
            reply: dict[str, Any] | None = None
            if reply_payload is not None:
                reply = self._insert_outbox_locked(
                    store,
                    tenant_id=context.tenant_id,
                    aggregate_type="session",
                    aggregate_id=claim.session_id,
                    event_type="reply.dispatch",
                    payload=dict(reply_payload),
                    trace_id=inbox["trace_id"],
                )
            if reservation_id is not None:
                self._settle_budget_locked(store, reservation_id, actual_units)
            session.update(
                state=dict(new_state),
                version=session["version"] + 1,
                last_event_seq=next_seq,
                active_inbox_id=None,
                lease_owner=None,
                lease_expires_at=None,
                updated_at=timestamp,
            )
            inbox["status"] = "reply_pending" if reply else "committed"
            inbox["updated_at"] = timestamp
            attempt = store["execution_attempts"][(claim.execution_id, claim.attempt_no)]
            attempt["status"] = "committed"
            attempt["finished_at"] = timestamp
            return self._clone(
                {"session": session, "events": event_rows, "reply_outbox": reply, "inbox": inbox}
            )

    def _insert_outbox_locked(
        self,
        store: dict[str, Any],
        *,
        tenant_id: str,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        trace_id: str,
    ) -> dict[str, Any]:
        idempotency_key = f"{aggregate_type}:{aggregate_id}:{event_type}"
        existing_id = store["outbox_by_idempotency"].get(idempotency_key)
        if existing_id is not None:
            return store["outbox"][existing_id]
        timestamp = _now()
        row = {
            "tenant_id": tenant_id,
            "outbox_id": deterministic_id(tenant_id, idempotency_key, prefix="out"),
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "inbox_id": None,
            "event_type": event_type,
            "payload": dict(payload),
            "idempotency_key": idempotency_key,
            "trace_id": trace_id,
            "status": "pending",
            "attempts": 0,
            "lease_owner": None,
            "lease_expires_at": None,
            "available_at": timestamp,
            "created_at": timestamp,
            "published_at": None,
            "delivered_at": None,
        }
        store["outbox"][row["outbox_id"]] = row
        store["outbox_by_idempotency"][idempotency_key] = row["outbox_id"]
        return row

    # -- canonical memory/knowledge/artifacts ---------------------------------

    async def list_recent_memory(
        self, context: TenantContext, *, subject_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self._store(context)["memories"].values()
            if subject_id is not None:
                rows = [row for row in rows if row["subject_id"] == subject_id]
            return self._clone(
                sorted(rows, key=lambda row: row["updated_at"], reverse=True)[:limit]
            )

    async def mark_memory_projected(
        self, context: TenantContext, *, memory_id: str, target_id: str, projected_version: int
    ) -> dict[str, Any]:
        async with self._lock:
            row = self._store(context)["memory_projections"].get((memory_id, target_id))
            if row is None:
                raise NotFoundError("memory projection does not exist")
            if row["projected_version"] is None or row["projected_version"] < projected_version:
                row["projected_version"] = projected_version
            row["status"] = (
                "ready" if row["projected_version"] >= row["requested_version"] else "processing"
            )
            row["updated_at"] = _now()
            return self._clone(row)

    async def put_knowledge_document(
        self, context: TenantContext, **document: Any
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            document_id = document["document_id"]
            existing = store["knowledge_documents"].get(document_id)
            timestamp = _now()
            row = {
                "tenant_id": context.tenant_id,
                "document_id": document_id,
                "knowledge_base_id": document["knowledge_base_id"],
                "object_uri": document["object_uri"],
                "checksum": document["checksum"],
                "acl": dict(document.get("acl", {})),
                "version": (existing or {}).get("version", 0) + 1,
                "index_status": document.get("index_status", "pending"),
                "created_at": (existing or {}).get("created_at", timestamp),
                "updated_at": timestamp,
            }
            store["knowledge_documents"][document_id] = row
            return self._clone(row)

    async def put_artifact(self, context: TenantContext, **artifact: Any) -> dict[str, Any]:
        if artifact.get("byte_size", 0) < 0:
            raise ValueError("artifact byte_size must not be negative")
        async with self._lock:
            store = self._store(context)
            if (
                artifact.get("session_id") is not None
                and artifact["session_id"] not in store["sessions"]
            ):
                raise NotFoundError("artifact session does not exist")
            artifact_id = artifact["artifact_id"]
            if artifact_id in store["artifacts"]:
                raise ConflictError("artifact already exists")
            row = {"tenant_id": context.tenant_id, **artifact, "created_at": _now()}
            store["artifacts"][artifact_id] = row
            return self._clone(row)

    # -- hard budgets ----------------------------------------------------------

    async def create_budget_account(
        self,
        context: TenantContext,
        *,
        budget_name: str,
        unit: str,
        period_start: datetime,
        period_end: datetime,
        limit_units: int,
    ) -> dict[str, Any]:
        if (
            unit not in {"cost_micros", "tokens", "tool_units"}
            or limit_units < 0
            or period_end <= period_start
        ):
            raise ValueError("invalid budget account")
        async with self._lock:
            store = self._store(context)
            key = (budget_name, period_start)
            if key in store["budget_accounts"]:
                raise ConflictError("budget account already exists")
            row = {
                "tenant_id": context.tenant_id,
                "budget_name": budget_name,
                "unit": unit,
                "period_start": period_start,
                "period_end": period_end,
                "limit_units": limit_units,
                "reserved_units": 0,
                "spent_units": 0,
                "version": 0,
                "updated_at": _now(),
            }
            store["budget_accounts"][key] = row
            return self._clone(row)

    async def reserve_budget(
        self,
        context: TenantContext,
        *,
        reservation_id: str,
        execution_id: str,
        budget_name: str,
        period_start: datetime,
        estimated_units: int,
        expires_at: datetime,
    ) -> dict[str, Any]:
        if estimated_units < 0:
            raise ValueError("estimated units must not be negative")
        async with self._lock:
            store = self._store(context)
            existing = store["budget_reservations"].get(reservation_id)
            if existing is not None:
                if (existing["execution_id"], existing["estimated_units"]) != (
                    execution_id,
                    estimated_units,
                ):
                    raise ConflictError("budget reservation id is immutable")
                return self._clone(existing)
            account = store["budget_accounts"].get((budget_name, period_start))
            if account is None:
                raise NotFoundError("budget account does not exist")
            execution_key = (execution_id, budget_name, period_start)
            if execution_key in store["reservation_by_execution"]:
                return self._clone(
                    store["budget_reservations"][store["reservation_by_execution"][execution_key]]
                )
            if (
                account["spent_units"] + account["reserved_units"] + estimated_units
                > account["limit_units"]
            ):
                raise BudgetExceededError("hard budget would be exceeded")
            account["reserved_units"] += estimated_units
            account["version"] += 1
            account["updated_at"] = _now()
            row = {
                "tenant_id": context.tenant_id,
                "reservation_id": reservation_id,
                "budget_name": budget_name,
                "period_start": period_start,
                "execution_id": execution_id,
                "estimated_units": estimated_units,
                "actual_units": None,
                "status": "reserved",
                "expires_at": expires_at,
                "created_at": _now(),
                "settled_at": None,
            }
            store["budget_reservations"][reservation_id] = row
            store["reservation_by_execution"][execution_key] = reservation_id
            return self._clone(row)

    def _settle_budget_locked(
        self, store: dict[str, Any], reservation_id: str, actual_units: int | None
    ) -> dict[str, Any]:
        reservation = store["budget_reservations"].get(reservation_id)
        if reservation is None:
            raise NotFoundError("budget reservation does not exist")
        if reservation["status"] == "settled":
            return reservation
        if reservation["status"] != "reserved" or actual_units is None or actual_units < 0:
            raise ConflictError("budget reservation cannot be settled")
        if actual_units > reservation["estimated_units"]:
            # The caller must atomically reserve an extension before committing.
            raise BudgetExceededError("actual usage exceeds hard reservation")
        account = store["budget_accounts"][
            (reservation["budget_name"], reservation["period_start"])
        ]
        account["reserved_units"] -= reservation["estimated_units"]
        account["spent_units"] += actual_units
        account["version"] += 1
        account["updated_at"] = _now()
        reservation["actual_units"] = actual_units
        reservation["status"] = "settled"
        reservation["settled_at"] = _now()
        return reservation

    async def settle_budget(
        self, context: TenantContext, *, reservation_id: str, actual_units: int
    ) -> dict[str, Any]:
        async with self._lock:
            return self._clone(
                self._settle_budget_locked(self._store(context), reservation_id, actual_units)
            )

    async def release_budget_reservation(
        self,
        context: TenantContext,
        *,
        reservation_id: str,
        definitively_terminated: bool,
        has_unknown_external_operation: bool,
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            row = store["budget_reservations"].get(reservation_id)
            if row is None:
                raise NotFoundError("budget reservation does not exist")
            if row["status"] != "reserved":
                return self._clone(row)
            if not definitively_terminated or has_unknown_external_operation:
                raise ConflictError("unknown or live execution reservations must be retained")
            account = store["budget_accounts"][(row["budget_name"], row["period_start"])]
            account["reserved_units"] -= row["estimated_units"]
            account["version"] += 1
            account["updated_at"] = _now()
            row["status"] = "released"
            row["settled_at"] = _now()
            return self._clone(row)

    # -- tool and delivery ledgers --------------------------------------------

    async def prepare_tool_execution(
        self,
        context: TenantContext,
        *,
        claim: ExecutionClaim,
        tool_step: int,
        tool_name: str,
        arguments_hash: str,
        retry_capability: str,
        trace_id: str,
    ) -> dict[str, Any]:
        if retry_capability not in {"idempotent", "queryable", "non_retriable"}:
            raise ValueError("invalid tool retry capability")
        async with self._lock:
            store = self._store(context)
            inbox, _ = self._assert_claim_locked(store, claim)
            if tool_name in store["runtime"]["tool_denylist"]:
                raise SecurityEnvelopeError("tool is denied by the live security envelope")
            key = (claim.execution_id, tool_step)
            existing_id = store["tool_by_step"].get(key)
            if existing_id is not None:
                existing = store["tool_executions"][existing_id]
                if (
                    existing["arguments_hash"] != arguments_hash
                    or existing["tool_name"] != tool_name
                ):
                    raise ExecutionDivergenceError("a deterministic tool step changed arguments")
                return self._clone(existing)
            tool_call_id = deterministic_id(
                claim.inbox_id, claim.execution_id, tool_step, prefix="tool"
            )
            row = {
                "tenant_id": context.tenant_id,
                "tool_call_id": tool_call_id,
                "inbox_id": inbox["inbox_id"],
                "execution_id": claim.execution_id,
                "session_id": claim.session_id,
                "tool_step": tool_step,
                "tool_name": tool_name,
                "arguments_hash": arguments_hash,
                "retry_capability": retry_capability,
                "provider_idempotency_key": tool_call_id
                if retry_capability == "idempotent"
                else None,
                "provider_operation_id": None,
                "lease_fence": claim.lease_fence,
                "routing_epoch": claim.routing_epoch,
                "security_epoch": claim.security_epoch,
                "status": "prepared",
                "result_ref": None,
                "last_error_code": None,
                "resolved_by": None,
                "trace_id": trace_id,
                "created_at": _now(),
                "updated_at": _now(),
            }
            store["tool_executions"][tool_call_id] = row
            store["tool_by_step"][key] = tool_call_id
            return self._clone(row)

    async def mark_tool_running(
        self, context: TenantContext, *, claim: ExecutionClaim, tool_call_id: str
    ) -> dict[str, Any]:
        async with self._lock:
            store = self._store(context)
            self._assert_claim_locked(store, claim)
            row = store["tool_executions"].get(tool_call_id)
            if row is None:
                raise NotFoundError("tool execution does not exist")
            if row["status"] == "running":
                return self._clone(row)
            if row["status"] not in {"prepared", "confirmed"}:
                raise ConflictError("tool intent cannot be started")
            row["status"] = "running"
            row["updated_at"] = _now()
            return self._clone(row)

    async def finish_tool_execution(
        self,
        context: TenantContext,
        *,
        tool_call_id: str,
        status: str,
        result_ref: str | None = None,
        error_code: str | None = None,
        provider_operation_id: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"succeeded", "failed", "unknown", "reconciling", "manual_review"}:
            raise ValueError("invalid terminal tool status")
        async with self._lock:
            row = self._store(context)["tool_executions"].get(tool_call_id)
            if row is None:
                raise NotFoundError("tool execution does not exist")
            if row["status"] not in {"running", "prepared", "confirmed", "reconciling"}:
                raise ConflictError("tool execution is already resolved")
            row.update(
                status=status,
                result_ref=result_ref,
                last_error_code=error_code,
                provider_operation_id=provider_operation_id,
                updated_at=_now(),
            )
            return self._clone(row)

    async def prepare_delivery_attempt(
        self,
        context: TenantContext,
        *,
        delivery_id: str,
        outbox_id: str,
        session_id: str,
        channel_binding_id: str,
        retry_capability: str,
        request_hash: str,
        trace_id: str,
    ) -> dict[str, Any]:
        if retry_capability not in {"idempotent", "queryable", "non_retriable"}:
            raise ValueError("invalid delivery retry capability")
        async with self._lock:
            store = self._store(context)
            if (
                outbox_id not in store["outbox"]
                or session_id not in store["sessions"]
                or channel_binding_id not in store["bindings"]
            ):
                raise NotFoundError("delivery references do not exist")
            matching = [
                row
                for row in store["delivery_attempts"].values()
                if row["delivery_id"] == delivery_id
            ]
            attempt_no = len(matching) + 1
            row = {
                "tenant_id": context.tenant_id,
                "delivery_id": delivery_id,
                "attempt_no": attempt_no,
                "outbox_id": outbox_id,
                "session_id": session_id,
                "channel_binding_id": channel_binding_id,
                "retry_capability": retry_capability,
                "provider_idempotency_key": delivery_id
                if retry_capability == "idempotent"
                else None,
                "provider_message_id": None,
                "request_hash": request_hash,
                "status": "prepared",
                "last_error_code": None,
                "trace_id": trace_id,
                "started_at": _now(),
                "finished_at": None,
            }
            store["delivery_attempts"][(delivery_id, attempt_no)] = row
            return self._clone(row)

    async def update_delivery_attempt(
        self,
        context: TenantContext,
        *,
        delivery_id: str,
        attempt_no: int,
        status: str,
        provider_message_id: str | None = None,
        error_code: str | None = None,
    ) -> dict[str, Any]:
        if status not in {
            "sending",
            "accepted",
            "failed",
            "unknown",
            "reconciling",
            "manual_review",
        }:
            raise ValueError("invalid delivery status")
        async with self._lock:
            row = self._store(context)["delivery_attempts"].get((delivery_id, attempt_no))
            if row is None:
                raise NotFoundError("delivery attempt does not exist")
            row.update(
                status=status, provider_message_id=provider_message_id, last_error_code=error_code
            )
            if status in {"accepted", "failed", "unknown", "manual_review"}:
                row["finished_at"] = _now()
            return self._clone(row)

    async def write_audit_log(self, context: TenantContext, **entry: Any) -> dict[str, Any]:
        if not entry.get("decision") or not entry.get("trace_id") or not entry.get("request_id"):
            raise ValueError("audit entries require decision, trace_id, and request_id")
        async with self._lock:
            row = {
                "tenant_id": context.tenant_id,
                "audit_id": entry.get("audit_id") or deterministic_id(entry, prefix="audit"),
                "occurred_at": entry.get("occurred_at", _now()),
                **entry,
            }
            self._store(context)["audit_logs"].append(row)
            return self._clone(row)

    async def list_audit_logs(
        self, context: TenantContext, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with self._lock:
            return self._clone(self._store(context)["audit_logs"][-limit:])


# Explicit semantic repository names make dependency injection readable.  They
# share the one transaction boundary above in local mode; production code can
# swap each protocol for a SQL implementation without changing the caller's
# TenantContext contract.
TenantRepository = InMemoryRepository
AgentRepository = InMemoryRepository
StorageRouteRepository = InMemoryRepository
ChannelRepository = InMemoryRepository
SessionRepository = InMemoryRepository
MemoryRepository = InMemoryRepository
ArtifactRepository = InMemoryRepository
InboxRepository = InMemoryRepository
OutboxRepository = InMemoryRepository
ExecutionRepository = InMemoryRepository
BudgetRepository = InMemoryRepository
ToolExecutionRepository = InMemoryRepository
DeliveryRepository = InMemoryRepository
AuditRepository = InMemoryRepository
