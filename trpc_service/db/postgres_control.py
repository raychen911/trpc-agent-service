"""PostgreSQL implementation of the control-plane API.

All normal reads and writes run inside a tenant-scoped transaction.  Callback
resolution is the only intentional exception: it invokes the tightly scoped
``SECURITY DEFINER app_security.resolve_binding`` function using a webhook-key
hash, then immediately resumes tenant-scoped access.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from trpc_service.control import (
    Agent,
    Binding,
    ControlConflict,
    ControlNotFound,
    Release,
    Tenant,
    webhook_key_hash,
)
from trpc_service.runtime.models import TenantContext

from .postgres import PostgresConnections


def _now() -> datetime:
    return datetime.now(UTC)


def _context(tenant_id: str, actor: str = "control") -> TenantContext:
    return TenantContext(tenant_id=tenant_id, actor_id=actor)


def _is_unique_violation(error: Exception) -> bool:
    return getattr(error, "sqlstate", None) == "23505"


def _json(value: Any) -> Any:
    """Adapt JSON values explicitly for psycopg's PostgreSQL codec."""

    from psycopg.types.json import Jsonb

    return Jsonb(value)


class PostgresControlPlane:
    """Durable control state shared by API, worker, and dispatcher processes."""

    def __init__(self, database_url: str, database_role: str | None = None) -> None:
        self._connections = PostgresConnections(database_url, database_role)

    @staticmethod
    def _tenant(row: dict[str, Any]) -> Tenant:
        return Tenant(
            tenant_id=str(row["tenant_id"]),
            display_name=str(row["display_name"]),
            status=str(row["status"]),
            audit_policy=dict(row["audit_policy"] or {}),
            budget_policy=dict(row["budget_policy"] or {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _agent(row: dict[str, Any]) -> Agent:
        return Agent(
            tenant_id=str(row["tenant_id"]),
            agent_id=str(row["agent_id"]),
            name=str(row["name"]),
            status=str(row["status"]),
            active_config_version=row["active_config_version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _release(row: dict[str, Any]) -> Release:
        return Release(
            tenant_id=str(row["tenant_id"]),
            agent_id=str(row["agent_id"]),
            version=int(row["config_version"]),
            app_config=dict(row["app_config"] or {}),
            model_config=dict(row["model_config"] or {}),
            tool_policy=dict(row["tool_policy"] or {}),
            knowledge_config=dict(row["knowledge_config"] or {}),
            created_by=str(row["created_by"]),
            change_reason=str(row["change_reason"]),
            release_status=str(row["release_status"]),
            created_at=row["created_at"],
        )

    @staticmethod
    def _binding(row: dict[str, Any]) -> Binding:
        return Binding(
            tenant_id=str(row["tenant_id"]),
            binding_id=str(row["binding_id"]),
            agent_id=str(row["agent_id"]),
            provider=str(row["provider"]),
            external_account_id=str(row["external_account_id"]),
            key_hash=str(row["webhook_key_hash"]),
            secret_ref=str(row["secret_ref"]),
            capabilities=dict(row["capabilities"] or {}),
            status=str(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _one(row: dict[str, Any] | None, message: str) -> dict[str, Any]:
        if row is None:
            raise ControlNotFound(message)
        return row

    def create_tenant(
        self,
        tenant_id: str,
        display_name: str,
        *,
        audit_policy: dict[str, Any] | None = None,
        budget_policy: dict[str, Any] | None = None,
    ) -> Tenant:
        now = _now()
        context = _context(tenant_id, "tenant-bootstrap")
        try:
            with self._connections.tenant(context) as connection:
                connection.execute(
                    """
                    INSERT INTO tenant (tenant_id, display_name, status, audit_policy, budget_policy, created_at, updated_at)
                    VALUES (%s, %s, 'active', %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        display_name,
                        _json(audit_policy or {}),
                        _json(budget_policy or {}),
                        now,
                        now,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO tenant_runtime_state
                    (tenant_id, routing_epoch, security_epoch, credential_revocation_epoch, execution_mode, tool_denylist, updated_at)
                    VALUES (%s, 1, 1, 1, 'normal', '[]'::jsonb, %s)
                    """,
                    (tenant_id, now),
                )
                connection.execute(
                    """
                    INSERT INTO tenant_locator (tenant_id, enabled, created_at, updated_at)
                    VALUES (%s, true, %s, %s)
                    """,
                    (tenant_id, now, now),
                )
                connection.execute(
                    """
                    INSERT INTO storage_route
                    (tenant_id, routing_epoch, profile, route_status, created_at, activated_at)
                    VALUES (%s, 1, %s, 'active', %s, %s)
                    """,
                    (tenant_id, _json({"profile": "postgres-default"}), now, now),
                )
        except Exception as exc:
            if _is_unique_violation(exc):
                raise ControlConflict(f"tenant {tenant_id!r} already exists") from exc
            raise
        return self.tenant(tenant_id)

    def tenant(self, tenant_id: str) -> Tenant:
        with self._connections.tenant(_context(tenant_id)) as connection:
            row = connection.execute(
                "SELECT * FROM tenant WHERE tenant_id = %s", (tenant_id,)
            ).fetchone()
        return self._tenant(self._one(row, f"tenant {tenant_id!r} does not exist"))

    def tenant_ids(self) -> list[str]:
        """List active tenants for a privileged dispatcher scheduler.

        The deployment role used for this method must be the dedicated control
        plane/dispatcher identity.  Normal data-plane queries remain tenant
        scoped through RLS and never call this method.
        """

        with self._connections.bootstrap() as connection:
            rows = connection.execute(
                "SELECT tenant_id FROM tenant_locator WHERE enabled ORDER BY tenant_id"
            ).fetchall()
        return [str(row["tenant_id"]) for row in rows]

    def create_agent(self, tenant_id: str, agent_id: str, name: str) -> Agent:
        now = _now()
        try:
            with self._connections.tenant(_context(tenant_id)) as connection:
                connection.execute(
                    """
                    INSERT INTO agent_app (tenant_id, agent_id, name, status, created_at, updated_at)
                    VALUES (%s, %s, %s, 'draft', %s, %s)
                    """,
                    (tenant_id, agent_id, name, now, now),
                )
        except Exception as exc:
            if _is_unique_violation(exc):
                raise ControlConflict(f"agent {agent_id!r} already exists") from exc
            raise
        return self.agent(tenant_id, agent_id)

    def agent(self, tenant_id: str, agent_id: str) -> Agent:
        with self._connections.tenant(_context(tenant_id)) as connection:
            row = connection.execute(
                "SELECT * FROM agent_app WHERE tenant_id = %s AND agent_id = %s",
                (tenant_id, agent_id),
            ).fetchone()
        return self._agent(self._one(row, f"agent {agent_id!r} does not exist"))

    def create_release(self, tenant_id: str, agent_id: str, version: int, **values: Any) -> Release:
        now = _now()
        try:
            with self._connections.tenant(_context(tenant_id)) as connection:
                connection.execute(
                    """
                    INSERT INTO agent_release
                    (tenant_id, agent_id, config_version, release_status, app_config, model_config,
                     tool_policy, knowledge_config, created_by, change_reason, created_at)
                    VALUES (%s, %s, %s, 'staged', %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        agent_id,
                        version,
                        _json(values.get("app_config") or {}),
                        _json(values.get("model_config") or {}),
                        _json(values.get("tool_policy") or {}),
                        _json(values.get("knowledge_config") or {}),
                        values.get("created_by", "admin"),
                        values.get("change_reason", "API release"),
                        now,
                    ),
                )
        except Exception as exc:
            if _is_unique_violation(exc):
                raise ControlConflict(f"release {version} already exists") from exc
            raise
        return self.release(tenant_id, agent_id, version)

    def release(self, tenant_id: str, agent_id: str, version: int) -> Release:
        with self._connections.tenant(_context(tenant_id)) as connection:
            row = connection.execute(
                """
                SELECT * FROM agent_release
                WHERE tenant_id = %s AND agent_id = %s AND config_version = %s
                """,
                (tenant_id, agent_id, version),
            ).fetchone()
        return self._release(self._one(row, f"release {version} does not exist"))

    def activate_release(self, tenant_id: str, agent_id: str, version: int) -> Release:
        now = _now()
        with self._connections.tenant(_context(tenant_id)) as connection:
            release = connection.execute(
                """
                SELECT * FROM agent_release
                WHERE tenant_id = %s AND agent_id = %s AND config_version = %s FOR UPDATE
                """,
                (tenant_id, agent_id, version),
            ).fetchone()
            self._one(release, f"release {version} does not exist")
            connection.execute(
                """
                UPDATE agent_release SET release_status = 'retired'
                WHERE tenant_id = %s AND agent_id = %s AND release_status = 'active'
                """,
                (tenant_id, agent_id),
            )
            connection.execute(
                """
                UPDATE agent_release SET release_status = 'active'
                WHERE tenant_id = %s AND agent_id = %s AND config_version = %s
                """,
                (tenant_id, agent_id, version),
            )
            updated = connection.execute(
                """
                UPDATE agent_app SET active_config_version = %s, status = 'active', updated_at = %s
                WHERE tenant_id = %s AND agent_id = %s RETURNING tenant_id
                """,
                (version, now, tenant_id, agent_id),
            ).fetchone()
            self._one(updated, f"agent {agent_id!r} does not exist")
        return self.release(tenant_id, agent_id, version)

    def rollback_agent(self, tenant_id: str, agent_id: str) -> Release:
        agent = self.agent(tenant_id, agent_id)
        with self._connections.tenant(_context(tenant_id)) as connection:
            row = connection.execute(
                """
                SELECT config_version FROM agent_release
                WHERE tenant_id = %s AND agent_id = %s
                  AND (%s IS NULL OR config_version < %s)
                ORDER BY config_version DESC LIMIT 1
                """,
                (tenant_id, agent_id, agent.active_config_version, agent.active_config_version),
            ).fetchone()
        if row is None:
            raise ControlConflict("agent has no earlier release to roll back to")
        return self.activate_release(tenant_id, agent_id, int(row["config_version"]))

    def active_release(self, tenant_id: str, agent_id: str) -> Release:
        agent = self.agent(tenant_id, agent_id)
        if agent.active_config_version is None:
            raise ControlConflict("agent does not have an active release")
        return self.release(tenant_id, agent_id, agent.active_config_version)

    def create_binding(self, tenant_id: str, *, webhook_key: str, **values: Any) -> Binding:
        now = _now()
        digest = webhook_key_hash(webhook_key)
        try:
            with self._connections.tenant(_context(tenant_id)) as connection:
                connection.execute(
                    """
                    INSERT INTO channel_binding
                    (tenant_id, binding_id, agent_id, provider, external_account_id, webhook_key_hash,
                     secret_ref, capabilities, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s)
                    """,
                    (
                        tenant_id,
                        values["binding_id"],
                        values["agent_id"],
                        values["provider"],
                        values["external_account_id"],
                        digest,
                        values["secret_ref"],
                        _json(values.get("capabilities") or {}),
                        now,
                        now,
                    ),
                )
        except Exception as exc:
            if _is_unique_violation(exc):
                raise ControlConflict("binding, account, or webhook key already exists") from exc
            raise
        return self.binding(tenant_id, str(values["binding_id"]))

    def binding(self, tenant_id: str, binding_id: str) -> Binding:
        with self._connections.tenant(_context(tenant_id)) as connection:
            row = connection.execute(
                "SELECT * FROM channel_binding WHERE tenant_id = %s AND binding_id = %s",
                (tenant_id, binding_id),
            ).fetchone()
        return self._binding(self._one(row, f"binding {binding_id!r} does not exist"))

    def bindings_for_provider(self, provider: str) -> list[Binding]:
        """List active bindings through per-tenant RLS transactions.

        The dispatcher first reads the scheduler-only tenant locator, then each
        channel row is read under that tenant's normal RLS context.  This keeps
        a connection gateway from getting a broad unscoped channel-table grant.
        """

        bindings: list[Binding] = []
        for tenant_id in self.tenant_ids():
            with self._connections.tenant(_context(tenant_id, "channel-gateway")) as connection:
                rows = connection.execute(
                    """
                    SELECT * FROM channel_binding
                    WHERE provider = %s AND status = 'active'
                    ORDER BY binding_id
                    """,
                    (provider,),
                ).fetchall()
            bindings.extend(self._binding(row) for row in rows)
        return bindings

    def resolve_callback_binding(self, provider: str, webhook_key: str) -> Binding:
        with self._connections.bootstrap() as connection:
            row = connection.execute(
                """
                SELECT resolved_tenant_id, resolved_binding_id
                FROM app_security.resolve_binding(%s, %s)
                """,
                (webhook_key_hash(webhook_key), provider),
            ).fetchone()
        if row is None:
            raise ControlNotFound("unknown channel binding")
        return self.binding(str(row["resolved_tenant_id"]), str(row["resolved_binding_id"]))

    @staticmethod
    def serialize(value: object) -> dict[str, Any]:
        data = asdict(value)  # type: ignore[arg-type]
        for key, item in list(data.items()):
            if isinstance(item, datetime):
                data[key] = item.isoformat()
        return data
