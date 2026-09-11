"""Durable approved-action consumer; a crashed side effect is never blindly replayed."""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import uuid

from sqlalchemy import select, or_
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import run_filters

from trpc_service.channels.persistence import as_utc, database_now
from trpc_service.persistence.models import (ActionJobRow, PendingActionRow, TenantRow, IMMembershipRow,
                                             ToolInvocationRow, ToolResultRow)
from trpc_service.reliability import SessionCoordinator
from trpc_service.reliability.tool_execution import DurableTools
from trpc_service.reliability.tool_invocation import _hash
from trpc_service.reliability.execution import SessionBusyError
from trpc_service.telemetry.runtime import operation
from .actions import ActionConflict
from .filters import ToolGovernanceFilter
from .model_filter import database_call


@dataclass(frozen=True)
class ActionDefinition:
    name: str
    revision: str
    # Pure validation returns (normalized arguments, immutable preconditions, public summary).
    prepare: object
    # Check current external object/version without changing it; execute must also
    # enforce the precondition atomically in its external write (e.g. If-Match).
    check: object
    execute: object


class ActionWorker:

    def __init__(self, service, catalog, *, owner=None, timeout=30):
        if not 0 < timeout <= 120:
            raise ValueError("action timeout must be bounded")
        self.service, self.database, self.catalog = service, service.database, dict(catalog)
        self.owner, self.timeout = owner or uuid.uuid4().hex, timeout
        self.coordinator = SessionCoordinator(self.database)

    def choose(self):
        with self.database.sessions() as session:
            row = session.scalar(
                select(ActionJobRow).where(
                    or_(ActionJobRow.status == "pending", (ActionJobRow.status == "executing") &
                        (ActionJobRow.lease_expires_at <= database_now(session)))).order_by(
                            ActionJobRow.created_at).limit(1))
            return (row.tenant_id, row.action_id) if row else None

    def claim(self, key):
        with self.database.sessions.begin() as session:
            session.get(TenantRow, key[0], with_for_update=True)
            job = session.get(ActionJobRow, key, with_for_update=True)
            action = session.get(PendingActionRow, key, with_for_update=True)
            if job.status == "executing":
                if as_utc(job.lease_expires_at) > database_now(session):
                    return None
                self.finish_in_session(session, action, job, recovery=True)
                return None
            if job.status != "pending" or action.status != "approved":
                return None
            spec = self.catalog.get((action.tenant_id, action.tool_name, action.tool_revision))
            try:
                route = self.service.validate_current(session, action)
                approver = session.get(IMMembershipRow, (action.tenant_id, action.binding_id, action.approver_actor_id))
                if (spec is None or as_utc(action.expires_at) <= database_now(session) or approver is None
                        or not approver.active or approver.authz_epoch != action.approver_epoch
                        or "approve" not in approver.grants_json):
                    raise PermissionError("action approval is no longer valid")
            except (PermissionError, ValueError, LookupError):
                action.status, action.revision, job.status = "cancelled", action.revision + 1, "cancelled"
                self.service.audit(session, action, "action_cancelled_before_execution", self.owner)
                self.service.notice(session, action, "cancelled", f"操作 {action.action_id}：授权或配置已失效，未执行。")
                return None
            return action, route, spec

    def begin(self, key):
        with self.database.sessions.begin() as session:
            self.service.lock_tenant(session, key[0])
            action = session.get(PendingActionRow, key, with_for_update=True)
            job = session.get(ActionJobRow, key, with_for_update=True)
            if action.status != "approved" or job.status != "pending":
                raise ActionConflict("action was claimed by another worker")
            self.service.validate_current(session, action)
            approver = session.get(IMMembershipRow, (key[0], action.binding_id, action.approver_actor_id))
            if (approver is None or not approver.active or approver.authz_epoch != action.approver_epoch
                    or "approve" not in approver.grants_json or as_utc(action.expires_at) <= database_now(session)):
                raise PermissionError("action authorization changed before dispatch")
            action.status, action.revision = "executing", action.revision + 1
            job.status, job.owner_id = "executing", self.owner
            job.lease_expires_at = database_now(session) + timedelta(seconds=self.timeout + 30)
            self.service.audit(session, action, "action_executing", self.owner)

    def cancel_precondition(self, key):
        with self.database.sessions.begin() as session:
            session.get(TenantRow, key[0], with_for_update=True)
            action = session.get(PendingActionRow, key, with_for_update=True)
            job = session.get(ActionJobRow, key, with_for_update=True)
            if action.status != "approved":
                return
            action.status, action.revision, job.status = "cancelled", action.revision + 1, "cancelled"
            self.service.audit(session, action, "action_precondition_changed", self.owner)
            self.service.notice(session, action, "cancelled", f"操作 {action.action_id}：对象条件已变化，未执行。")

    def authorize_dispatch(self, key):
        with self.database.sessions.begin() as session:
            self.service.lock_tenant(session, key[0])
            action = session.get(PendingActionRow, key, with_for_update=True)
            job = session.get(ActionJobRow, key)
            self.service.validate_current(session, action)
            member = session.get(IMMembershipRow, (key[0], action.binding_id, action.approver_actor_id))
            if (action.status != "executing" or job.status != "executing" or job.owner_id != self.owner
                    or as_utc(job.lease_expires_at) <= database_now(session) or member is None or not member.active
                    or member.authz_epoch != action.approver_epoch or "approve" not in member.grants_json):
                raise PermissionError("action authorization changed before tool dispatch")

    def finish_in_session(self, session, action, job, *, recovery=False):
        invocation = session.get(ToolInvocationRow, (action.execution_id, "action:" + action.action_id))
        result = session.get(ToolResultRow, (action.execution_id, "action:" + action.action_id))
        succeeded = (invocation is not None and invocation.tenant_id == action.tenant_id
                     and invocation.tool_name == action.tool_name and invocation.status == "succeeded"
                     and result is not None and result.tenant_id == action.tenant_id
                     and invocation.request_hash == _hash(self.service.payload(action)["arguments"])
                     and invocation.result_hash == _hash(result.result_json.get("value")))
        action.status = "succeeded" if succeeded else "unknown_outcome"
        action.revision += 1
        action.result_ref = f"{action.execution_id}:action:{action.action_id}" if succeeded else None
        job.status, job.lease_expires_at = action.status, None
        self.service.audit(session, action, "action_" + action.status, self.owner, metadata={"recovery": recovery})
        self.service.notice(session, action, action.status,
                            f"操作 {action.action_id}：" + ("已执行成功。" if succeeded else "执行结果待核对，不会自动重试。"))

    def finish(self, key):
        with self.database.sessions.begin() as session:
            session.get(TenantRow, key[0], with_for_update=True)
            action = session.get(PendingActionRow, key, with_for_update=True)
            job = session.get(ActionJobRow, key, with_for_update=True)
            if job.status != "executing" or job.owner_id != self.owner:
                return
            self.finish_in_session(session, action, job)

    async def run_once(self):
        await database_call(self.service.expire)
        key = await database_call(self.choose)
        if key is None:
            return False
        chosen = await database_call(self.claim, key)
        if chosen is None:
            return True
        action, route, spec = chosen
        try:
            lease = await self.coordinator.acquire(key[0], route.agent_app_id, route.internal_user_id, route.session_id,
                                                   self.owner + ":" + uuid.uuid4().hex, 2 * self.timeout + 60)
        except SessionBusyError:
            return False
        started = False
        try:
            payload = self.service.payload(action)
            if not await asyncio.wait_for(spec.check(payload["arguments"], payload["preconditions"]), self.timeout):
                await database_call(self.cancel_precondition, key)
                return True
            # Revalidate the lease after the potentially slow resource check. The
            # subsequent bounded execution fits within this renewed lease. A stale
            # owner must fail before dispatch; the external write still needs CAS.
            await self.coordinator.renew(lease, self.timeout + 60)

            def begin():
                nonlocal started
                self.begin(key)
                started = True

            await database_call(begin)
            ctx = AgentContext()
            for name, value in {
                    "tenant_id": key[0],
                    "agent_app_id": route.agent_app_id,
                    "execution_id": action.execution_id,
                    "action_id": action.action_id
            }.items():
                ctx.with_metadata(name, value)

            async def confirmed(context, args):
                return context is ctx and args == payload["arguments"]

            async def authorize(_):
                await database_call(self.authorize_dispatch, key)

            with self.database.sessions() as session:
                _, app = self.service.current_policy(session, self.service.parent(session, key[0], action.execution_id))
            guard = ToolGovernanceFilter(key[0],
                                         route.agent_app_id,
                                         action.tool_name,
                                         app.tool_policy,
                                         confirmation_check=confirmed,
                                         authorize=authorize)

            async def execute():
                return await DurableTools(self.database).invoke(
                    key[0],
                    action.execution_id,
                    "action:" + action.action_id,
                    action.tool_name,
                    payload["arguments"],
                    lambda _: spec.execute(payload["arguments"], payload["preconditions"], action.action_id),
                    confirmed_action_id=action.action_id)

            with operation("action.execute",
                           parent=payload.get("trace_context"),
                           attributes={
                               "tenant.id": key[0],
                               "action.id": key[1]
                           }):
                await asyncio.wait_for(run_filters(ctx, payload["arguments"], [guard], execute), self.timeout)
        except (PermissionError, ActionConflict):
            if not started:
                await database_call(self.cancel_precondition, key)
        except Exception:
            # Uncertain external results never return to pending.
            if not started:
                await database_call(self.cancel_precondition, key)
        finally:
            if started:
                await database_call(self.finish, key)
            await self.coordinator.release(lease)
        return True
