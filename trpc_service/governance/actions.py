"""Durable two-stage actions. Only authenticated principals can change decisions."""

from datetime import timedelta
import hashlib
import hmac
import re
import secrets
import uuid

from sqlalchemy import select

from trpc_service.channels.authorization import require_im_execution_in_session
from trpc_service.channels.persistence import as_utc, database_now
from trpc_service.persistence.models import (PendingActionRow, ActionJobRow, ActionNoticeRow, TenantRow,
                                             InboundMessageRow, IMEventReceiptRow, IMMembershipRow,
                                             TenantConfigVersionRow, AuditLogRow)
from trpc_service.tenant import TenantConfig, ResolvedRoute
from .budget import digest

TOKEN = re.compile(r"([arc]):([a-f0-9]{32}):([A-Za-z0-9_-]{22}):([1-9][0-9]{0,5})")
TERMINAL = {"succeeded", "failed", "rejected", "cancelled", "expired", "unknown_outcome"}


class ActionConflict(ValueError):
    pass


def conversation_hash(channel, chat, topic=None):
    return digest([channel, str(chat), str(topic) if topic is not None else None])


def policy_hash(policy):
    """Hash set-valued tool policy fields deterministically across processes."""
    value = policy.model_dump(mode="json")
    for field in ("allow", "deny", "require_confirmation"):
        if field in value:
            value[field] = sorted(value[field])
    return digest(value)


class ActionService:

    def __init__(self, database, cipher):
        self.database, self.cipher = database, cipher

    @staticmethod
    def lock_tenant(session, tenant):
        row = session.get(TenantRow, tenant, with_for_update=True)
        if row is None or row.status != "active":
            raise PermissionError("action tenant is unavailable")
        return row

    @staticmethod
    def parent(session, tenant, execution):
        row = session.scalar(
            select(InboundMessageRow).where(InboundMessageRow.tenant_id == tenant,
                                            InboundMessageRow.execution_id == execution).with_for_update())
        if row is None or "authz_epoch" not in row.payload_json:
            raise PermissionError("action requires a trusted IM execution")
        return row

    @staticmethod
    def current_policy(session, parent):
        route = ResolvedRoute.model_validate(parent.payload_json["route"])
        require_im_execution_in_session(session, route, parent.payload_json["authz_epoch"])
        tenant = session.get(TenantRow, route.tenant_id)
        if tenant.active_config_version != route.config_version:
            raise PermissionError("action configuration has changed")
        snapshot = session.get(TenantConfigVersionRow, (route.tenant_id, route.config_version))
        if snapshot is None or snapshot.status != "published":
            raise PermissionError("action requires an immutable published policy")
        config = TenantConfig.model_validate(snapshot.config_json)
        app = next(app for app in config.applications if app.app_id == route.agent_app_id)
        if not app.enabled:
            raise PermissionError("action application is disabled")
        return route, app

    @staticmethod
    def audit(session, action, decision, actor, *, metadata=None):
        parent = ActionService.parent(session, action.tenant_id, action.execution_id)
        session.add(
            AuditLogRow(audit_id=uuid.uuid4().hex,
                        tenant_id=action.tenant_id,
                        channel=parent.payload_json["message"]["channel"],
                        user_id=actor,
                        session_id=action.session_id,
                        tool_name=action.tool_name,
                        decision=decision,
                        trace_id=parent.trace_id,
                        request_id=parent.request_id,
                        config_version=action.config_version,
                        redacted=True,
                        metadata_json={
                            "action_id": action.action_id,
                            "revision": action.revision,
                            "execution_id": action.execution_id,
                            **(metadata or {})
                        }))

    def payload(self, action):
        payload = self.cipher.open((action.tenant_id, "action", action.action_id), action.encrypted_payload)
        if digest([payload["arguments"], payload["preconditions"]]) != action.arguments_hash:
            raise ActionConflict("action arguments no longer match their approval")
        return payload

    def notice(self, session, action, kind, text, reply=None):
        if session.scalar(
                select(ActionNoticeRow.notice_id).where(ActionNoticeRow.tenant_id == action.tenant_id,
                                                        ActionNoticeRow.action_id == action.action_id,
                                                        ActionNoticeRow.kind == kind)):
            return
        payload = self.payload(action)
        identity = uuid.uuid4().hex
        session.add(
            ActionNoticeRow(notice_id=identity,
                            tenant_id=action.tenant_id,
                            action_id=action.action_id,
                            binding_id=action.binding_id,
                            kind=kind,
                            status="pending",
                            encrypted_payload=self.cipher.seal(
                                (action.tenant_id, "notice", identity), {
                                    "reply": reply or payload.get("completion_reply", payload["reply"]),
                                    "text": text,
                                    "trace_context": payload["trace_context"]
                                })))

    def propose(self,
                tenant,
                execution,
                call_id,
                name,
                tool_revision,
                arguments,
                preconditions,
                safe_summary,
                *,
                ttl_seconds=300):
        """Arguments/preconditions/summary must come from a registered tool validator.

        Initial approval rule is the originating actor with an explicit approve grant.
        Delegated and four-eyes policies are intentionally not inferred from membership.
        """
        if (not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", tool_revision) or not 30 <= ttl_seconds <= 3600
                or not isinstance(arguments, dict) or not isinstance(preconditions, dict)
                or not isinstance(safe_summary, dict) or not call_id or len(call_id) > 128):
            raise ValueError("invalid action definition")
        if set(safe_summary) - {"operation", "target", "impact", "amount", "currency"} or any(
                not isinstance(value, str) or len(value) > 200 for value in safe_summary.values()):
            raise ValueError("summary must use the registered public fields")
        signature = digest([arguments, preconditions])
        with self.database.sessions.begin() as session:
            self.lock_tenant(session, tenant)
            parent = self.parent(session, tenant, execution)
            route, app = self.current_policy(session, parent)
            if (name not in app.tool_policy.require_confirmation or name not in app.tool_policy.allow
                    or name in app.tool_policy.deny):
                raise PermissionError("tool is not an approved confirmation entrypoint")
            old = session.scalar(
                select(PendingActionRow).where(PendingActionRow.tenant_id == tenant,
                                               PendingActionRow.execution_id == execution))
            if old:
                if (old.tool_call_id, old.tool_name, old.tool_revision, old.arguments_hash,
                        old.safe_summary) != (call_id, name, tool_revision, signature, safe_summary):
                    raise ActionConflict("execution already has a different dangerous action")
                return self.view(old)
            receipt = session.scalar(
                select(IMEventReceiptRow).where(IMEventReceiptRow.tenant_id == tenant,
                                                IMEventReceiptRow.inbound_id == parent.inbound_message_id))
            if receipt is None or not receipt.context_ciphertext:
                raise PermissionError("action has no authenticated reply context")
            context = self.cipher.open((tenant, receipt.binding_id, receipt.external_event_id),
                                       receipt.context_ciphertext)["reply"]
            action_id, nonce = uuid.uuid4().hex, secrets.token_urlsafe(16)
            action = PendingActionRow(tenant_id=tenant,
                                      action_id=action_id,
                                      execution_id=execution,
                                      tool_call_id=call_id,
                                      binding_id=route.channel_binding_id,
                                      session_id=route.session_id,
                                      requester_actor_id=route.actor_id,
                                      requester_epoch=parent.payload_json["authz_epoch"],
                                      approvers_json=[route.actor_id],
                                      tool_name=name,
                                      tool_revision=tool_revision,
                                      config_version=route.config_version,
                                      policy_hash=policy_hash(app.tool_policy),
                                      arguments_hash=signature,
                                      safe_summary=safe_summary,
                                      status="pending",
                                      revision=1,
                                      conversation_hash=conversation_hash(parent.payload_json["message"]["channel"],
                                                                          context["chat_id"], context.get("thread_id")),
                                      nonce_hash=hashlib.sha256(nonce.encode()).hexdigest(),
                                      expires_at=database_now(session) + timedelta(seconds=ttl_seconds),
                                      encrypted_payload=self.cipher.seal(
                                          (tenant, "action", action_id), {
                                              "arguments": arguments,
                                              "preconditions": preconditions,
                                              "reply": context,
                                              "trace_context": parent.payload_json.get("trace_context")
                                          }))
            session.add(action)
            session.flush()
            summary = "\n".join(f"{key}：{value}" for key, value in safe_summary.items())
            text = (f"待确认操作：{name}\n{summary}\n有效期 {ttl_seconds} 秒。仅授权发起人可以确认。\n"
                    f"批准：/action a:{action_id}:{nonce}:1\n拒绝：/action r:{action_id}:{nonce}:1\n"
                    f"取消：/action c:{action_id}:{nonce}:1")
            self.notice(session, action, "confirmation", text)
            self.audit(session, action, "action_created", route.actor_id)
            return self.view(action)

    def validate_current(self, session, action):
        parent = self.parent(session, action.tenant_id, action.execution_id)
        route, app = self.current_policy(session, parent)
        if (action.requester_epoch != parent.payload_json["authz_epoch"]
                or policy_hash(app.tool_policy) != action.policy_hash
                or action.tool_name not in app.tool_policy.require_confirmation):
            raise PermissionError("action policy is no longer valid")
        self.payload(action)
        return route

    def decide(self,
               tenant,
               action_id,
               principal,
               decision,
               nonce,
               expected_revision,
               *,
               binding_id=None,
               conversation=None,
               reply=None):
        with self.database.sessions.begin() as session:
            self.lock_tenant(session, tenant)
            return self.decide_in_session(session,
                                          tenant,
                                          action_id,
                                          principal,
                                          decision,
                                          nonce,
                                          expected_revision,
                                          binding_id=binding_id,
                                          conversation=conversation,
                                          reply=reply)

    def decide_in_session(self,
                          session,
                          tenant,
                          action_id,
                          principal,
                          decision,
                          nonce,
                          expected_revision,
                          *,
                          binding_id=None,
                          conversation=None,
                          reply=None):
        action = session.get(PendingActionRow, (tenant, action_id), with_for_update=True)
        if action is None:
            raise LookupError("action not found")
        if decision not in {"approve", "reject", "cancel"}:
            raise ValueError("unsupported decision")
        if (binding_id is not None and binding_id != action.binding_id
                or conversation is not None and conversation != action.conversation_hash):
            raise PermissionError("confirmation belongs to a different conversation")
        if not hmac.compare_digest(action.nonce_hash, hashlib.sha256(nonce.encode()).hexdigest()):
            raise PermissionError("confirmation nonce is invalid")
        member = session.get(IMMembershipRow, (tenant, action.binding_id, principal))
        if member is None or not member.active or "chat" not in member.grants_json:
            raise PermissionError("active actor membership required")
        if decision == "cancel":
            if principal != action.requester_actor_id:
                raise PermissionError("only the originating actor can cancel")
        elif principal not in action.approvers_json or "approve" not in member.grants_json:
            raise PermissionError("specific action approval grant required")
        target = {"approve": "approved", "reject": "rejected", "cancel": "cancelled"}[decision]
        if action.status != "pending":
            if expected_revision == 1 and action.revision >= 2 and (
                    action.status == target or decision == "approve" and action.approver_actor_id == principal
                    and action.status in {"executing", "succeeded", "failed", "unknown_outcome"}):
                return self.view(action)
            raise ActionConflict("action is no longer pending")
        if action.revision != expected_revision:
            raise ActionConflict("action revision changed")
        if as_utc(action.expires_at) <= database_now(session):
            action.status, action.revision = "expired", action.revision + 1
            self.audit(session, action, "action_expired", principal)
            self.notice(session, action, "expired", f"操作 {action.action_id}：确认已过期，未执行。")
            return self.view(action)
        self.validate_current(session, action)
        action.status, action.revision = target, action.revision + 1
        if decision == "approve":
            action.approver_actor_id, action.approver_epoch = principal, member.authz_epoch
            action.approved_at = database_now(session)
            if reply is not None:
                payload = self.payload(action)
                payload["completion_reply"] = reply
                action.encrypted_payload = self.cipher.seal((tenant, "action", action_id), payload)
            session.add(ActionJobRow(tenant_id=tenant, action_id=action_id, status="pending"))
        self.audit(session, action, "action_" + target, principal)
        parent = self.parent(session, tenant, action.execution_id)
        # WeCom's approval callback is reserved for the final action result. Do
        # not finish that response stream early with a queued acknowledgement.
        if decision != "approve" or parent.payload_json["message"]["channel"] != "wecom":
            self.notice(
                session, action, target, f"操作 {action_id}：" + {
                    "approve": "批准已保存，等待执行。",
                    "reject": "已拒绝，未执行。",
                    "cancel": "已取消，未执行。"
                }[decision], reply)
        return self.view(action)

    def decide_event(self, session, event, binding, principal):
        parsed = TOKEN.fullmatch(event.action_data or "")
        if parsed is None:
            raise ValueError("invalid action command")
        verb, action_id, nonce, revision = parsed.groups()
        return self.decide_in_session(session,
                                      binding.tenant_id,
                                      action_id,
                                      principal, {
                                          "a": "approve",
                                          "r": "reject",
                                          "c": "cancel"
                                      }[verb],
                                      nonce,
                                      int(revision),
                                      binding_id=binding.binding_id,
                                      # Feishu prefixes external_chat_id with tenant_key for
                                      # routing, while the authenticated reply context stores
                                      # the native chat_id used when the action was proposed.
                                      # Bind approval to that context to keep both transports
                                      # consistent without weakening tenant isolation.
                                      conversation=conversation_hash(
                                          binding.channel.value,
                                          event.reply_context.get("chat_id", event.external_chat_id),
                                          event.reply_context.get("thread_id")),
                                      reply=event.reply_context)

    def expire(self):
        """Expiry cannot turn executing/unknown work back into an unexecuted action."""
        with self.database.sessions() as session:
            tenants = list(
                session.scalars(
                    select(PendingActionRow.tenant_id).where(PendingActionRow.status == "pending",
                                                             PendingActionRow.expires_at
                                                             <= database_now(session)).distinct()))
        for tenant in tenants:
            with self.database.sessions.begin() as session:
                session.get(TenantRow, tenant, with_for_update=True)
                for action in session.scalars(
                        select(PendingActionRow).where(PendingActionRow.tenant_id == tenant,
                                                       PendingActionRow.status == "pending", PendingActionRow.expires_at
                                                       <= database_now(session))):
                    action.status, action.revision = "expired", action.revision + 1
                    self.audit(session, action, "action_expired", "action_worker")
                    self.notice(session, action, "expired", f"操作 {action.action_id}：确认已过期，未执行。")

    @staticmethod
    def view(action):
        return {
            "action_id": action.action_id,
            "status": action.status,
            "revision": action.revision,
            "tool_name": action.tool_name,
            "tool_revision": action.tool_revision,
            "safe_summary": action.safe_summary,
            "expires_at": as_utc(action.expires_at),
            "requester_actor_id": action.requester_actor_id,
            "approver_actor_id": action.approver_actor_id,
            "session_id": action.session_id,
            "result_ref": action.result_ref
        }
