"""Shared tenant governance decisions used before Runner and inside tools."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from tenant_agent.models import Attachment, InboundEnvelope, TenantConfig
from tenant_agent.security import Redactor
from tenant_agent.storage.base import ReceiptRepository, TenantDataPlane


class PolicyDenied(RuntimeError):
    def __init__(self, decision: str, public_message: str):
        super().__init__(decision)
        self.decision = decision
        self.public_message = public_message


class BudgetExceeded(PolicyDenied):
    pass


def estimate_input_tokens(text: str, *, attachment_count: int = 0) -> int:
    """Conservative tokenizer-free admission estimate for mixed scripts."""

    ascii_characters = sum(ord(character) < 128 for character in text)
    non_ascii_characters = len(text) - ascii_characters
    text_tokens = math.ceil(ascii_characters / 4) + non_ascii_characters
    return max(1, text_tokens + attachment_count * 256)


class GovernanceService:
    def __init__(self, redactor: Redactor) -> None:
        self.redactor = redactor

    def effective_input(
        self,
        tenant: TenantConfig,
        text: str,
        attachments: object,
    ) -> str:
        input_parts = [text] if text else []
        values = attachments if isinstance(attachments, (list, tuple)) else ()
        for raw_attachment in values:
            attachment = (
                raw_attachment
                if isinstance(raw_attachment, Attachment)
                else Attachment.model_validate(raw_attachment)
            )
            safe_metadata: dict[str, Any] = {"kind": attachment.kind}
            if attachment.filename:
                safe_metadata["filename"] = attachment.filename[:256]
            if attachment.mime_type:
                safe_metadata["content_type"] = attachment.mime_type[:128]
            if attachment.size_bytes is not None:
                safe_metadata["size_bytes"] = attachment.size_bytes
            input_parts.append(
                "[Attachment metadata: " + json.dumps(safe_metadata, ensure_ascii=False, sort_keys=True) + "]"
            )
        effective_input = "\n".join(input_parts)
        if tenant.governance.redaction.redact_before_model:
            tenant_redactor = Redactor(tenant.governance.redaction, self.redactor.registry)
            return tenant_redactor.text(effective_input)
        return effective_input

    async def authorize_input(
        self,
        tenant: TenantConfig,
        envelope: InboundEnvelope,
        plane: TenantDataPlane,
        *,
        usage_period: str,
        reservation_id: str,
        reservation_expires_at: datetime,
    ) -> str:
        if tenant.status.value != "active":
            raise PolicyDenied("tenant_suspended", "This assistant is temporarily unavailable.")
        app = tenant.apps.get(envelope.app_id)
        if app is None or not app.enabled:
            raise PolicyDenied("app_disabled", "This assistant is unavailable.")
        users = tenant.governance.users
        user = envelope.external_user_id
        if user in users.deny_users:
            raise PolicyDenied("user_denied", "You are not authorized to use this assistant.")
        if users.allow_users and user not in users.allow_users:
            raise PolicyDenied("user_not_allowed", "You are not authorized to use this assistant.")
        if envelope.chat_type.value != "direct" and users.allow_groups:
            if envelope.external_chat_id not in users.allow_groups:
                raise PolicyDenied(
                    "group_not_allowed",
                    "This group or channel is not authorized for this assistant.",
                )

        effective_input = self.effective_input(tenant, envelope.text, envelope.attachments)
        estimate = estimate_input_tokens(
            effective_input,
            attachment_count=len(envelope.attachments),
        )
        policy = tenant.governance.budget
        if estimate > policy.max_tokens_per_request:
            raise BudgetExceeded("request_token_limit", "This message is too large for the tenant policy.")
        profile = tenant.models[app.model_profile]
        llm_calls = 1 if profile.provider == "deterministic" else policy.max_llm_calls_per_request
        provider_attempts = 1 if profile.provider == "deterministic" else profile.retry_count + 1
        reserved_input_tokens = llm_calls * provider_attempts * profile.context_window_tokens
        reserved_output_tokens = llm_calls * provider_attempts * profile.max_output_tokens
        reserved_tokens = reserved_input_tokens + reserved_output_tokens
        reserved_cost = (
            reserved_input_tokens * profile.input_cost_per_million
            + reserved_output_tokens * profile.output_cost_per_million
        ) / 1_000_000
        reservation = await plane.usage.reserve_usage(
            tenant_id=tenant.tenant_id,
            reservation_id=reservation_id,
            period=usage_period,
            reserved_tokens=reserved_tokens,
            reserved_cost_usd=reserved_cost,
            token_limit=policy.monthly_tokens,
            cost_limit_usd=policy.monthly_cost_usd,
            expires_at=reservation_expires_at,
        )
        if not reservation.acquired:
            decision = reservation.reason or "monthly_token_budget"
            public_message = (
                "The tenant cost budget has been reached."
                if decision == "monthly_cost_budget"
                else "The tenant token budget has been reached."
            )
            raise BudgetExceeded(decision, public_message)

        return effective_input

    @staticmethod
    def authorize_tool(tenant: TenantConfig, app_id: str, tool_name: str) -> str:
        app = tenant.apps[app_id]
        policy = tenant.governance.tools
        if tool_name in policy.deny:
            return "deny"
        if tool_name not in policy.allow or tool_name not in app.allowed_tools:
            return "deny"
        if tool_name in policy.dangerous:
            return "confirm"
        return "allow"

    def redact_output(self, tenant: TenantConfig, text: str) -> str:
        return Redactor(tenant.governance.redaction, self.redactor.registry).text(text)


class ConfirmationManager:
    """Signed, scoped, expiring, one-use dangerous-tool confirmation tokens."""

    def __init__(self, key: bytes, receipts: ReceiptRepository) -> None:
        if len(key) < 16:
            raise ValueError("confirmation HMAC key must be at least 16 bytes")
        self.key = key
        self.receipts = receipts

    @staticmethod
    def _args_hash(args: dict[str, Any]) -> str:
        canonical = json.dumps(args, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def issue(
        self,
        *,
        tenant_id: str,
        user_id: str,
        session_id: str,
        tool_name: str,
        args: dict[str, Any],
        ttl_seconds: int,
    ) -> str:
        payload = {
            "v": 1,
            "jti": uuid.uuid4().hex,
            "tenant": tenant_id,
            "user": user_id,
            "session": session_id,
            "tool": tool_name,
            "args": self._args_hash(args),
            "exp": int(time.time()) + ttl_seconds,
        }
        encoded = (
            base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
            .decode()
            .rstrip("=")
        )
        signature = (
            base64.urlsafe_b64encode(hmac.new(self.key, encoded.encode(), hashlib.sha256).digest())
            .decode()
            .rstrip("=")
        )
        return f"{encoded}.{signature}"

    def inspect(self, token: str) -> dict[str, Any] | None:
        encoded, separator, signature = token.partition(".")
        if not separator:
            return None
        expected = (
            base64.urlsafe_b64encode(hmac.new(self.key, encoded.encode(), hashlib.sha256).digest())
            .decode()
            .rstrip("=")
        )
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            padding = "=" * (-len(encoded) % 4)
            payload = json.loads(base64.urlsafe_b64decode(encoded + padding))
        except (ValueError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("v") != 1 or int(payload.get("exp", 0)) < int(time.time()):
            return None
        return {str(key): value for key, value in payload.items()}

    async def consume(
        self,
        token: str,
        *,
        tenant_id: str,
        user_id: str,
        session_id: str,
        tool_name: str,
        args: dict[str, Any],
    ) -> bool:
        payload = self.inspect(token)
        expected = {
            "tenant": tenant_id,
            "user": user_id,
            "session": session_id,
            "tool": tool_name,
            "args": self._args_hash(args),
        }
        if payload is None or any(payload.get(key) != value for key, value in expected.items()):
            return False
        dedupe_key = f"c_{hashlib.sha256(token.encode()).hexdigest()[:62]}"
        owner = f"confirmation:{uuid.uuid4().hex}"
        claim = await self.receipts.claim_receipt(
            tenant_id=tenant_id,
            dedupe_key=dedupe_key,
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=30),
        )
        if not claim.acquired:
            return False
        await self.receipts.complete_receipt(
            tenant_id=tenant_id,
            dedupe_key=dedupe_key,
            owner=owner,
            response=(),
        )
        return True


def confirmation_tokens(text: str) -> tuple[str, ...]:
    """Extract explicit `/confirm <signed-token>` commands only."""

    stripped = text.strip()
    if not stripped.startswith("/confirm "):
        return ()
    token = stripped.removeprefix("/confirm ").strip().split(maxsplit=1)[0]
    return (token,) if token else ()
