"""Starlette-facing adapter for the signed local HTTP contract."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse

from trpc_service.audit.models import AuditDecision, AuditRecord, PreAuthScope
from trpc_service.channels.contracts import InboundMessage, OutboundReply
from trpc_service.metrics.contracts import MetricsUnavailable
from trpc_service.log import log_delivery
from trpc_service.channels.hmac_auth import verify_request
from trpc_service.storage.contracts import AccessDenied, AuditUnavailable, PlatformPortError, Unauthorized
from trpc_service.web.errors import map_platform_error


def reply_envelope(reply: OutboundReply) -> dict[str, object]:
    data = None
    if reply.tenant_id is not None:
        data = {
            "tenant_id": reply.tenant_id,
            "platform_session_id": reply.platform_session_id,
            "external_message_id": reply.external_message_id,
            "text": reply.text,
            "delivery_action": reply.delivery_action.value,
        }
        if reply.status.value == "processing":
            data["retryable"] = True
        elif reply.error is not None:
            data["retryable"] = reply.error.retryable
    return {
        "status": reply.status.value,
        "trace_id": str(reply.trace_id),
        "original_trace_id": str(reply.original_trace_id) if reply.original_trace_id else None,
        "data": data,
        "error": reply.error.model_dump(mode="json") if reply.error else None,
    }


class LocalHttpChannelAdapter:
    def __init__(self, runtime: object) -> None:
        self.runtime = runtime

    async def _observe_rejection(
        self,
        *,
        decision: AuditDecision,
        trace_id: UUID,
        binding_id: str,
        external_message_id: str | None,
    ) -> None:
        scope = PreAuthScope()
        record = AuditRecord(
            audit_id=uuid4(), trace_id=trace_id, tenant_id=None,
            channel="local_http",
            binding_id_digest="sha256:" + sha256(binding_id.encode("utf-8")).hexdigest(),
            user_id=None, session_id=None, decision=decision, latency_ms=0,
            error_type=decision.value,
            cost=Decimal("0"),
            external_message_digest=(
                "sha256:" + sha256(external_message_id.encode("utf-8")).hexdigest()
                if external_message_id else None
            ),
            created_at=self.runtime.now(),
        )
        try:
            await self.runtime.adapters.audit.append(scope, record)
        except AuditUnavailable:
            pass
        try:
            self.runtime.metrics.record(
                scope, trace_id=trace_id, stage="request", outcome="error", duration_ms=0,
            )
        except MetricsUnavailable:
            pass
        log_delivery(
            trace_id=trace_id,
            outcome=decision.value,
            status_code={AuditDecision.INVALID_REQUEST: 400, AuditDecision.UNAUTHORIZED: 401, AuditDecision.ACCESS_DENIED: 403}[decision],
        )

    async def handle(self, request: Request) -> JSONResponse:
        raw = await request.body()
        binding_id = request.headers.get("x-channel-binding", "")
        external_message_id: str | None = None
        trace_header = request.headers.get("x-trace-id")
        try:
            trace_id = UUID(trace_header) if trace_header else uuid4()
        except ValueError:
            trace_id = uuid4()
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError
            if {"binding_id", "received_at", "trace_id"}.intersection(payload):
                raise ValueError
            external_message_id = payload.get("external_message_id")
            if not isinstance(external_message_id, str):
                raise ValueError
        except (ValueError, json.JSONDecodeError):
            await self._observe_rejection(
                decision=AuditDecision.INVALID_REQUEST, trace_id=trace_id,
                binding_id=binding_id, external_message_id=external_message_id,
            )
            return self._error(400, "invalid_request", "Request validation failed.", trace_id)
        try:
            verified = await verify_request(
                binding_id=binding_id,
                timestamp=request.headers.get("x-request-timestamp", ""),
                signature=request.headers.get("x-signature", ""),
                external_message_id=external_message_id,
                raw_body=raw,
                registry=self.runtime.adapters,
                resolver=self.runtime.secrets,
                now=self.runtime.now(),
            )
            message = InboundMessage(
                **payload,
                binding_id=binding_id,
                received_at=self.runtime.now(),
                trace_id=trace_id,
            )
            reply = await self.runtime.gateway.handle_verified_message(verified, message)
            status_code = {
                "succeeded": 200,
                "duplicate": 200,
                "processing": 202,
                "conflict": 409,
                "invalid_request": 400,
                "unauthorized": 401,
                "access_denied": 403,
            }.get(reply.status.value, 502 if reply.error and reply.error.code == "agent_failed" else 503)
            log_delivery(trace_id=reply.trace_id, outcome=reply.status.value, status_code=status_code)
            return JSONResponse(reply_envelope(reply), status_code=status_code)
        except Unauthorized:
            await self._observe_rejection(
                decision=AuditDecision.UNAUTHORIZED, trace_id=trace_id,
                binding_id=binding_id, external_message_id=external_message_id,
            )
            return self._error(401, "unauthorized", "Request authentication failed.", trace_id)
        except AccessDenied:
            await self._observe_rejection(
                decision=AuditDecision.ACCESS_DENIED, trace_id=trace_id,
                binding_id=binding_id, external_message_id=external_message_id,
            )
            return self._error(403, "access_denied", "Request access denied.", trace_id)
        except ValidationError:
            await self._observe_rejection(
                decision=AuditDecision.INVALID_REQUEST, trace_id=trace_id,
                binding_id=binding_id, external_message_id=external_message_id,
            )
            return self._error(400, "invalid_request", "Request validation failed.", trace_id)
        except PlatformPortError as exc:
            mapped = map_platform_error(exc)
            return self._error(mapped.status_code, mapped.code, mapped.message, trace_id)

    @staticmethod
    def _error(status: int, code: str, message: str, trace_id: UUID) -> JSONResponse:
        return JSONResponse(
            {
                "status": code if code != "agent_failed" else "failed",
                "trace_id": str(trace_id),
                "original_trace_id": None,
                "data": None,
                "error": {"code": code, "message": message, "retryable": False, "execution_started": False},
            },
            status_code=status,
        )
