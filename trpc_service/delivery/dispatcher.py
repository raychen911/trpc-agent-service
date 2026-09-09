"""Fenced Outbox dispatcher for Telegram and enterprise WeChat.

The most important invariant here is not "retry every failure".  Once request
bytes may have reached a non-idempotent channel endpoint, an inconclusive result
is persisted as ``UNKNOWN`` and requires reconciliation instead of a blind retry.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import ValidationError

from trpc_service.delivery.contracts import (
    BindingPort,
    DeliveryBinding,
    DispatchReport,
    DispatchState,
    OutboxPort,
    TelegramDeliveryRoute,
    TextDeliveryPayload,
)
from trpc_service.metrics import METRICS
from trpc_service.reliability.types import (
    OutboxDeliveryClaim,
    OutboxDeliveryOutcome,
)
from trpc_service.security.secrets import (
    EnvelopeCipher,
    InvalidCiphertextError,
    SecretResolutionError,
    SecretResolver,
)

_TELEGRAM_CREDENTIAL = "telegram_delivery_context"
_WECOM_CREDENTIAL = "wecom_response_url"
_TELEGRAM_TEXT_LIMIT = 4_096
_TELEGRAM_TOKEN = re.compile(r"\A[1-9][0-9]{4,15}:[A-Za-z0-9_-]{30,80}\Z")
_MAX_ROUTE_BYTES = 8_192
_DEFAULT_WECOM_HOSTS = frozenset({"qyapi.weixin.qq.com"})


@dataclass(frozen=True, slots=True)
class _Decision:
    outcome: OutboxDeliveryOutcome
    error_type: str | None = None
    external_message_id: str | None = None
    retry_after: timedelta | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def _sleep(delay: float) -> None:
    await asyncio.sleep(delay)


class OutboxDispatcher:
    """Deliver at most one durable Outbox part per invocation."""

    def __init__(
        self,
        repository: OutboxPort,
        bindings: BindingPort,
        secrets: SecretResolver,
        cipher: EnvelopeCipher,
        client: httpx.AsyncClient,
        *,
        dispatcher_id: str,
        clock: Callable[[], datetime] = _utc_now,
        sleeper: Callable[[float], Awaitable[None]] = _sleep,
        telegram_transport_attempts: int = 3,
        base_retry_delay: timedelta = timedelta(seconds=5),
        request_timeout: httpx.Timeout | None = None,
        wecom_allowed_hosts: frozenset[str] = _DEFAULT_WECOM_HOSTS,
    ) -> None:
        if not dispatcher_id or len(dispatcher_id) > 128:
            raise ValueError("dispatcher_id must contain 1..128 characters")
        if telegram_transport_attempts < 1 or telegram_transport_attempts > 5:
            raise ValueError("telegram_transport_attempts must be in 1..5")
        if base_retry_delay <= timedelta(0):
            raise ValueError("base_retry_delay must be positive")
        if not wecom_allowed_hosts:
            raise ValueError("wecom_allowed_hosts must not be empty")
        self._repository = repository
        self._bindings = bindings
        self._secrets = secrets
        self._cipher = cipher
        self._client = client
        self._dispatcher_id = dispatcher_id
        self._clock = clock
        self._sleeper = sleeper
        self._telegram_transport_attempts = telegram_transport_attempts
        self._base_retry_delay = base_retry_delay
        self._request_timeout = request_timeout or httpx.Timeout(
            connect=3.0,
            read=8.0,
            write=8.0,
            pool=3.0,
        )
        self._wecom_allowed_hosts = frozenset(host.casefold() for host in wecom_allowed_hosts)

        # httpx's INFO access line includes the complete Telegram bot-token URL.
        # Raising these library loggers is a process-level secret-leak safeguard;
        # this dispatcher emits only its own safe status metrics/results.
        for logger_name in ("httpx", "httpcore"):
            client_logger = logging.getLogger(logger_name)
            if client_logger.getEffectiveLevel() < logging.WARNING:
                client_logger.setLevel(logging.WARNING)

    async def dispatch_once(self, tenant_id: str) -> DispatchReport:
        """Claim, deliver, and durably classify one ordered Outbox part."""

        claim = await self._repository.claim_outbox(tenant_id, self._dispatcher_id)
        if claim is None:
            return DispatchReport(state=DispatchState.NO_WORK)

        decision = await self._prepare_and_deliver(claim)
        channel = _metric_channel(claim)
        if decision.error_type == "delivery_lease_lost":
            METRICS.delivery_total.labels(
                claim.tenant_id,
                channel,
                decision.outcome.value,
            ).inc()
            return DispatchReport(
                state=DispatchState.LEASE_LOST,
                outbox_id=claim.outbox_id,
                outcome=decision.outcome,
                error_type=decision.error_type,
                persisted=False,
            )
        next_retry_at = None
        if decision.retry_after is not None:
            next_retry_at = self._now() + decision.retry_after
        persisted = await self._repository.record_delivery(
            claim,
            decision.outcome,
            external_message_id=decision.external_message_id,
            error_type=decision.error_type,
            next_retry_at=next_retry_at,
        )
        METRICS.delivery_total.labels(
            claim.tenant_id,
            channel,
            decision.outcome.value,
        ).inc()
        return DispatchReport(
            state=DispatchState.RECORDED if persisted else DispatchState.LEASE_LOST,
            outbox_id=claim.outbox_id,
            outcome=decision.outcome,
            error_type=decision.error_type,
            persisted=persisted,
        )

    async def _prepare_and_deliver(self, claim: OutboxDeliveryClaim) -> _Decision:
        try:
            binding = await self._bindings.load(claim.tenant_id, claim.binding_id)
        except Exception:  # Safe boundary: third-party stores must not leak their error text.
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "delivery_binding_unavailable",
            )
        if (
            binding.tenant_id != claim.tenant_id
            or binding.binding_id != claim.binding_id
            or binding.status != "active"
        ):
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "delivery_binding_mismatch",
            )

        try:
            payload = TextDeliveryPayload.model_validate(claim.payload, strict=True)
        except ValidationError:
            return _Decision(OutboxDeliveryOutcome.DEAD_LETTER, "outbox_payload_invalid")

        credential = claim.reply_credential
        if credential is None:
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "reply_credential_missing",
            )
        expected_kind = {
            "telegram": _TELEGRAM_CREDENTIAL,
            "wecom": _WECOM_CREDENTIAL,
        }.get(binding.channel_type)
        if expected_kind is None or credential.credential_kind != expected_kind:
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "reply_credential_kind_mismatch",
            )
        try:
            plaintext = self._cipher.decrypt(
                credential.ciphertext,
                context={
                    "tenant_id": claim.tenant_id,
                    "binding_id": claim.binding_id,
                    "delivery_id": claim.delivery_id,
                    "credential_kind": credential.credential_kind,
                },
            ).get_secret_value()
        except (InvalidCiphertextError, ValueError):
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "reply_credential_invalid",
            )
        if len(plaintext.encode("utf-8")) > _MAX_ROUTE_BYTES:
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "reply_credential_invalid",
            )

        if binding.channel_type == "telegram":
            return await self._deliver_telegram(claim, binding, payload, plaintext)
        return await self._deliver_wecom(claim, payload, plaintext)

    async def _deliver_telegram(
        self,
        claim: OutboxDeliveryClaim,
        binding: DeliveryBinding,
        payload: TextDeliveryPayload,
        plaintext_route: str,
    ) -> _Decision:
        if len(payload.text) > _TELEGRAM_TEXT_LIMIT:
            return _Decision(OutboxDeliveryOutcome.DEAD_LETTER, "telegram_text_too_long")
        try:
            route_data = _strict_json_object(plaintext_route)
            route = TelegramDeliveryRoute.model_validate(route_data, strict=True)
            token_ref = binding.secret_refs["bot_token"]
            token = self._secrets.resolve(token_ref).get_secret_value()
            if _TELEGRAM_TOKEN.fullmatch(token) is None:
                raise ValueError("invalid token shape")
        except (KeyError, SecretResolutionError, ValueError, ValidationError):
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "telegram_delivery_configuration_invalid",
            )
        request_json: dict[str, Any] = {
            "chat_id": route.chat_id,
            "text": payload.text,
            "reply_parameters": {
                "message_id": route.reply_to_message_id,
                "allow_sending_without_reply": True,
            },
        }
        if route.message_thread_id is not None:
            request_json["message_thread_id"] = route.message_thread_id

        endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
        for attempt in range(1, self._telegram_transport_attempts + 1):
            if not await self._repository.renew_outbox_claim(claim):
                return _Decision(OutboxDeliveryOutcome.UNKNOWN, "delivery_lease_lost")
            try:
                response = await self._client.post(
                    endpoint,
                    json=request_json,
                    timeout=self._request_timeout,
                    follow_redirects=False,
                )
            except (httpx.ReadTimeout, httpx.WriteTimeout):
                return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")
            except (httpx.ConnectTimeout, httpx.ConnectError, httpx.PoolTimeout):
                if attempt < self._telegram_transport_attempts:
                    await self._sleeper(self._backoff_seconds(attempt))
                    continue
                return _Decision(
                    OutboxDeliveryOutcome.RETRY_WAIT,
                    "telegram_connect_failed",
                    retry_after=self._backoff(attempt),
                )
            except httpx.RequestError:
                return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")

            decision = _classify_telegram_response(response)
            if (
                decision.error_type == "telegram_server_unavailable"
                and attempt < self._telegram_transport_attempts
            ):
                await self._sleeper(self._backoff_seconds(attempt))
                continue
            if (
                decision.retry_after is None
                and decision.outcome is OutboxDeliveryOutcome.RETRY_WAIT
            ):
                return _Decision(
                    decision.outcome,
                    decision.error_type,
                    retry_after=self._backoff(attempt),
                )
            return decision
        raise AssertionError("Telegram attempt loop must always return")

    async def _deliver_wecom(
        self,
        claim: OutboxDeliveryClaim,
        payload: TextDeliveryPayload,
        response_url: str,
    ) -> _Decision:
        if not _trusted_wecom_url(response_url, self._wecom_allowed_hosts):
            return _Decision(
                OutboxDeliveryOutcome.DEAD_LETTER,
                "wecom_response_route_invalid",
            )
        if not await self._repository.renew_outbox_claim(claim):
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "delivery_lease_lost")
        try:
            response = await self._client.post(
                response_url,
                json={"msgtype": "text", "text": {"content": payload.text}},
                timeout=self._request_timeout,
                follow_redirects=False,
            )
        except httpx.RequestError:
            # The one-use response URL cannot be retried after an inconclusive send.
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "wecom_result_unknown")

        if 200 <= response.status_code < 300:
            body = _safe_response_json(response)
            if body is not None and body.get("errcode") == 0:
                return _Decision(OutboxDeliveryOutcome.SENT)
            # A successful HTTP status without the channel's application-level
            # acknowledgement cannot prove that the one-use route was consumed.
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "wecom_result_unknown")
        if 300 <= response.status_code < 500:
            return _Decision(OutboxDeliveryOutcome.DEAD_LETTER, "wecom_request_rejected")
        return _Decision(OutboxDeliveryOutcome.UNKNOWN, "wecom_result_unknown")

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("dispatcher clock must return a timezone-aware datetime")
        return now.astimezone(UTC)

    def _backoff(self, attempt: int) -> timedelta:
        multiplier = min(2 ** max(attempt - 1, 0), 64)
        return self._base_retry_delay * multiplier

    def _backoff_seconds(self, attempt: int) -> float:
        return self._backoff(attempt).total_seconds()


def _strict_json_object(raw: str) -> dict[str, Any]:
    def reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        del value
        raise ValueError("non-finite JSON number")

    parsed = json.loads(
        raw,
        object_pairs_hook=reject_duplicate,
        parse_constant=reject_constant,
    )
    if not isinstance(parsed, dict):
        raise ValueError("route must be a JSON object")
    return parsed


def _safe_response_json(response: httpx.Response) -> Mapping[str, Any] | None:
    if len(response.content) > 64 * 1024:
        return None
    try:
        value = response.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _classify_telegram_response(response: httpx.Response) -> _Decision:
    body = _safe_response_json(response)
    if 200 <= response.status_code < 300:
        if body is None:
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")
        if body.get("ok") is not True:
            error_code = body.get("error_code")
            if error_code == 429:
                return _Decision(
                    OutboxDeliveryOutcome.RETRY_WAIT,
                    "telegram_rate_limited",
                    retry_after=timedelta(seconds=_telegram_retry_after(body)),
                )
            if isinstance(error_code, int) and not isinstance(error_code, bool):
                if 400 <= error_code < 500:
                    return _Decision(
                        OutboxDeliveryOutcome.DEAD_LETTER,
                        "telegram_request_rejected",
                    )
                if error_code >= 500:
                    return _Decision(
                        OutboxDeliveryOutcome.RETRY_WAIT,
                        "telegram_server_unavailable",
                    )
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")
        result = body.get("result")
        if not isinstance(result, Mapping):
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")
        message_id = result.get("message_id")
        if isinstance(message_id, bool) or not isinstance(message_id, int) or message_id <= 0:
            return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")
        return _Decision(
            OutboxDeliveryOutcome.SENT,
            external_message_id=str(message_id),
        )
    if response.status_code == 429:
        retry_seconds = _telegram_retry_after(body)
        return _Decision(
            OutboxDeliveryOutcome.RETRY_WAIT,
            "telegram_rate_limited",
            retry_after=timedelta(seconds=retry_seconds),
        )
    if 400 <= response.status_code < 500:
        return _Decision(OutboxDeliveryOutcome.DEAD_LETTER, "telegram_request_rejected")
    if response.status_code >= 500:
        # A bare HTTP 5xx does not prove sendMessage had no side effect. Only
        # an explicit Bot API rejection (ok=false + server error_code) is safe
        # to retry; an ambiguous gateway/proxy response goes to reconciliation.
        error_code = body.get("error_code") if body is not None else None
        if (
            body is not None
            and body.get("ok") is False
            and isinstance(error_code, int)
            and not isinstance(error_code, bool)
            and error_code >= 500
        ):
            return _Decision(
                OutboxDeliveryOutcome.RETRY_WAIT,
                "telegram_server_unavailable",
            )
        return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")
    return _Decision(OutboxDeliveryOutcome.UNKNOWN, "telegram_result_unknown")


def _telegram_retry_after(body: Mapping[str, Any] | None) -> int:
    if body is None:
        return 30
    parameters = body.get("parameters")
    if not isinstance(parameters, Mapping):
        return 30
    value = parameters.get("retry_after")
    if isinstance(value, bool) or not isinstance(value, int):
        return 30
    return min(max(value, 1), 3_600)


def _trusted_wecom_url(value: str, allowed_hosts: frozenset[str]) -> bool:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return bool(
        parsed.scheme.casefold() == "https"
        and parsed.hostname is not None
        and parsed.hostname.casefold() in allowed_hosts
        and parsed.username is None
        and parsed.password is None
        and port in {None, 443}
        and not parsed.fragment
        and parsed.path
    )


def _metric_channel(claim: OutboxDeliveryClaim) -> str:
    credential = claim.reply_credential
    if credential is None:
        return "unknown"
    return {
        _TELEGRAM_CREDENTIAL: "telegram",
        _WECOM_CREDENTIAL: "wecom",
    }.get(credential.credential_kind, "unknown")
