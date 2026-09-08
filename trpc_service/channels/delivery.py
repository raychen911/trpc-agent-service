import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from trpc_service.channels.bindings import ChannelBindingRepository
from trpc_service.config import SecretResolver
from trpc_service.domain import ChannelType
from trpc_service.storage.contracts import OutboxHandler, OutboxRecord


def split_long_text(text: str, limit: int) -> tuple[str, ...]:
    if limit < 1:
        raise ValueError("limit must be positive")
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        boundary = max(remaining.rfind("\n", 0, limit + 1), remaining.rfind(" ", 0, limit + 1))
        if boundary < limit // 2:
            boundary = limit
        chunks.append(remaining[:boundary].rstrip())
        remaining = remaining[boundary:].lstrip()
    return tuple(chunks) or ("",)


class ImDeliveryHandlers:
    def __init__(
        self,
        bindings: ChannelBindingRepository,
        secrets: SecretResolver,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._bindings = bindings
        self._secrets = secrets
        self._client = client or httpx.AsyncClient(timeout=15)
        self._owns_client = client is None
        self._wecom_tokens: dict[str, tuple[str, float]] = {}

    def handlers(self) -> Mapping[str, OutboxHandler]:
        return {
            "im.reply.telegram": self.telegram_reply,
            "im.reply.wecom": self.wecom_reply,
        }

    async def telegram_reply(self, record: OutboxRecord) -> None:
        payload = record.payload
        binding = await self._bindings.resolve(ChannelType.TELEGRAM, str(payload["account_id"]))
        if binding.secret_ref is None:
            raise RuntimeError("Telegram binding requires bot token secret_ref")
        bot_token = await self._secrets.resolve(binding.secret_ref)
        request: dict[str, Any] = {"chat_id": payload["conversation_id"]}
        thread_id = dict(payload.get("metadata", {})).get("thread_id")
        if thread_id is not None:
            request["message_thread_id"] = thread_id
        delivery = dict(payload.get("delivery", {}))
        card = delivery.get("card")
        stream_updates = list(delivery.get("stream_updates") or [])
        final_chunks = split_long_text(str(payload.get("text", "")), 4000)
        if stream_updates:
            display_updates = [
                split_long_text(str(update), 4000)[0] for update in stream_updates if str(update)
            ]
            if final_chunks[0] and (not display_updates or display_updates[-1] != final_chunks[0]):
                display_updates.append(final_chunks[0])
            first_update = display_updates[0] if display_updates else final_chunks[0]
            response = await self._telegram_call(
                bot_token,
                "sendMessage",
                {
                    **request,
                    "text": first_update,
                    **({"reply_markup": card} if card else {}),
                },
            )
            message_id = response.get("result", {}).get("message_id")
            for update_text in display_updates[1:]:
                await self._telegram_call(
                    bot_token,
                    "editMessageText",
                    {
                        "chat_id": payload["conversation_id"],
                        "message_id": message_id,
                        "text": update_text,
                        **({"reply_markup": card} if card else {}),
                    },
                )
        remaining_chunks = final_chunks[1:] if stream_updates else final_chunks
        for index, chunk in enumerate(remaining_chunks):
            body = {**request, "text": chunk}
            if index == 0 and card and not stream_updates:
                body["reply_markup"] = card
            await self._telegram_call(bot_token, "sendMessage", body)
        for attachment in delivery.get("attachments") or []:
            item = dict(attachment)
            kind = "sendPhoto" if item.get("type") == "image" else "sendDocument"
            field = "photo" if kind == "sendPhoto" else "document"
            await self._telegram_call(
                bot_token, kind, {**request, field: item["url"], "caption": item.get("caption")}
            )

    async def _telegram_call(
        self, bot_token: str, method: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            response = await self._client.post(
                f"https://api.telegram.org/bot{bot_token}/{method}", json=payload
            )
            response.raise_for_status()
        except httpx.HTTPError:
            raise RuntimeError("Telegram delivery HTTP request failed") from None
        body = response.json()
        if not body.get("ok", False):
            raise RuntimeError(f"Telegram delivery rejected: {body}")
        return body

    async def wecom_reply(self, record: OutboxRecord) -> None:
        payload = record.payload
        binding = await self._bindings.resolve(ChannelType.WECOM, str(payload["account_id"]))
        if binding.options.get("mode", "app") == "aibot":
            await self._wecom_aibot_reply(payload)
            return
        corp_id = str(binding.options.get("corp_id", ""))
        agent_id = int(binding.options.get("agent_id", 0))
        corp_secret_ref = str(binding.options.get("delivery_token_ref", ""))
        if not corp_id or not agent_id or not corp_secret_ref:
            raise RuntimeError(
                "WeCom async delivery requires options.corp_id, agent_id and delivery_token_ref"
            )
        access_token = await self._wecom_access_token(binding.account_id, corp_id, corp_secret_ref)
        base = {"touser": payload["sender_user_id"], "agentid": agent_id}
        delivery = dict(payload.get("delivery", {}))
        card = delivery.get("card")
        if card:
            await self._wecom_call(
                access_token, {**base, "msgtype": "template_card", "template_card": card}
            )
        for chunk in split_long_text(str(payload.get("text", "")), 1900):
            await self._wecom_call(
                access_token, {**base, "msgtype": "text", "text": {"content": chunk}}
            )
        for attachment in delivery.get("attachments") or []:
            item = dict(attachment)
            if not item.get("media_id"):
                raise RuntimeError("WeCom attachment requires a previously uploaded media_id")
            kind = "image" if item.get("type") == "image" else "file"
            await self._wecom_call(
                access_token, {**base, "msgtype": kind, kind: {"media_id": item["media_id"]}}
            )

    async def _wecom_aibot_reply(self, payload: Mapping[str, Any]) -> None:
        metadata = payload.get("metadata", {})
        response_url = str(metadata.get("response_url", "")) if isinstance(metadata, dict) else ""
        parsed = urlsplit(response_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "qyapi.weixin.qq.com"
            or parsed.path != "/cgi-bin/aibot/response"
        ):
            raise RuntimeError("WeCom AIBot response_url is missing or not allowed")
        text = _truncate_utf8(str(payload.get("text", "")), 20_480)
        try:
            response = await self._client.post(
                response_url,
                json={"msgtype": "markdown", "markdown": {"content": text}},
            )
            response.raise_for_status()
        except httpx.HTTPError:
            raise RuntimeError("WeCom AIBot response_url delivery failed") from None
        if response.content:
            try:
                result = response.json()
            except ValueError:
                result = None
            if isinstance(result, dict) and int(result.get("errcode", 0)) != 0:
                raise RuntimeError(f"WeCom AIBot delivery rejected: {result}")

    async def _wecom_call(self, access_token: str, payload: dict[str, Any]) -> None:
        try:
            response = await self._client.post(
                "https://qyapi.weixin.qq.com/cgi-bin/message/send",
                params={"access_token": access_token},
                json=payload,
            )
            response.raise_for_status()
        except httpx.HTTPError:
            raise RuntimeError("WeCom delivery HTTP request failed") from None
        body = response.json()
        if int(body.get("errcode", -1)) != 0:
            raise RuntimeError(f"WeCom delivery rejected: {body}")

    async def _wecom_access_token(self, account_id: str, corp_id: str, secret_ref: str) -> str:
        cached = self._wecom_tokens.get(account_id)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        corp_secret = await self._secrets.resolve(secret_ref)
        try:
            response = await self._client.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={"corpid": corp_id, "corpsecret": corp_secret},
            )
            response.raise_for_status()
        except httpx.HTTPError:
            raise RuntimeError("WeCom access-token request failed") from None
        body = response.json()
        if int(body.get("errcode", -1)) != 0:
            raise RuntimeError(f"WeCom token request rejected: {body}")
        token = str(body["access_token"])
        self._wecom_tokens[account_id] = (
            token,
            time.monotonic() + max(30, int(body.get("expires_in", 7200)) - 120),
        )
        return token

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def _truncate_utf8(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")
