# ===================================================================
# channels.feishu - 飞书（Lark）通道（第三类真实可连 IM）
# ===================================================================
# 说明: 飞书开放平台事件订阅 + 机器人消息（PRD 3.1/3.3，满足「≥2 种 IM」）。
#   与企微的接入差异（PRD 3.1）:
#   - 鉴权: tenant_access_token = POST /auth/v3/tenant_access_token/internal
#           （app_id + app_secret，无需企业认证，个人开发者免费创建应用即可）
#   - 发送: POST /im/v1/messages?receive_id_type=open_id|chat_id
#           （Bearer token，content 为 JSON 字符串）
#   - 回调验签: header.token 与配置的 verification_token 比对（或
#           X-Lark-Signature = base64(HMAC-SHA256(timestamp+nonce+body, token))）
#   - 加密: 事件体可整体 AES-256-CBC 加密，key = md5(encrypt_key)（32 字节），
#           IV = key[:16]，明文格式 16B 随机 + 4B 长度 + 消息 + app_id
#   平台优势: 无 IP 白名单、个人号可真连发送（沙箱出站实测可达 open.feishu.cn）。
# 依赖: 标准库 + hashlib/hmac/base64 + cryptography + httpx（真实发送）。
# ===================================================================

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Optional

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ..events import AgentEvent, AgentResponse, AgentResponseChunk, MessageType
from ..tenant.models import ImChannelConfig
from .base import IMAdapter, ParsedWebhook, PlatformLimits, RecallEvent

FEISHU_API_BASE = "https://open.feishu.cn"

# ----------------------------------------------------------------------
# 飞书事件 AES 加解密（md5(encrypt_key) 作 32 字节密钥，CBC，IV=key[:16]）
# ----------------------------------------------------------------------


def _feishu_aes_key(encrypt_key: str) -> bytes:
    """md5(encrypt_key) -> 32 字符十六进制 -> 32 字节密钥（AES-256）。"""
    return hashlib.md5(encrypt_key.encode("utf-8")).hexdigest().encode("utf-8")


def _feishu_decrypt(encrypt: str, encrypt_key: str) -> str:
    """解密飞书事件体（AES-256-CBC，PKCS7），返回明文 JSON 字符串。"""
    key = _feishu_aes_key(encrypt_key)
    iv = key[:16]
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decrypted = cipher.decryptor().update(base64.b64decode(encrypt))
    unpadder = padding.PKCS7(128).unpadder()
    plaintext = unpadder.update(decrypted) + unpadder.finalize()
    # 明文格式: 16 字节随机 + 4 字节消息体长度 + 消息体 + app_id
    msg_len = int.from_bytes(plaintext[16:20], "big")
    return plaintext[20:20 + msg_len].decode("utf-8")


def _feishu_encrypt(plaintext: str, encrypt_key: str, app_id: str) -> str:
    """加密飞书事件体（供自测构造加密回调，与 _feishu_decrypt 互逆）。"""
    import os

    key = _feishu_aes_key(encrypt_key)
    iv = key[:16]
    payload = (os.urandom(16) + len(plaintext.encode()).to_bytes(4, "big") + plaintext.encode() + app_id.encode())
    padder = padding.PKCS7(128).padder()
    padded = padder.update(payload) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    return base64.b64encode(cipher.encryptor().update(padded) + cipher.encryptor().finalize()).decode()


class FeishuAdapter(IMAdapter):
    """飞书应用/机器人通道（支持明文与安全模式事件回调 + 机器人发送）。"""

    channel_type = "feishu"

    def __init__(self, config: Optional[ImChannelConfig] = None, api_base: str = FEISHU_API_BASE) -> None:
        super().__init__(config)
        self._api_base = api_base.rstrip("/")
        self._app_id = ""
        self._secret = None
        self._encrypt_key: Optional[str] = None
        self._default_target: str = ""
        if config is not None:
            self._app_id = config.app_id or ""
            if config.secret_ref is not None:
                self._secret = config.secret_ref.get_secret_value()
            if config.aes_key_ref is not None:
                self._encrypt_key = config.aes_key_ref.get_secret_value()
            if config.default_target:
                self._default_target = config.default_target
        # tenant_access_token 缓存（约 2h TTL，提前 60s 刷新）
        self._tenant_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._http = None  # 懒加载 httpx.AsyncClient

    # ------------------------------------------------------------------
    # URL 验证（飞书以 POST 发送 url_verification challenge）
    # ------------------------------------------------------------------

    def url_verification_response(self, body: bytes, headers: dict[str, str]) -> Optional[dict]:
        """若本请求为飞书 URL 验证，返回需回显的 JSON 体；否则 None。

        飞书在事件订阅保存时发 POST {"type":"url_verification","challenge":...,
        "token":...}；校验 token 后原样回显 challenge（PRD 3.4）。
        """
        try:
            data = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        if data.get("type") != "url_verification":
            return None
        token = data.get("token", "")
        # 配置了 verification_token 时必须一致（未配置则放行，本地自测）
        if self._token and token and not hmac.compare_digest(self._token, token):
            return None
        return {"challenge": data.get("challenge", "")}

    # ------------------------------------------------------------------
    # 验签（PRD 3.4）
    # ------------------------------------------------------------------

    def verify_signature(self, body: bytes, signature: str) -> bool:
        """飞书验签。

        优先 X-Lark-Signature（HMAC-SHA256(timestamp+nonce+body, verification_token)）；
        否则比对事件体 header.token 与配置 verification_token。未配置 token 时放行。
        """
        token = self._token or ""
        if not token:
            return True
        # 1) HMAC 签名头（飞书新版推荐）: sha256=<base64>[,|?]timestamp=..[&|,]nonce=..
        if isinstance(signature, str) and signature.startswith("sha256="):
            b64, _, _ = _parse_lark_header(signature)
            if b64:
                return hmac.compare_digest(b64, _compute_lark_signature(token, body, signature))
        # 2) 事件体 header.token 比对（parse 时填入 signature）
        return hmac.compare_digest(token, signature or "")

    # ------------------------------------------------------------------
    # 解析
    # ------------------------------------------------------------------

    def parse_webhook(self, body: bytes, headers: dict[str, str]) -> ParsedWebhook:
        """解析飞书回调（明文 / 加密模式）为 AgentEvent。"""
        try:
            raw = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError(f"飞书 webhook JSON 解析失败: {exc}") from exc

        # 加密模式: {"encrypt": "base64(AES(...))"}
        if "encrypt" in raw and self._encrypt_key:
            plaintext = _feishu_decrypt(raw["encrypt"], self._encrypt_key)
            data = json.loads(plaintext)
        else:
            data = raw

        header = data.get("header", {})
        event_type = header.get("event_type", "")
        signature = header.get("token", "")

        # 消息接收事件
        if event_type == "im.message.receive_v1":
            event = self._parse_message_event(data)
            return ParsedWebhook(event=event, signature=signature, raw_body=body, headers=headers)

        # 其它事件（进群等）统一成空文本事件，交由链路兜底
        event = AgentEvent(
            channel_type="feishu",
            channel_id=self._app_id,
            user_id="",
            msg_id=data.get("event_id", ""),
            content="",
            msg_type=MessageType.TEXT,
            metadata={"event_type": event_type},
        )
        return ParsedWebhook(event=event, signature=signature, raw_body=body, headers=headers)

    def parse_recall_event(self, body: bytes, headers: dict[str, str]) -> Optional[RecallEvent]:
        """识别飞书消息撤回（im.message.recalled_v1）。

        撤回事件结构（官方协议）:
          header.event_type = "im.message.recalled_v1"
          event.message_id   = 被撤回消息的 message_id（用于历史定位）
          event.operator_id  = 撤回人（open_id / user_id）
        返回 RecallEvent；非撤回事件返回 None（按普通消息处理）。
        """
        try:
            data = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        if "encrypt" in data and self._encrypt_key:
            try:
                data = json.loads(_feishu_decrypt(data["encrypt"], self._encrypt_key))
            except Exception:  # noqa: BLE001 - 解密失败按非撤回处理
                return None
        header = data.get("header", {})
        if header.get("event_type") != "im.message.recalled_v1":
            return None
        event = data.get("event", {})
        operator = event.get("operator_id", {}) or {}
        return RecallEvent(
            msg_id=event.get("message_id", "") or "",
            user_id=operator.get("open_id", "") or operator.get("user_id", "") or "",
            channel_id=self._app_id,
            channel_type="feishu",
            event_type="im.message.recalled_v1",
        )

    def _parse_message_event(self, data: dict) -> AgentEvent:
        event = data.get("event", {})
        message = event.get("message", {})
        sender = event.get("sender", {})
        sender_id = sender.get("sender_id", {})
        open_id = sender_id.get("open_id", "")
        chat_id = message.get("chat_id", "")
        msg_id = message.get("message_id", "")
        # content 是 JSON 字符串: {"text": "..."} / {"post": ...} / ...
        content = ""
        try:
            content_obj = json.loads(message.get("content", "{}"))
            content = content_obj.get("text", "")
        except (ValueError, AttributeError):
            content = message.get("content", "")
        is_group = message.get("chat_type") == "group"
        return AgentEvent(
            channel_type="feishu",
            channel_id=chat_id or self._app_id,
            user_id=open_id,
            msg_id=msg_id,
            content=content,
            msg_type=MessageType.TEXT,
            is_group=is_group,
            metadata={
                "chat_id": chat_id,
                "open_id": open_id,
                "chat_type": message.get("chat_type", "")
            },
        )

    # ------------------------------------------------------------------
    # 发送（机器人消息接口）
    # ------------------------------------------------------------------

    async def _client(self):
        if self._http is None:
            import httpx

            self._http = httpx.AsyncClient(timeout=10.0)
        return self._http

    async def _get_tenant_token(self) -> str:
        """获取/缓存 tenant_access_token（app_id + app_secret）。"""
        if self._tenant_token and time.time() < self._token_expires_at:
            return self._tenant_token
        if not self._secret:
            raise RuntimeError("飞书通道未配置 secret_ref，无法获取 tenant_access_token")
        client = await self._client()
        resp = await client.post(
            f"{self._api_base}/open-apis/auth/v3/tenant_access_token/internal",
            json={
                "app_id": self._app_id,
                "app_secret": self._secret
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"飞书 tenant_access_token 失败: {data.get('msg')}")
        self._tenant_token = data["tenant_access_token"]
        self._token_expires_at = time.time() + int(data.get("expire", 7200)) - 60
        return self._tenant_token

    async def send_message(self, tenant_id: str, msg: AgentResponse) -> None:
        """通过机器人消息接口真实投递（PRD 3.2）。"""
        if not self._secret:
            raise RuntimeError(f"飞书通道未配置 secret_ref: tenant={tenant_id}")
        token = await self._get_tenant_token()
        # 收件人: 群消息发到 chat_id，单聊发到 open_id
        chat_id = msg.metadata.get("chat_id") or msg.metadata.get("from_chat")
        open_id = msg.metadata.get("user_id") or msg.metadata.get("from_user") or self._default_target
        if chat_id:
            receive_id = chat_id
            receive_id_type = "chat_id"
        elif open_id:
            receive_id = open_id
            receive_id_type = "open_id"
        else:
            raise RuntimeError(f"飞书 send 缺少 chat_id/open_id: tenant={tenant_id}")
        client = await self._client()
        for chunk_text in self.split_long_message(msg.content):
            payload = {
                "receive_id": receive_id,
                "msg_type": "text",
                "content": json.dumps({"text": chunk_text}, ensure_ascii=False),
            }
            resp = await client.post(
                f"{self._api_base}/open-apis/im/v1/messages",
                params={"receive_id_type": receive_id_type},
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code", 0) != 0:
                raise RuntimeError(f"飞书消息发送失败: {data.get('msg')}")

    async def send_streaming(self, tenant_id: str, chunk: AgentResponseChunk) -> None:
        # 飞书不支持流式编辑，累积后整条发送（PRD 3.6）
        return None

    def platform_limits(self) -> PlatformLimits:
        return PlatformLimits(
            max_message_len=2000,
            rate_limit_per_sec=5.0,
            supports_streaming=False,
            supports_card=True,
            supports_media=True,
            ack_required=False,
        )

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None


def _parse_lark_header(value: str) -> tuple[str, str, str]:
    """从 X-Lark-Signature 头解析出 (base64, timestamp, nonce)。

    支持两种常见形态:
      sha256=<b64>,timestamp=<ts>,nonce=<nonce>    （逗号分隔）
      sha256=<b64>?timestamp=<ts>&nonce=<nonce>    （query 形态）
    无法解析时返回 (空, "", "")。
    """
    b64 = ""
    timestamp = ""
    nonce = ""
    if not value.startswith("sha256="):
        return b64, timestamp, nonce
    rest = value[len("sha256="):]
    # base64 部分在首个分隔符（, 或 ?）之前
    head = rest.split("?", 1)[0].split(",", 1)[0]
    if head:
        b64 = head
    # 逗号分隔段: ,timestamp=..,nonce=..
    for seg in rest.split(",")[1:]:
        k, _, v = seg.partition("=")
        if k == "timestamp":
            timestamp = v
        elif k == "nonce":
            nonce = v
    # query 段: ?timestamp=..&nonce=..
    if "?" in rest:
        _, qs = rest.split("?", 1)
        for pair in qs.split("&"):
            k, _, v = pair.partition("=")
            if k == "timestamp":
                timestamp = v
            elif k == "nonce":
                nonce = v
    return b64, timestamp, nonce


def _compute_lark_signature(token: str, body: bytes, header_value: str) -> str:
    """飞书 X-Lark-Signature = base64(HMAC-SHA256(timestamp+nonce+body, token))。

    header_value 为 X-Lark-Signature 头的完整值（形如
    'sha256=...,timestamp=..,nonce=..' 或 'sha256=...?timestamp=..&nonce=..'），
    从中解析出 timestamp/nonce 参与签名。
    """
    _, timestamp, nonce = _parse_lark_header(header_value)
    raw = (timestamp + nonce).encode("utf-8") + body
    digest = hmac.new(token.encode("utf-8"), raw, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()
