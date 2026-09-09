# ===================================================================
# channels.wechat_work - 企业微信通道（题目硬性要求候选）
# ===================================================================
# 说明: 企业微信回调消息（PRD 3.3/3.4）:
#   - 验签: msg_signature = SHA1(sort(token, timestamp, nonce, encrypt))
#   - 加解密: AES-256-CBC，AESKey = Base64Decode(EncodingAESKey + "=")，
#     密文 JSON 包裹；明文 = 16字节随机 + 4字节长度 + 消息 + corp_id；
#     填充为企微官方 PKCS7 但分组 **32 字节**（勿用标准 16 字节 unpadder）
#   - 回复: 应用消息接口（需 access_token）或被动回复
#   - 平台限制: 2048 字节分段、~20 次/秒、5 秒 ack（PRD 3.6）
# 依赖: 标准库 + hashlib/hmac/cryptography + httpx（真实发送）
# 规范: secret_ref 为应用 Secret（gettoken 用），aes_key_ref 为回调
#   EncodingAESKey，token_ref 为回调验证 Token —— 三者用途不同不可混用。
# ===================================================================

from __future__ import annotations

import base64
import hashlib
import hmac
import time
import xml.etree.ElementTree as ET
from typing import Optional

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ..events import AgentEvent, AgentResponse, AgentResponseChunk, MessageType
from ..tenant.models import ImChannelConfig
from .base import IMAdapter, ParsedWebhook, PlatformLimits

# 企业微信回调报文 XML 命名空间
_NS = {"ns": "http://www.work.weixin.qq.com"}

QYAPI_BASE = "https://qyapi.weixin.qq.com"


def derive_aes_key(encoding_aes_key: str) -> bytes:
    """EncodingAESKey(43 字符) -> 32 字节 AES 密钥（官方算法）。"""
    return base64.b64decode((encoding_aes_key + "=").encode())


def _pkcs7_pad_wechat(data: bytes) -> bytes:
    """企微 WXBizMsgCrypt 填充: PKCS7 风格但分组为 **32 字节**（官方 blockSize=32，
    pad 值 = 补齐长度，1..32）。用标准 16 字节 unpadder 会把合法密文误判非法
    （实测真实企微回调 Invalid padding bytes，2026-09-02 修复）。"""
    pad = 32 - (len(data) % 32)
    return data + bytes([pad]) * pad


def _pkcs7_unpad_wechat(data: bytes) -> bytes:
    """去掉企微 PKCS7(32) 填充（末字节 = 补齐长度 1..32）。"""
    pad = data[-1]
    if not (1 <= pad <= 32) or data[-pad:] != bytes([pad]) * pad:
        raise ValueError("Invalid padding bytes.")
    return data[:-pad]


def _aes_decrypt(encrypt: str, aes_key: bytes) -> str:
    """AES-256-CBC 解密回调密文（企微 PKCS7-32），返回 XML 明文。"""
    iv = aes_key[:16]
    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv))
    decrypted = cipher.decryptor().update(base64.b64decode(encrypt))
    plaintext = _pkcs7_unpad_wechat(decrypted)
    # 明文格式: 16字节随机 + 4字节消息体长度 + 消息体 + CorpId
    msg_len = int.from_bytes(plaintext[16:20], "big")
    return plaintext[20:20 + msg_len].decode("utf-8")


def _aes_encrypt(plaintext: str, aes_key: bytes, receive_id: str) -> str:
    """AES-256-CBC 加密（企微 PKCS7-32），构造回调密文（测试 / 调试用）。"""
    import os

    iv = aes_key[:16]
    payload = os.urandom(16) + len(plaintext.encode()).to_bytes(4, "big") + plaintext.encode() + receive_id.encode()
    padded = _pkcs7_pad_wechat(payload)
    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv))
    return base64.b64encode(cipher.encryptor().update(padded) + cipher.encryptor().finalize()).decode()


def _signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    """msg_signature = SHA1(sort(token, timestamp, nonce, encrypt))。"""
    sort_list = sorted([token, timestamp, nonce, encrypt])
    return hashlib.sha1("".join(sort_list).encode()).hexdigest()


class WechatWorkAdapter(IMAdapter):
    """企业微信应用回调通道（满足「微信类」硬性要求）。"""

    channel_type = "wechat_work"

    def __init__(self, config: Optional[ImChannelConfig] = None, api_base: str = QYAPI_BASE) -> None:
        super().__init__(config)
        self._api_base = api_base.rstrip("/")
        self._secret = None
        self._aes_key: Optional[bytes] = None
        if config is not None:
            if config.secret_ref is not None:
                self._secret = config.secret_ref.get_secret_value()
            if config.aes_key_ref is not None:
                self._aes_key = derive_aes_key(config.aes_key_ref.get_secret_value())
            self._corp_id = config.app_id or ""
            self._agent_id = config.agent_id or ""
        else:
            self._corp_id = ""
            self._agent_id = ""
        # access_token 缓存（7200s TTL，提前 60s 刷新）
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        # 验签所需的当前回调参数（parse 时填充）
        self._last_timestamp = ""
        self._last_nonce = ""
        self._last_encrypt = ""
        self._http = None  # 懒加载 httpx.AsyncClient

    # ------------------------------------------------------------------
    # 验签（PRD 3.4）
    # ------------------------------------------------------------------

    def verify_signature(self, body: bytes, signature: str) -> bool:
        """企业微信 msg_signature = SHA1(sort(token, timestamp, nonce, encrypt))。

        使用 parse_webhook 填充的 timestamp/nonce/encrypt 计算；未配置 token 时
        放行（本地自测/调试，同 SignatureFilter 的 skip 语义）。
        """
        token = self._token or ""
        if not token:
            return True
        expected = _signature(token, self._last_timestamp, self._last_nonce, self._last_encrypt)
        return hmac.compare_digest(expected, signature)

    def verify_echostr(self, echostr: str, timestamp: str, nonce: str, signature: str) -> bool:
        """URL 验证: msg_signature = SHA1(sort(token, timestamp, nonce, echostr))。"""
        token = self._token or ""
        if not token or not signature:
            return True  # 未配置 token / 无签名（本地自测）
        expected = _signature(token, timestamp, nonce, echostr)
        return hmac.compare_digest(expected, signature)

    def decrypt_echostr(self, echostr: str) -> str:
        """URL 验证用: echostr 密文 AES 解密回明文（未配置 aes_key 原样返回）。"""
        if self._aes_key is None:
            return echostr
        return _aes_decrypt(echostr, self._aes_key)

    # ------------------------------------------------------------------
    # 解析
    # ------------------------------------------------------------------

    def parse_webhook(self, body: bytes, headers: dict[str, str]) -> ParsedWebhook:
        """解析企微回调 XML（加密模式）。"""
        try:
            root = ET.fromstring(body.decode("utf-8"))
        except ET.ParseError as exc:
            raise ValueError(f"企微 webhook XML 解析失败: {exc}") from exc

        def _find(tag: str) -> str:
            # 兼容带 xmlns 命名空间（真实企微回调）与不带命名空间两种形态
            node = root.find(f"ns:{tag}", _NS)
            if node is None:
                node = root.find(tag)
            return node.text or "" if node is not None else ""

        encrypt = _find("Encrypt")
        self._last_encrypt = encrypt
        # URL 参数中的签名（headers 由 web 层传入 query 合并）
        signature = headers.get("msg_signature", "")
        timestamp = headers.get("timestamp", "")
        nonce = headers.get("nonce", "")
        if timestamp:
            self._last_timestamp = timestamp
        if nonce:
            self._last_nonce = nonce
        if not timestamp and not nonce:
            self._last_timestamp = _find("TimeStamp") or str(int(time.time()))
            self._last_nonce = _find("Nonce")

        if encrypt and self._aes_key is not None:
            plaintext = _aes_decrypt(encrypt, self._aes_key)
            # 解密后: <xml><ToUserName/><FromUserName/><CreateTime/><MsgType/><Content/><MsgId/></xml>
            msg_root = ET.fromstring(plaintext)

            def _msg(tag: str) -> str:
                node = msg_root.find(tag)
                return node.text or "" if node is not None else ""

            # 群聊判定（对齐 wecom_bot 先例）：回调带 ChatId 即群聊，
            # channel_id 用群 ID；单聊无 ChatId，channel_id 仍用应用 ID。
            chat_id = _msg("ChatId")
            event = AgentEvent(
                channel_type="wechat_work",
                channel_id=chat_id or _msg("ToUserName"),
                user_id=_msg("FromUserName"),
                msg_id=_msg("MsgId") or f"wx_{int(time.time() * 1000)}",
                content=_msg("Content"),
                msg_type=MessageType.TEXT,
                is_group=bool(chat_id),
                metadata={
                    "create_time": _msg("CreateTime"),
                    "chat_id": chat_id
                },
            )
        else:
            # 未配置 aes_key / 明文调试模式: 从 XML 直接读
            chat_id = _find("ChatId")
            event = AgentEvent(
                channel_type="wechat_work",
                channel_id=chat_id or _find("ToUserName"),
                user_id=_find("FromUserName"),
                msg_id=_find("MsgId") or f"wx_{int(time.time() * 1000)}",
                content=_find("Content"),
                msg_type=MessageType.TEXT,
                is_group=bool(chat_id),
                metadata={"chat_id": chat_id},
            )
        return ParsedWebhook(event=event, signature=signature, raw_body=body, headers=headers)

    # ------------------------------------------------------------------
    # 发送（应用消息接口，需 access_token）
    # ------------------------------------------------------------------

    async def _client(self):
        if self._http is None:
            import httpx

            self._http = httpx.AsyncClient(timeout=10.0)
        return self._http

    async def _get_access_token(self) -> str:
        """获取/缓存 access_token（corpsecret 换 token，7200s TTL）。"""
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token
        if not self._secret:
            raise RuntimeError("企微通道未配置应用 secret_ref，无法获取 access_token")
        client = await self._client()
        resp = await client.get(
            f"{self._api_base}/cgi-bin/gettoken",
            params={
                "corpid": self._corp_id,
                "corpsecret": self._secret
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode", 0) != 0:
            raise RuntimeError(f"企微 gettoken 失败: {data.get('errmsg')}")
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + int(data.get("expires_in", 7200)) - 60
        return self._access_token

    async def send_message(self, tenant_id: str, msg: AgentResponse) -> None:
        """通过应用消息接口真实投递（PRD 3.2/3.6 文本分段）。"""
        if not self._secret:
            raise RuntimeError(f"企微通道未配置 secret_ref: tenant={tenant_id}")
        token = await self._get_access_token()
        # 接收者: 单聊为发起者 user_id；群聊为群 ID + 指定成员
        touser = msg.metadata.get("user_id") or msg.metadata.get("from_user")
        if not touser:
            raise RuntimeError(f"企微 send 缺少 user_id: tenant={tenant_id}")
        client = await self._client()
        for chunk_text in self.split_long_message(msg.content):
            payload = {
                "touser": touser,
                "msgtype": "text",
                "agentid": int(self._agent_id or 0),
                "text": {
                    "content": chunk_text
                },
            }
            resp = await client.post(
                f"{self._api_base}/cgi-bin/message/send",
                params={"access_token": token},
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errcode", 0) != 0:
                raise RuntimeError(f"企微 message/send 失败: {data.get('errmsg')}")

    async def send_streaming(self, tenant_id: str, chunk: AgentResponseChunk) -> None:
        # 企微不支持流式编辑，累积后整条发送（PRD 3.6 异步回复）
        return None

    def platform_limits(self) -> PlatformLimits:
        return PlatformLimits(
            max_message_len=2048,  # 企微按字节限长（PRD 3.6）
            length_unit="bytes",
            rate_limit_per_sec=20.0,
            supports_streaming=False,
            supports_card=True,
            supports_media=True,
            ack_required=True,  # 5 秒内先回 ack（PRD 3.6）
        )

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None
