# ===================================================================
# channels.base - IM Channel Adapter 抽象（平台层新增）
# ===================================================================
# 说明: 每类 IM 一个实现（PRD 3.1），统一接口:
#   parse_webhook / send_message / send_streaming / verify_signature / platform_limits
#   （抽象一个 IM 类，适配企微/微信/飞书——老师建议）
# 规范: 外部 IM 消息 -> AgentEvent；AgentResponse -> IM 回复（PRD 3.2）。
# ===================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ..events import AgentEvent, AgentResponse, AgentResponseChunk
from ..tenant.models import ImChannelConfig


@dataclass
class PlatformLimits:
    """IM 平台限制（PRD 3.6）。"""

    max_message_len: int = 2048
    """单条消息最大长度（超长自动分段）。"""
    length_unit: str = "char"
    """长度计量单位: "char"（如飞书 2000 字）或 "bytes"（如企微 2048 字节）。
    审查 09-04：企微按字节限长而分段按字符计，纯中文长回复可 3 倍超限被
    平台拒收——bytes 单位下分段按 UTF-8 编码长度累积。"""
    rate_limit_per_sec: float = 20.0
    """消息频率上限（令牌桶）。"""
    supports_streaming: bool = False
    """是否支持流式（编辑式）回复。"""
    supports_card: bool = False
    """是否支持卡片消息。"""
    supports_media: bool = False
    """是否支持图片 / 文件。"""
    ack_required: bool = False
    """是否需 5 秒内先回 ack 再异步推送（企微）。"""


@dataclass
class ParsedWebhook:
    """webhook 解析结果（含路由信息与 AgentEvent）。"""

    event: AgentEvent
    signature: str = ""
    """回调签名（验签用，PIIFilter 会脱敏）。"""
    raw_body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class RecallEvent:
    """IM 消息撤回事件（PRD 3.7）。

    经 IMAdapter.parse_recall_event 识别平台撤回回调产出；平台侧标记
    会话历史 revoked=true + 记审计 recall，**不触发 Agent**（撤回不是新输入）。
    """

    msg_id: str
    """被撤回消息的平台 ID（用于在 session history 中定位标记）。"""
    user_id: str = ""
    """撤回操作者（内部 user_id，经 map_user_id 映射后使用）。"""
    channel_id: str = ""
    """平台侧账号标识（corp_id / bot_id / app_id）。"""
    channel_type: str = ""
    """IM 通道类型（如 feishu / wechat_work）；空时由调用方按 adapter 补。"""
    is_group: bool = False
    """撤回是否发生在群聊（能判定时由 adapter 填写）；影响 session_id 推算
    的 scope（generate_session_id 群聊规则），单聊撤回保持 False。"""
    session_id: str = ""
    """可选：能定位到具体会话时填写（部分平台撤回事件仅带 message_id，
    无会话上下文则留空——此时平台只记审计，不做历史标记，见 PRD 3.7）。"""
    event_type: str = "recall"
    """平台原始事件类型（如 im.message.recalled_v1）。"""


def mark_message_revoked(session_state: dict, recall_msg_id: str) -> bool:
    """在会话历史中把指定 msg_id 的用户消息标记 revoked=true（PRD 3.7）。

    纯函数便于单测。平台事件若不含 session 定位信息（如飞书撤回事件只有
    message_id 无 chat_id），则由上层决定如何取得对应 session_state 后调用。

    Args:
        session_state: Session state（含 history 列表）
        recall_msg_id: 被撤回消息的平台 ID

    Returns:
        True 表示有历史消息被标记；False 表示未找到（含 history 缺省/无匹配）。
    """
    marked = False
    for item in session_state.get("history", []):
        if item.get("role") == "user" and item.get("msg_id") == recall_msg_id:
            item["revoked"] = True
            marked = True
    return marked


class IMAdapter(ABC):
    """IM 通道适配器抽象（PRD 3.1）。"""

    channel_type: str = "base"

    def __init__(self, config: Optional[ImChannelConfig] = None) -> None:
        self._config = config
        self._token = None
        if config is not None and config.token_ref is not None:
            self._token = config.token_ref.get_secret_value()

    @property
    def config(self) -> Optional[ImChannelConfig]:
        return self._config

    def map_user_id(self, external_user_id: str) -> str:
        """外部平台 user_id -> 内部 user_id（PRD 3.4 身份映射）。

        按租户 `im_channel_config.user_id_mapping`（{外部 user_id: 内部 user_id}）
        做值映射；未命中时原样返回。例如飞书 open_id -> 企业内部员工号，
        使跨通道身份统一，供 UserAuthFilter 白名单 / 审计使用。
        """
        cfg = self._config
        if cfg is None or not cfg.user_id_mapping:
            return external_user_id
        return cfg.user_id_mapping.get(external_user_id, external_user_id)

    @abstractmethod
    def parse_webhook(self, body: bytes, headers: dict[str, str]) -> ParsedWebhook:
        """解析 IM webhook 回调为 AgentEvent（PRD 3.2 IM -> Agent 输入）。

        实现须同步填充:
          - event.msg_id（幂等去重）
          - event.channel_type / channel_id / user_id / content
          - ParsedWebhook.signature（验签）
        """

    @abstractmethod
    async def send_message(self, tenant_id: str, msg: AgentResponse) -> None:
        """发送完整回复（文本 / 卡片）。"""

    @abstractmethod
    async def send_streaming(self, tenant_id: str, chunk: AgentResponseChunk) -> None:
        """发送流式分片（不支持流式的通道回退为累积文本）。"""

    @abstractmethod
    def verify_signature(self, body: bytes, signature: str) -> bool:
        """回调验签（PRD 3.4）。"""

    def verify_echostr(self, echostr: str, timestamp: str, nonce: str, signature: str) -> bool:
        """URL 验证（echostr）签名校验。无验签通道默认放行。

        企微在配置回调 URL 时发 GET 请求携带 echostr，
        服务端需校验签名后原样返回明文。默认放行（web 等无验签通道）。
        """
        return True

    def url_verification_response(self, body: bytes, headers: dict[str, str]) -> Optional[dict]:
        """URL 验证回显（飞书以 POST 发送 url_verification challenge）。

        默认返回 None（表示非此类请求）。飞书等通道重写此方法：
        命中时返回需回显的 JSON 体（如 {"challenge": "..."}），
        Gateway 在验签前短路直接响应，避免把 challenge 当成消息处理。
        """
        return None

    def parse_recall_event(self, body: bytes, headers: dict[str, str]) -> Optional[RecallEvent]:
        """识别平台撤回事件回调（PRD 3.7）。

        默认返回 None（不识别撤回，按普通消息处理）；支持撤回回调的平台
        覆写此方法：命中撤回时返回 RecallEvent（含被撤回消息 msg_id），
        Gateway 据此标记会话历史 revoked + 记审计，不触发 Agent。

        ⚠️ 平台协议实现状态：飞书 `im.message.recalled_v1` 已实现；其余平台
        撤回回调格式需公网接收方向验证（留老师代验），未覆写前按非撤回处理。
        """
        return None

    def decrypt_echostr(self, echostr: str) -> str:
        """URL 验证用：解密 echostr 返回明文。默认原样返回。"""
        return echostr

    @abstractmethod
    def platform_limits(self) -> PlatformLimits:
        """返回平台限制（PRD 3.6 分段 / 限频）。"""

    async def send_media(self, tenant_id: str, msg: AgentResponse, media_url: str, media_type: str = "image") -> None:
        """发送图片 / 文件消息（PRD 3.1/3.6 多模态）。

        默认抛 NotImplementedError；各通道按平台能力实现
        （企微 upload media / 飞书 media API）。
        """
        raise NotImplementedError(f"{self.channel_type} 未实现图片/文件发送")

    def split_long_message(self, text: str) -> list[str]:
        """按平台限制自动分段（PRD 3.6 消息长度）。

        length_unit="bytes" 时按 UTF-8 编码长度累积切分（企微 2048 字节），
        "char" 按字符数切分（飞书 2000 字）——审查 09-04 修复字节/字符混用。
        """
        limits = self.platform_limits()
        max_len = limits.max_message_len
        if not text:
            return []
        if limits.length_unit == "bytes":
            encoded = text.encode("utf-8")
            if len(encoded) <= max_len:
                return [text]
            chunks: list[str] = []
            start = 0
            while start < len(encoded):
                end = min(start + max_len, len(encoded))
                # 回退到字符边界：切点处（首个被排除的字节）若是续字节，
                # 说明把一个多字节字符切成了两半，向前回退到 lead 字节
                while end < len(encoded) and (encoded[end] & 0xC0) == 0x80:
                    end -= 1
                if end <= start:  # 单字符超长（理论不发生于 max_len>=4）
                    end = start + max_len
                chunks.append(encoded[start:end].decode("utf-8", errors="ignore"))
                start = end
            return chunks
        if len(text) <= max_len:
            return [text]
        return [text[i:i + max_len] for i in range(0, len(text), max_len)]
