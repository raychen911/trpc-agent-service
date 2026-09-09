# ===================================================================
# tenant.resolver - 租户解析与 Session ID 生成（平台层新增）
# ===================================================================
# 说明: Gateway 通过 Filter 从 URL 子域名 / Header X-Tenant-ID /
#   webhook path 提取 tenant_id 注入运行上下文（PRD 1.3-2）。
#   同时提供 PRD 1.3-3 的 Session ID 生成规则。
# 规范: 解析优先级: Header > 子域名 > webhook path > 查询参数
# ===================================================================

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Optional

from .models import ChannelType

# Header 名（PRD 1.3-2）
HEADER_TENANT_ID = "X-Tenant-ID"

# webhook path 形状: /webhook/{channel_type}/{binding_id}，binding_id 隐含 tenant_id
_WEBHOOK_PATH_RE = re.compile(r"^/webhook/(?P<channel>[a-z_]+)/(?P<binding>[A-Za-z0-9_-]+)/?$")

# 子域名形状: {tenant_id}.gateway.example.com（Host 头大小写不敏感，匹配忽略大小写、保留原始 tenant）
_SUBDOMAIN_RE = re.compile(r"^(?P<tenant>[a-zA-Z0-9_-]+)\.", re.IGNORECASE)


@dataclass
class ResolvedRequest:
    """从一次 IM webhook / HTTP 请求中解析出的路由信息。"""

    tenant_id: str = ""
    channel_type: Optional[ChannelType] = None
    binding_id: str = ""
    """channel_binding.binding_id，隐含租户归属（PRD 3.4）。"""
    session_id: str = ""
    """预生成的 session_id（可空，由 Runtime 按需生成）。"""
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def is_resolved(self) -> bool:
        return bool(self.tenant_id)


def resolve_tenant_id(
    headers: Optional[dict[str, str]] = None,
    host: Optional[str] = None,
    path: Optional[str] = None,
    query: Optional[dict[str, str]] = None,
) -> str:
    """按优先级解析 tenant_id: Header > 子域名 > webhook path > 查询参数。"""
    headers = headers or {}
    query = query or {}

    # 1. Header X-Tenant-ID
    tenant = headers.get(HEADER_TENANT_ID) or headers.get(HEADER_TENANT_ID.lower())
    if tenant:
        return tenant

    # 2. 子域名: {tenant}.gateway.example.com（Host 头大小写不敏感，匹配忽略大小写、保留原始 tenant）
    if host:
        match = _SUBDOMAIN_RE.match(host)
        if match:
            candidate = match.group("tenant")
            if candidate.lower() not in ("gateway", "admin", "www"):
                return candidate

    # 3. webhook path: /webhook/{channel}/{binding}
    if path:
        match = _WEBHOOK_PATH_RE.match(path)
        if match:
            binding = match.group("binding")
            # binding_id 前缀约定: {tenant_id}__{channel_binding_id}
            if "__" in binding:
                return binding.split("__", 1)[0]

    # 4. 查询参数
    return query.get("tenant_id", "")


def generate_session_id(
    tenant_id: str,
    channel_type: ChannelType,
    channel_id: str,
    external_user_id: str,
    is_group: bool = False,
) -> str:
    """生成确定性 session_id（PRD 1.3-3）。

    同一 (tenant, channel, 用户) 恒定映射到同一 session；
    群聊时 external_user_id 传 group_id_user_id（PRD 3.5）。
    """
    scope = "group" if is_group else "user"
    raw = f"{tenant_id}:{channel_type}:{channel_id}:{scope}:{external_user_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def parse_webhook_path(path: str) -> tuple[Optional[str], str]:
    """解析 webhook path，返回 (channel_type, binding_id)。

    Args:
        path: 如 /webhook/wechat_work/demo__wecom

    Returns:
        (channel_type, binding_id)；无法解析时返回 (None, "")
    """
    match = _WEBHOOK_PATH_RE.match(path)
    if not match:
        return None, ""
    return match.group("channel"), match.group("binding")
