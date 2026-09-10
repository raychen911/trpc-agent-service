"""IM 用户权限校验（网关层 + Web 路由守卫）。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class UserBinding:
    tenant_id: str
    channel: str
    external_user_id: str
    session_id: str


class UserAuthzService:
    """用户-租户绑定表（内存版；SQL 持久化见 docs/data-model.md）。"""

    def __init__(self) -> None:
        # key: channel:external_user_id
        self._bindings: Dict[str, UserBinding] = {}

    @staticmethod
    def _key(channel: str, external_user_id: str) -> str:
        return f"{channel}:{external_user_id}"

    def bind(self, tenant_id: str, channel: str, external_user_id: str, session_id: str) -> UserBinding:
        binding = UserBinding(
            tenant_id=tenant_id,
            channel=channel,
            external_user_id=external_user_id,
            session_id=session_id,
        )
        self._bindings[self._key(channel, external_user_id)] = binding
        return binding

    def resolve(self, channel: str, external_user_id: str) -> Optional[UserBinding]:
        return self._bindings.get(self._key(channel, external_user_id))

    def unbind(self, channel: str, external_user_id: str) -> bool:
        return self._bindings.pop(self._key(channel, external_user_id), None) is not None

    def check(self, channel: str, external_user_id: str, tenant_id: str) -> bool:
        """校验外部用户是否已绑定到该租户。"""
        binding = self.resolve(channel, external_user_id)
        return binding is not None and binding.tenant_id == tenant_id


user_authz = UserAuthzService()
