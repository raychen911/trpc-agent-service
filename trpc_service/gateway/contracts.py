from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class NormalizedMessage:
    tenant_id: str
    agent_app_id: str
    channel: str
    account_id: str
    external_message_id: str
    sender_user_id: str
    conversation_id: str
    conversation_type: str
    text: str
    trace_id: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def session_id(self) -> str:
        if self.conversation_type == "group":
            return f"{self.channel}:{self.account_id}:group:{self.conversation_id}"
        return f"{self.channel}:{self.account_id}:direct:{self.sender_user_id}"

    @property
    def session_user_id(self) -> str:
        if self.conversation_type == "group":
            return f"group:{self.conversation_id}"
        return self.sender_user_id

    @property
    def route_key(self) -> str:
        return f"{self.tenant_id}:{self.agent_app_id}:{self.session_id}"


@dataclass(frozen=True, slots=True)
class AgentReply:
    text: str
    summary: str | None = None
    state_delta: Mapping[str, Any] = field(default_factory=dict)
    input_tokens: int = 0
    output_tokens: int = 0
    cost: float = 0
    attachments: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    card: Mapping[str, Any] | None = None
    stream_updates: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class GatewayResponse:
    status: str
    node_id: str
    session_id: str
    trace_id: str
    reply_text: str | None = None
    session_version: int | None = None
    delivery: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NodeRecord:
    node_id: str
    base_url: str
    capacity: int
    expires_at: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)


class NodeDirectory(Protocol):
    async def heartbeat(
        self,
        node_id: str,
        base_url: str,
        *,
        capacity: int,
        ttl_seconds: float,
        metadata: Mapping[str, Any] | None = None,
    ) -> NodeRecord: ...

    async def list_healthy(self) -> Sequence[NodeRecord]: ...

    async def unregister(self, node_id: str) -> None: ...


class AgentExecutor(Protocol):
    async def execute(self, message: NormalizedMessage) -> AgentReply: ...


class MessageHandler(Protocol):
    async def handle(self, message: NormalizedMessage, node_id: str) -> GatewayResponse: ...
