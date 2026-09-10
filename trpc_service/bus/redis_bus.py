"""Redis Stream execution bus backed by a SQL outbox record."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis

from trpc_service.agent.execution import AgentReply, RunAgentCommand, ToolEvent
from trpc_service.config.models import (
    ChannelType,
    ExecutionOutboxRecord,
    OutboxStatus,
)
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import ExecutionOutboxRepository


class RemoteExecutionError(RuntimeError):
    pass


class RedisExecutionBus:
    def __init__(
        self,
        database: Database,
        redis_url: str,
        *,
        stream: str = "trpc-service:agent-runs",
        result_timeout_seconds: float = 180.0,
    ) -> None:
        self._outbox = ExecutionOutboxRepository(database)
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._stream = stream
        self._result_timeout_seconds = result_timeout_seconds

    async def submit(self, command: RunAgentCommand) -> AgentReply:
        outbox_id = uuid4().hex
        await self._outbox.create(
            ExecutionOutboxRecord(
                outbox_id=outbox_id,
                trace_id=command.trace_id,
                tenant_id=command.tenant_id,
                payload=serialize_command(command),
            )
        )
        try:
            await self._redis.xadd(self._stream, {"outbox_id": outbox_id})
        except Exception as exc:
            await self._outbox.mark(
                outbox_id,
                OutboxStatus.PUBLISH_FAILED,
                error_type=type(exc).__name__,
            )
            raise

        result = await self._redis.blpop(
            result_key(outbox_id), timeout=self._result_timeout_seconds
        )
        if result is None:
            raise TimeoutError("queued agent execution timed out")
        payload = json.loads(result[1])
        if not payload.get("ok"):
            error_type = str(payload.get("error_type", "RemoteExecutionError"))
            raise RemoteExecutionError(f"worker execution failed: {error_type}")
        return deserialize_reply(payload["reply"])

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

    async def close(self) -> None:
        await self._redis.aclose()


def serialize_command(command: RunAgentCommand) -> dict[str, Any]:
    return {
        "tenant_id": command.tenant_id,
        "app_id": command.app_id,
        "user_id": command.user_id,
        "session_id": command.session_id,
        "message": command.message,
        "channel": command.channel.value,
        "trace_id": command.trace_id,
    }


def deserialize_command(payload: dict[str, Any]) -> RunAgentCommand:
    return RunAgentCommand(
        tenant_id=str(payload["tenant_id"]),
        app_id=str(payload["app_id"]),
        user_id=str(payload["user_id"]),
        session_id=str(payload["session_id"]),
        message=str(payload["message"]),
        channel=ChannelType(str(payload["channel"])),
        trace_id=str(payload["trace_id"]),
    )


def serialize_reply(reply: AgentReply) -> dict[str, Any]:
    return {
        "tenant_id": reply.tenant_id,
        "app_id": reply.app_id,
        "user_id": reply.user_id,
        "session_id": reply.session_id,
        "trace_id": reply.trace_id,
        "text": reply.text,
        "tool_events": [
            {"type": event.type, "name": event.name, "data": event.data}
            for event in reply.tool_events
        ],
    }


def deserialize_reply(payload: dict[str, Any]) -> AgentReply:
    return AgentReply(
        tenant_id=str(payload["tenant_id"]),
        app_id=str(payload["app_id"]),
        user_id=str(payload["user_id"]),
        session_id=str(payload["session_id"]),
        trace_id=str(payload["trace_id"]),
        text=str(payload["text"]),
        tool_events=tuple(
            ToolEvent(type=str(item["type"]), name=str(item["name"]), data=item.get("data"))
            for item in payload.get("tool_events", [])
        ),
    )


def result_key(outbox_id: str) -> str:
    return f"trpc-service:agent-result:{outbox_id}"


def encode_result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


__all__ = [
    "RedisExecutionBus",
    "RemoteExecutionError",
    "deserialize_command",
    "deserialize_reply",
    "encode_result",
    "result_key",
    "serialize_command",
    "serialize_reply",
]
