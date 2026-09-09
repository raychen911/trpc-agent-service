"""Canonical records crossing storage backends during a migration."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from trpc_agent_sdk.events import Event


def _digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _event_json(event: Event) -> dict[str, Any]:
    value = event.model_dump(mode="json", exclude_none=True)
    # PostgreSQL stores timestamps at microsecond precision. Canonical hashes
    # compare what both supported backends can faithfully represent.
    if isinstance(value.get("timestamp"), (int, float)):
        value["timestamp"] = round(float(value["timestamp"]), 6)
    # SQL reconstructs both NULL and an empty set as an empty set. They have
    # identical SDK behavior and therefore share one canonical representation.
    if not value.get("long_running_tool_ids"):
        value.pop("long_running_tool_ids", None)
    return value


class SessionSnapshot(BaseModel):
    """Lossless SDK Session representation plus separately stored state."""

    model_config = ConfigDict(extra="forbid")

    app_name: str
    user_id: str
    session_id: str
    session_state: dict[str, Any] = Field(default_factory=dict)
    app_state: dict[str, Any] = Field(default_factory=dict)
    user_state: dict[str, Any] = Field(default_factory=dict)
    events: list[Event] = Field(default_factory=list)
    historical_events: list[Event] = Field(default_factory=list)
    conversation_count: int = 0
    last_update_time: float = 0.0
    remaining_ttl_seconds: int = -1
    app_state_remaining_ttl_seconds: int = -1
    user_state_remaining_ttl_seconds: int = -1
    source_updated_at: float = 0.0
    storage_layout_version: str = "trpc-agent-py/1.1.19"
    content_hash: str = ""

    def canonical(self) -> dict[str, Any]:
        return {
            "app_name": self.app_name,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "session_state": self.session_state,
            "app_state": self.app_state,
            "user_state": self.user_state,
            "events": [_event_json(event) for event in self.events],
            "historical_events": [_event_json(event) for event in self.historical_events],
            "conversation_count": self.conversation_count,
        }

    def calculate_hash(self) -> str:
        return _digest(self.canonical())

    def seal(self) -> "SessionSnapshot":
        self.content_hash = self.calculate_hash()
        return self


class MemorySnapshot(BaseModel):
    """Behavior-equivalent SDK Memory projection."""

    model_config = ConfigDict(extra="forbid")

    save_key: str
    session_id: str
    events: list[Event] = Field(default_factory=list)
    remaining_ttl_seconds: int = -1
    source_updated_at: float = 0.0
    storage_layout_version: str = "trpc-agent-py/1.1.19"
    content_hash: str = ""

    def canonical(self) -> dict[str, Any]:
        return {
            "save_key": self.save_key,
            "session_id": self.session_id,
            "events": [_event_json(event) for event in self.events],
        }

    def calculate_hash(self) -> str:
        return _digest(self.canonical())

    def seal(self) -> "MemorySnapshot":
        self.content_hash = self.calculate_hash()
        return self


class MigrationBatchResult(BaseModel):
    """Bounded work result persisted before another batch starts."""

    model_config = ConfigDict(extra="forbid")

    cursor: dict[str, Any] = Field(default_factory=dict)
    read_count: int = 0
    written_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    phase_complete: bool = False
    source_hash: str = ""
    target_hash: str = ""
    error_keys: list[str] = Field(default_factory=list)
