"""SSE encode/decode for WorkerEvent."""

from __future__ import annotations

import json
import uuid

from trpc_service.transport.models import WorkerEvent


def encode_worker_event(event: WorkerEvent) -> str:
    payload = event.model_dump(mode="json")
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def decode_worker_event(line: str, *, expected_request_id: uuid.UUID) -> WorkerEvent:
    if not line.startswith("data: "):
        raise ValueError("SSE line must start with 'data: '")
    json_str = line[len("data: "):].rstrip("\n")
    raw = json.loads(json_str)
    event = WorkerEvent.model_validate(raw)
    if event.request_id != expected_request_id:
        raise ValueError("request_id mismatch")
    return event


__all__ = ["decode_worker_event", "encode_worker_event"]
