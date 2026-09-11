"""RED tests for WorkerEvent SSE encode/decode."""

from __future__ import annotations

import json
import uuid

import pytest

from trpc_service.transport.models import (
    WorkerErrorCode,
    WorkerEvent,
    WorkerToolCallData,
)
from trpc_service.transport.sse import decode_worker_event, encode_worker_event


class TestEncodeWorkerEvent:

    def test_encode_delta(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(protocol_version=1, request_id=rid, type="delta", data="hello", error_code=None)
        line = encode_worker_event(event)
        assert line.startswith("data: ")
        assert line.endswith("\n\n")
        payload = json.loads(line[6:-2])
        assert payload["protocol_version"] == 1
        assert payload["request_id"] == str(rid)
        assert payload["type"] == "delta"
        assert payload["data"] == "hello"
        assert payload["error_code"] is None

    def test_encode_tool_call(self) -> None:
        rid = uuid.uuid4()
        tool = WorkerToolCallData(kind="call", name="get_current_time", args={"tz": "UTC"})
        event = WorkerEvent(protocol_version=1, request_id=rid, type="tool", data=tool, error_code=None)
        line = encode_worker_event(event)
        payload = json.loads(line[6:-2])
        assert payload["data"]["kind"] == "call"
        assert payload["data"]["name"] == "get_current_time"
        assert payload["data"]["args"] == {"tz": "UTC"}

    def test_encode_done(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(protocol_version=1, request_id=rid, type="done", data=None, error_code=None)
        line = encode_worker_event(event)
        payload = json.loads(line[6:-2])
        assert payload["type"] == "done"
        assert payload["data"] is None

    def test_encode_error(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.MODEL_RUNTIME,
        )
        line = encode_worker_event(event)
        payload = json.loads(line[6:-2])
        assert payload["type"] == "error"
        assert payload["error_code"] == "model_runtime"
        assert payload["data"] is None

    def test_encode_unicode(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(protocol_version=1, request_id=rid, type="delta", data="你好世界", error_code=None)
        line = encode_worker_event(event)
        payload = json.loads(line[6:-2])
        assert payload["data"] == "你好世界"


class TestDecodeWorkerEvent:

    def test_decode_round_trip_delta(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(protocol_version=1, request_id=rid, type="delta", data="hello", error_code=None)
        line = encode_worker_event(event)
        decoded = decode_worker_event(line, expected_request_id=rid)
        assert decoded.type == "delta"
        assert decoded.data == "hello"
        assert decoded.request_id == rid

    def test_decode_round_trip_tool_call(self) -> None:
        rid = uuid.uuid4()
        tool = WorkerToolCallData(kind="call", name="tool_a", args={"k": "v"})
        event = WorkerEvent(protocol_version=1, request_id=rid, type="tool", data=tool, error_code=None)
        line = encode_worker_event(event)
        decoded = decode_worker_event(line, expected_request_id=rid)
        assert decoded.type == "tool"
        assert isinstance(decoded.data, WorkerToolCallData)
        assert decoded.data.name == "tool_a"

    def test_decode_round_trip_done(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(protocol_version=1, request_id=rid, type="done", data=None, error_code=None)
        line = encode_worker_event(event)
        decoded = decode_worker_event(line, expected_request_id=rid)
        assert decoded.type == "done"
        assert decoded.data is None

    def test_decode_round_trip_error(self) -> None:
        rid = uuid.uuid4()
        event = WorkerEvent(
            protocol_version=1,
            request_id=rid,
            type="error",
            data=None,
            error_code=WorkerErrorCode.WORKER_TIMEOUT,
        )
        line = encode_worker_event(event)
        decoded = decode_worker_event(line, expected_request_id=rid)
        assert decoded.type == "error"
        assert decoded.error_code == WorkerErrorCode.WORKER_TIMEOUT

    def test_rejects_malformed_json(self) -> None:
        rid = uuid.uuid4()
        with pytest.raises(Exception):
            decode_worker_event("data: {broken\n\n", expected_request_id=rid)

    def test_rejects_non_data_prefix(self) -> None:
        rid = uuid.uuid4()
        with pytest.raises(Exception):
            decode_worker_event("event: something\n\n", expected_request_id=rid)

    def test_rejects_wrong_request_id(self) -> None:
        rid = uuid.uuid4()
        other_rid = uuid.uuid4()
        event = WorkerEvent(protocol_version=1, request_id=rid, type="done", data=None, error_code=None)
        line = encode_worker_event(event)
        with pytest.raises(Exception):
            decode_worker_event(line, expected_request_id=other_rid)

    def test_rejects_wrong_protocol_version(self) -> None:
        rid = uuid.uuid4()
        payload = {
            "protocol_version": 99,
            "request_id": str(rid),
            "type": "done",
            "data": None,
            "error_code": None,
        }
        line = f"data: {json.dumps(payload)}\n\n"
        with pytest.raises(Exception):
            decode_worker_event(line, expected_request_id=rid)

    def test_rejects_unknown_fields(self) -> None:
        rid = uuid.uuid4()
        payload = {
            "protocol_version": 1,
            "request_id": str(rid),
            "type": "done",
            "data": None,
            "error_code": None,
            "extra": "bad",
        }
        line = f"data: {json.dumps(payload)}\n\n"
        with pytest.raises(Exception):
            decode_worker_event(line, expected_request_id=rid)


# ── Stage 6A2: approval event SSE roundtrip (RED) ───────────────────────────


def test_approval_event_roundtrip():
    import uuid

    from trpc_service.transport.models import WorkerApprovalData, WorkerEvent
    from trpc_service.transport.sse import decode_worker_event, encode_worker_event

    rid = uuid.uuid4()
    aid = uuid.uuid4()
    ev = WorkerEvent(
        protocol_version=1,
        request_id=rid,
        type="approval",
        data=WorkerApprovalData(approval_id=aid, tool_name="get_current_time"),
    )
    line = encode_worker_event(ev)
    back = decode_worker_event(line, expected_request_id=rid)
    assert back.type == "approval"
    assert back.data.approval_id == aid
    assert back.data.tool_name == "get_current_time"


def test_approval_event_decode_rejects_request_id_mismatch():
    import uuid

    from trpc_service.transport.models import WorkerApprovalData, WorkerEvent
    from trpc_service.transport.sse import decode_worker_event, encode_worker_event

    rid = uuid.uuid4()
    ev = WorkerEvent(
        protocol_version=1,
        request_id=rid,
        type="approval",
        data=WorkerApprovalData(approval_id=uuid.uuid4(), tool_name="t"),
    )
    line = encode_worker_event(ev)
    with pytest.raises(ValueError):
        decode_worker_event(line, expected_request_id=uuid.uuid4())


def test_approval_event_rejects_args_like_extra_keys():
    import json
    import uuid

    from pydantic import ValidationError
    from trpc_service.transport.sse import decode_worker_event

    rid = uuid.uuid4()
    tampered = {
        "protocol_version": 1,
        "request_id": str(rid),
        "type": "approval",
        "data": {
            "approval_id": str(uuid.uuid4()),
            "tool_name": "t",
            "args": {
                "secret": "leak"
            },
        },
        "error_code": None,
    }
    line = f"data: {json.dumps(tampered)}\n\n"
    with pytest.raises(ValidationError):
        decode_worker_event(line, expected_request_id=rid)
