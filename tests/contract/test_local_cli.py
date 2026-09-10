from __future__ import annotations

import json
import secrets
from types import SimpleNamespace

import pytest

from trpc_service import _cli


def test_send_request_signs_exact_json_bytes_and_never_returns_secret() -> None:
    args = SimpleNamespace(
        url="http://127.0.0.1:8000",
        binding_id="binding-alpha",
        secret_env="RUNTIME_SECRET",
        external_message_id="cli-001",
        external_user_id="user-001",
        conversation_type="direct",
        external_conversation_id="conversation-001",
        text="Remember validation token ALPHA.",
        trace_id=None,
    )
    runtime_secret = secrets.token_urlsafe(32)
    request = _cli.build_signed_request(args, {"RUNTIME_SECRET": runtime_secret}, timestamp=1788595200)
    assert json.loads(request.content)["external_message_id"] == "cli-001"
    assert request.headers["x-signature"].startswith("v1=")
    assert runtime_secret not in repr(request)
    assert request.headers["x-signature"] not in repr(request)
    assert request.content.decode() not in repr(request)
    assert request.url == "http://127.0.0.1:8000/v1/local/messages"


def test_send_request_requires_nonempty_secret() -> None:
    args = SimpleNamespace(secret_env="MISSING")
    with pytest.raises(SystemExit, match="unavailable"):
        _cli.build_signed_request(args, {}, timestamp=1788595200)


def test_send_request_rejects_non_loopback_destination() -> None:
    args = SimpleNamespace(
        url="https://example.invalid", binding_id="binding-alpha", secret_env="RUNTIME_SECRET",
        external_message_id="cli-001", external_user_id="user-001", conversation_type="direct",
        external_conversation_id="conversation-001", text="hello", trace_id=None,
    )
    with pytest.raises(SystemExit, match="loopback"):
        _cli.build_signed_request(args, {"RUNTIME_SECRET": secrets.token_urlsafe(32)}, timestamp=1788595200)


def test_send_main_maps_invalid_server_response_without_leaking_secret(monkeypatch, capsys) -> None:
    runtime_secret = secrets.token_urlsafe(32)
    monkeypatch.setenv("CLI_RUNTIME_SECRET", runtime_secret)

    class Response:
        status_code = 200
        def json(self):
            raise ValueError("untrusted response")

    monkeypatch.setattr(_cli, "_post_local", lambda _request: Response())
    result = _cli.send_main([
        "--binding-id", "binding-alpha", "--secret-env", "CLI_RUNTIME_SECRET",
        "--external-message-id", "cli-001", "--external-user-id", "user-001",
        "--conversation-type", "direct", "--external-conversation-id", "conversation-001",
        "--text", "hello",
    ])
    output = capsys.readouterr().out
    assert result == 2 and "invalid_response" in output
    assert runtime_secret not in output
