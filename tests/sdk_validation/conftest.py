"""Shared safety fixtures for the SDK validation test suite."""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest


MODEL_CREDENTIAL_ENV_VARS = (
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY",
    "GEMINI_API_KEY",
    "DEEPSEEK_API_KEY",
    "AZURE_OPENAI_API_KEY",
)


@pytest.fixture(autouse=True)
def offline_validation_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Remove model credentials and fail every attempted outbound socket call."""

    for variable in MODEL_CREDENTIAL_ENV_VARS:
        monkeypatch.delenv(variable, raising=False)

    blocked_calls: list[str] = []

    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_create_connection = socket.create_connection

    def is_loopback(address: object) -> bool:
        if isinstance(address, tuple) and address:
            return str(address[0]).lower() in {"127.0.0.1", "::1", "localhost"}
        return False

    def guarded_connect(sock: socket.socket, address: object) -> object:
        if is_loopback(address):
            return original_connect(sock, address)  # type: ignore[arg-type]
        blocked_calls.append(str(address))
        raise AssertionError("external socket access is forbidden during SDK validation")

    def guarded_connect_ex(sock: socket.socket, address: object) -> int:
        if is_loopback(address):
            return original_connect_ex(sock, address)  # type: ignore[arg-type]
        blocked_calls.append(str(address))
        raise AssertionError("external socket access is forbidden during SDK validation")

    def guarded_create_connection(address: object, *args: object, **kwargs: object) -> socket.socket:
        if is_loopback(address):
            return original_create_connection(address, *args, **kwargs)  # type: ignore[arg-type]
        blocked_calls.append(str(address))
        raise AssertionError("external socket access is forbidden during SDK validation")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    monkeypatch.setattr(socket, "create_connection", guarded_create_connection)
    yield blocked_calls
