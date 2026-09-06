"""Offline coverage for model protocol, retry, and tool-call behavior."""

from __future__ import annotations

import json
from io import BytesIO
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import pytest

from trpc_service.agent.model_client import (
    CodexCliModelClient,
    ModelClientError,
    ResponsesModelClient,
    extract_chat_tool_calls,
    extract_response_text,
    extract_response_tool_calls,
)
from trpc_service.tenant.models import ModelConfig


class Response:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.payload


def test_response_and_chat_parsers_cover_text_tools_and_invalid_arguments():
    assert extract_response_text({"output_text": " direct "}) == "direct"
    assert extract_response_text({"output": [{"content": [{"text": "a"}, {"text": {"value": "b"}}]}]}) == "ab"
    assert extract_response_text({"output": [None, {"content": [None]}]}) == ""
    response_calls = extract_response_tool_calls({"output": [None, {"type": "message"}, {"type": "function_call", "id": "c", "name": "tool", "arguments": "{\"x\":1}"}, {"type": "function_call", "call_id": "bad", "arguments": "[]"}]})
    assert response_calls[0].arguments == {"x": 1}
    assert response_calls[1].arguments_error
    chat_calls = extract_chat_tool_calls({"tool_calls": [None, {"id": "c", "function": {"name": "tool", "arguments": {"x": 1}}}, {"id": "bad", "function": {"name": "", "arguments": "not-json"}}]})
    assert chat_calls[0].name == "tool" and chat_calls[1].arguments_error


def test_model_client_from_config_and_responses_request(monkeypatch):
    config = ModelConfig("provider", "model", base_url="https://api.example", api_key_env="TEST_MODEL_KEY")
    assert ResponsesModelClient.from_config(config) is None
    monkeypatch.setenv("TEST_MODEL_KEY", "key")
    client = ResponsesModelClient.from_config(config)
    assert client is not None and client.api_key == "key"
    response = {"id": "resp-1", "model": " model ", "output_text": "answer", "usage": {"input_tokens": 2, "output_tokens": 3}}
    requests = []

    def open_response(request, timeout):
        requests.append((request, timeout))
        return Response(response)

    monkeypatch.setattr("trpc_service.agent.model_client.urlopen", open_response)
    result = client.generate_with_usage(
        "model", "system", [
            {"role": "assistant", "content": "old", "tool_calls": [{"id": "call", "function": {"name": "lookup", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "call", "content": "tool result"},
            {"role": "user", "content": "question"},
        ],
        tools=[{"type": "function"}],
    )
    assert result.text == "answer" and result.total_tokens == 5
    sent = json.loads(requests[0][0].data)
    assert sent["input"][1]["type"] == "function_call" and sent["tools"]


def test_chat_retry_errors_and_circuit_breaker(monkeypatch):
    client = ResponsesModelClient("https://api.example", "secret", wire_api="chat", max_retries=1)
    success = Response({"id": "chat-1", "model": "model", "choices": [{"message": {"content": "hello", "tool_calls": [{"id": "c", "function": {"name": "tool", "arguments": "{}"}}]}}], "usage": {"prompt_tokens": 1, "completion_tokens": 2}})
    calls = [HTTPError("url", 500, "error", {}, BytesIO(b'{"error":"temporary"}')), success]

    def open_with_retry(*args, **kwargs):
        value = calls.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr("trpc_service.agent.model_client.urlopen", open_with_retry)
    monkeypatch.setattr("trpc_service.agent.model_client.sleep", lambda seconds: None)
    result = client.generate_with_usage("model", "system", [{"role": "user", "content": "hi"}])
    assert result.text == "hello" and result.tool_calls[0].name == "tool"

    client.max_retries = 0
    error_body = BytesIO(b"api_key=secret")
    monkeypatch.setattr("trpc_service.agent.model_client.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(HTTPError("url", 400, "error", {}, error_body)))
    with pytest.raises(ModelClientError, match="HTTP 400"):
        client.generate("model", "system", [])
    monkeypatch.setattr("trpc_service.agent.model_client.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(URLError("offline")))
    with pytest.raises(ModelClientError, match="connection failed"):
        client.generate("model", "system", [])
    client.circuit_failures = 4
    client._record_failure()
    with pytest.raises(ModelClientError, match="circuit breaker"):
        client.generate("model", "system", [])


def test_model_client_rejects_empty_or_invalid_provider_output(monkeypatch):
    client = ResponsesModelClient("https://api.example", "key", max_retries=0)
    monkeypatch.delenv("CPA_MODEL", raising=False)
    for payload, message in ((b"not-json", "invalid JSON"), (b'{"output": []}', "no output")):
        class RawResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return payload

        monkeypatch.setattr("trpc_service.agent.model_client.urlopen", lambda *args, **kwargs: RawResponse())
        with pytest.raises(ModelClientError, match=message):
            client.generate("model", "system", [])
    with pytest.raises(ModelClientError, match="CPA_MODEL"):
        client.generate("", "system", [])


def test_codex_cli_client_builds_ascii_prompt_and_cleans_output(monkeypatch):
    monkeypatch.setenv("CPA_CODEX_EXECUTABLE", "codex")
    monkeypatch.setenv("CPA_MODEL", "local-model")
    client = CodexCliModelClient.from_environment()

    def run(command, **kwargs):
        output_path = command[command.index("--output-last-message") + 1]
        with open(output_path, "w", encoding="utf-8") as output:
            output.write("answer")
        assert kwargs["input"].isascii()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("trpc_service.agent.model_client.subprocess.run", run)
    result = client.generate_with_usage("", "system", [{"role": "user", "content": "你好"}])
    assert result.text == "answer" and result.model == "local-model"

    monkeypatch.setattr("trpc_service.agent.model_client.subprocess.run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="failed"))
    with pytest.raises(ModelClientError, match="exit code"):
        client.generate("local-model", "system", [])
