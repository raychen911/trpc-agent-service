"""OpenAI-compatible Responses API client.

Credentials are read from the environment and are never persisted by this
module. The raw HTTP implementation keeps the service independent of SDK
versions and works with compatible providers.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from trpc_service.security.secrets import SecretManager, redact_secret_text
from trpc_service.security.ssrf import validate_outbound_url
from trpc_service.tenant.models import ModelConfig


class ModelClientError(RuntimeError):
    pass


@dataclass(slots=True)
class ModelToolCall:
    call_id: str
    name: str
    arguments: dict[str, Any]
    arguments_error: str = ""


@dataclass(slots=True)
class ModelResponse:
    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    model: str | None = None
    tool_calls: list[ModelToolCall] = field(default_factory=list)
    response_id: str | None = None


@dataclass(slots=True)
class CodexCliModelClient:
    """Use the locally installed original Codex CLI as the model provider."""

    executable: str
    timeout_ms: int = 120_000
    cwd: str | None = None

    @classmethod
    def from_environment(cls, timeout_ms: int = 120_000) -> CodexCliModelClient:
        executable = os.getenv("CPA_CODEX_EXECUTABLE", "").strip() or shutil.which("codex.cmd") or shutil.which("codex")
        if not executable:
            raise ModelClientError("Codex CLI was not found; install the original codex-cli or unset CPA_USE_CODEX_CLI")
        return cls(
            executable=executable,
            timeout_ms=int(os.getenv("CPA_CODEX_TIMEOUT_MS", str(timeout_ms))),
            cwd=os.getenv("CPA_CODEX_CWD") or None,
        )

    def generate(
        self,
        model: str,
        system_prompt: str,
        conversation: list[dict[str, str]],
        temperature: float = 0.2,
        max_output_tokens: int = 1_024,
        tools: list[dict[str, Any]] | None = None,
    ) -> str:
        return self.generate_with_usage(
            model, system_prompt, conversation, temperature, max_output_tokens, tools
        ).text

    def generate_with_usage(
        self,
        model: str,
        system_prompt: str,
        conversation: list[dict[str, str]],
        temperature: float = 0.2,
        max_output_tokens: int = 1_024,
        tools: list[dict[str, Any]] | None = None,
    ) -> ModelResponse:
        del temperature, max_output_tokens, tools
        effective_model = model.strip() or os.getenv("CPA_MODEL", "").strip()
        if not effective_model:
            raise ModelClientError("CPA_MODEL is required when Codex CLI is enabled")

        prompt_parts = [
            "You are answering a user chat message through an Agent service.",
            "Do not call tools, modify files, or explain this instruction. Reply directly to the user.",
            f"System instruction:\n{system_prompt}",
            "Conversation:",
        ]
        for item in conversation:
            prompt_parts.append(f"{item['role']}: {item['content']}")
        prompt = "\n\n".join(prompt_parts)
        # Windows native subprocess argument/stdin encoding can turn non-ASCII
        # prompt text into question marks. Keep the transport ASCII-only and
        # let the model decode the original UTF-8 prompt.
        encoded_prompt = base64.b64encode(prompt.encode("utf-8")).decode("ascii")
        cli_prompt = (
            "Decode the following base64 UTF-8 text and follow it exactly. "
            "Do not mention decoding.\nBASE64_UTF8:\n" + encoded_prompt
        )

        output_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                prefix="trpc-agent-codex-",
                suffix=".txt",
                delete=False,
            ) as output_file:
                output_path = output_file.name
            command = [
                self.executable,
                "exec",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--skip-git-repo-check",
                "--output-last-message",
                output_path,
                "--model",
                effective_model,
                "-",
            ]
            completed = subprocess.run(
                command,
                input=cli_prompt,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=self.cwd,
                timeout=self.timeout_ms / 1000,
                check=False,
            )
            answer = Path(output_path).read_text(encoding="utf-8", errors="replace").strip()
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout).strip()
                raise ModelClientError(f"Codex CLI failed with exit code {completed.returncode}: {detail[:500]}")
            if not answer:
                raise ModelClientError("Codex CLI returned an empty response")
            estimated = len((prompt + answer).split())
            return ModelResponse(answer, estimated, len(answer.split()), estimated, effective_model)
        except subprocess.TimeoutExpired as exc:
            raise ModelClientError("Codex CLI request timed out") from exc
        except OSError as exc:
            raise ModelClientError(f"Codex CLI could not be started: {exc}") from exc
        finally:
            if output_path:
                try:
                    Path(output_path).unlink(missing_ok=True)
                except OSError:
                    pass


@dataclass(slots=True)
class ResponsesModelClient:
    base_url: str
    api_key: str
    timeout_ms: int = 60_000
    wire_api: str = "responses"
    max_retries: int = 3
    circuit_failures: int = 0
    circuit_open_until: float = 0.0

    @classmethod
    def from_config(
        cls,
        config: ModelConfig,
        secrets: SecretManager | None = None,
    ) -> ResponsesModelClient | CodexCliModelClient | None:
        if os.getenv("CPA_USE_CODEX_CLI", "0").strip().lower() in {"1", "true", "yes"}:
            return CodexCliModelClient.from_environment(config.timeout_ms)
        api_key = ""
        if config.api_key_ref:
            api_key = (secrets or SecretManager()).resolve(config.api_key_ref).strip()
        if not api_key:
            api_key = os.getenv(config.api_key_env, "").strip()
        if not api_key:
            return None
        base_url = config.base_url.strip() or os.getenv("CPA_BASE_URL", "").strip()
        if not base_url:
            raise ModelClientError("CPA_BASE_URL is required when real model authentication is enabled")
        return cls(
            base_url,
            api_key,
            config.timeout_ms,
            config.wire_api,
            int(os.getenv("CPA_MAX_RETRIES", "3")),
        )

    def generate(
        self,
        model: str,
        system_prompt: str,
        conversation: list[dict[str, str]],
        temperature: float = 0.2,
        max_output_tokens: int = 1_024,
        tools: list[dict[str, Any]] | None = None,
    ) -> str:
        return self.generate_with_usage(
            model, system_prompt, conversation, temperature, max_output_tokens, tools
        ).text

    def generate_with_usage(
        self,
        model: str,
        system_prompt: str,
        conversation: list[dict[str, str]],
        temperature: float = 0.2,
        max_output_tokens: int = 1_024,
        tools: list[dict[str, Any]] | None = None,
    ) -> ModelResponse:
        effective_model = model.strip() or os.getenv("CPA_MODEL", "").strip()
        if not effective_model:
            raise ModelClientError("CPA_MODEL is required when real model authentication is enabled")
        if self.wire_api.lower().replace("-", "_") in {"chat", "chat_completions", "chat.completions"}:
            return self._generate_chat_completions(
                effective_model,
                system_prompt,
                conversation,
                temperature,
                max_output_tokens,
                tools,
            )
        return self._generate_responses(
            effective_model,
            system_prompt,
            conversation,
            temperature,
            max_output_tokens,
            tools,
        )

    def _generate_responses(
        self,
        effective_model: str,
        system_prompt: str,
        conversation: list[dict[str, str]],
        temperature: float,
        max_output_tokens: int,
        tools: list[dict[str, Any]] | None,
    ) -> ModelResponse:
        input_items: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": system_prompt}],
            }
        ]
        for item in conversation:
            if item.get("role") == "assistant" and item.get("tool_calls"):
                for call in item["tool_calls"]:
                    function = call.get("function") or {}
                    input_items.append(
                        {
                            "type": "function_call",
                            "call_id": call.get("id"),
                            "name": function.get("name"),
                            "arguments": function.get("arguments", "{}"),
                        }
                    )
                continue
            if item.get("role") == "tool":
                input_items.append(
                    {
                        "type": "function_call_output",
                        "call_id": item.get("tool_call_id"),
                        "output": item.get("content", ""),
                    }
                )
                continue
            input_items.append(
                {
                    "role": item["role"],
                    "content": [{"type": "input_text", "text": item.get("content", "")}],
                }
            )
        payload = {
            "model": effective_model,
            "input": input_items,
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
        }
        if tools:
            payload["tools"] = tools
        request = Request(
            validate_outbound_url(self.base_url).rstrip("/") + "/responses",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        if monotonic() < self.circuit_open_until:
            raise ModelClientError("model provider circuit breaker is open")
        response_body = ""
        for attempt in range(self.max_retries + 1):
            try:
                with urlopen(request, timeout=self.timeout_ms / 1000) as response:
                    response_body = response.read().decode("utf-8")
                self.circuit_failures = 0
                break
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if retryable and attempt < self.max_retries:
                    sleep(min(8.0, 0.5 * (2**attempt)))
                    continue
                self._record_failure()
                raise ModelClientError(
                    redact_secret_text(f"model provider returned HTTP {exc.code}: {detail[:500]}")
                ) from exc
            except (URLError, TimeoutError) as exc:
                if attempt < self.max_retries:
                    sleep(min(8.0, 0.5 * (2**attempt)))
                    continue
                self._record_failure()
                detail = getattr(exc, "reason", "request timed out")
                raise ModelClientError(redact_secret_text(f"model provider connection failed: {detail}")) from exc

        try:
            response_json = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise ModelClientError("model provider returned invalid JSON") from exc
        text = extract_response_text(response_json)
        tool_calls = extract_response_tool_calls(response_json)
        if not text and not tool_calls:
            raise ModelClientError("model provider returned no output text")
        usage = response_json.get("usage") or {}
        input_tokens = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
        total_tokens = int(usage.get("total_tokens") or (input_tokens + output_tokens))
        if not total_tokens:
            input_tokens = len(json.dumps(input_items, ensure_ascii=False).split())
            output_tokens = len(text.split())
            total_tokens = input_tokens + output_tokens
        actual_model = response_json.get("model")
        actual_model = actual_model.strip() if isinstance(actual_model, str) else None
        return ModelResponse(
            text,
            input_tokens,
            output_tokens,
            total_tokens,
            actual_model,
            tool_calls,
            response_json.get("id") if isinstance(response_json.get("id"), str) else None,
        )

    def _generate_chat_completions(
        self,
        effective_model: str,
        system_prompt: str,
        conversation: list[dict[str, str]],
        temperature: float,
        max_output_tokens: int,
        tools: list[dict[str, Any]] | None,
    ) -> ModelResponse:
        messages: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]
        messages.extend(_chat_message(item) for item in conversation)
        payload = {
            "model": effective_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_output_tokens,
        }
        if tools:
            payload["tools"] = tools
        request = Request(
            validate_outbound_url(self.base_url).rstrip("/") + "/chat/completions",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        if monotonic() < self.circuit_open_until:
            raise ModelClientError("model provider circuit breaker is open")
        response_body = ""
        for attempt in range(self.max_retries + 1):
            try:
                with urlopen(request, timeout=self.timeout_ms / 1000) as response:
                    response_body = response.read().decode("utf-8")
                self.circuit_failures = 0
                break
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if retryable and attempt < self.max_retries:
                    sleep(min(8.0, 0.5 * (2**attempt)))
                    continue
                self._record_failure()
                raise ModelClientError(
                    redact_secret_text(f"model provider returned HTTP {exc.code}: {detail[:500]}")
                ) from exc
            except (URLError, TimeoutError) as exc:
                if attempt < self.max_retries:
                    sleep(min(8.0, 0.5 * (2**attempt)))
                    continue
                self._record_failure()
                detail = getattr(exc, "reason", "request timed out")
                raise ModelClientError(redact_secret_text(f"model provider connection failed: {detail}")) from exc

        try:
            response_json = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise ModelClientError("model provider returned invalid JSON") from exc
        choices = response_json.get("choices") or []
        text = ""
        tool_calls: list[ModelToolCall] = []
        if choices and isinstance(choices[0], dict):
            message = choices[0].get("message") or {}
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    text = content.strip()
                tool_calls = extract_chat_tool_calls(message)
        if not text and not tool_calls:
            raise ModelClientError("model provider returned no output text")
        usage = response_json.get("usage") or {}
        input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
        total_tokens = int(usage.get("total_tokens") or (input_tokens + output_tokens))
        if not total_tokens:
            input_tokens = len(json.dumps(messages, ensure_ascii=False).split())
            output_tokens = len(text.split())
            total_tokens = input_tokens + output_tokens
        actual_model = response_json.get("model")
        actual_model = actual_model.strip() if isinstance(actual_model, str) else None
        return ModelResponse(
            text,
            input_tokens,
            output_tokens,
            total_tokens,
            actual_model,
            tool_calls,
            response_json.get("id") if isinstance(response_json.get("id"), str) else None,
        )

    @staticmethod
    def usage(response: dict[str, Any]) -> tuple[int, float]:
        usage = response.get("usage") or {}
        tokens = int(usage.get("total_tokens") or usage.get("input_tokens", 0) + usage.get("output_tokens", 0) or 0)
        return tokens, 0.0

    def _record_failure(self) -> None:
        self.circuit_failures += 1
        if self.circuit_failures >= 5:
            self.circuit_open_until = monotonic() + 30.0


def extract_response_text(response: dict[str, Any]) -> str:
    direct = response.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    chunks: list[str] = []
    for item in response.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str):
                chunks.append(text)
            elif isinstance(text, dict) and isinstance(text.get("value"), str):
                chunks.append(text["value"])
    return "".join(chunks).strip()


def _parse_tool_arguments(raw: Any) -> tuple[dict[str, Any], str]:
    if isinstance(raw, dict):
        return raw, ""
    if not isinstance(raw, str):
        return {}, "tool arguments must be a JSON object"
    try:
        value = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        return {}, f"invalid tool arguments JSON: {exc.msg}"
    if not isinstance(value, dict):
        return {}, "tool arguments JSON must decode to an object"
    return value, ""


def extract_response_tool_calls(response: dict[str, Any]) -> list[ModelToolCall]:
    calls: list[ModelToolCall] = []
    for item in response.get("output", []) or []:
        if not isinstance(item, dict) or item.get("type") != "function_call":
            continue
        arguments, error = _parse_tool_arguments(item.get("arguments", "{}"))
        name = str(item.get("name", "")).strip()
        if not name:
            error = error or "tool call has no name"
        calls.append(
            ModelToolCall(
                str(item.get("call_id") or item.get("id") or ""),
                name,
                arguments,
                error,
            )
        )
    return calls


def extract_chat_tool_calls(message: dict[str, Any]) -> list[ModelToolCall]:
    calls: list[ModelToolCall] = []
    for item in message.get("tool_calls", []) or []:
        if not isinstance(item, dict):
            continue
        function = item.get("function") or {}
        if not isinstance(function, dict):
            function = {}
        arguments, error = _parse_tool_arguments(function.get("arguments", "{}"))
        name = str(function.get("name", "")).strip()
        if not name:
            error = error or "tool call has no name"
        calls.append(ModelToolCall(str(item.get("id") or ""), name, arguments, error))
    return calls


def _chat_message(item: dict[str, Any]) -> dict[str, Any]:
    message = {"role": item["role"], "content": item.get("content", "")}
    if item.get("tool_calls"):
        message["tool_calls"] = item["tool_calls"]
    if item.get("tool_call_id"):
        message["tool_call_id"] = item["tool_call_id"]
    return message
