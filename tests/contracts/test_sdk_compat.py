from __future__ import annotations

import asyncio
import inspect
from importlib.metadata import version

import pytest
from packaging.version import Version
from trpc_agent_sdk import cancel
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.tools.safety import ToolSafetyFilter, ToolSafetyGuard
from trpc_agent_sdk.types import Content, Part


def test_locked_sdk_version_and_public_runner_contract() -> None:
    assert version("trpc-agent-py") == "1.1.19"
    parameters = inspect.signature(Runner.run_async).parameters
    assert list(parameters) == [
        "self",
        "user_id",
        "session_id",
        "new_message",
        "run_config",
        "agent_context",
    ]


def test_agent_context_metadata_and_event_round_trip() -> None:
    context = AgentContext()
    context.with_metadata("tenant_id", "tenant")
    assert context.get_metadata("tenant_id") == "tenant"
    event = Event(
        invocation_id="invocation",
        author="agent",
        content=Content(parts=[Part(text="response")]),
    )
    restored = Event.model_validate_json(event.model_dump_json(by_alias=True))
    assert restored.get_text() == "response"
    assert restored.is_final_response()


def test_tool_safety_public_contract() -> None:
    assert hasattr(ToolSafetyGuard(), "check")
    assert inspect.signature(ToolSafetyFilter).parameters["audit_log_path"].default is None


@pytest.mark.asyncio
async def test_runner_cancellation_contract() -> None:
    runner = object.__new__(Runner)
    runner.app_name = "sdk-compat-cancellation"
    user_id = "compat-user"
    session_id = "compat-session"

    assert await runner.cancel_run_async(user_id, session_id, timeout=0.01) is False

    key = await cancel.register_run(runner.app_name, user_id, session_id)

    async def cleanup_after_cancel() -> None:
        await asyncio.sleep(0)
        await cancel.cleanup_run(runner.app_name, user_id, session_id)

    cleanup_task = asyncio.create_task(cleanup_after_cancel())
    assert await runner.cancel_run_async(user_id, session_id, timeout=0.5) is True
    await cleanup_task
    assert await cancel.is_run_cancelled(key) is False


def test_openclaw_transitive_security_floors() -> None:
    __import__("nanobot")
    assert Version(version("nanobot-ai")) >= Version("0.3.0")
    assert Version(version("dulwich")) >= Version("1.2.12")
    assert Version(version("pypdf")) >= Version("6.16.1")
