# mypy: disable-error-code="import-untyped"
"""Runtime compatibility guard for the pinned tRPC-Agent-Python SDK.

The project intentionally integrates through public SDK symbols only.  This probe is
small enough to run at process startup and explicit enough to fail with an actionable
message when an unreviewed SDK upgrade changes a relied-on contract.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.version import __version__ as sdk_version

SUPPORTED_SDK_VERSION = "1.1.19"


class IncompatibleSdkError(RuntimeError):
    """The installed SDK does not satisfy the reviewed public API contract."""


@dataclass(frozen=True, slots=True)
class CompatibilityReport:
    """Result of checking the exact public surface used by this service."""

    sdk_version: str
    distribution_version: str | None
    issues: tuple[str, ...]

    @property
    def compatible(self) -> bool:
        """Return whether every reviewed compatibility check passed."""

        return not self.issues


def probe_sdk_compatibility() -> CompatibilityReport:
    """Inspect the installed SDK without constructing clients or using credentials."""

    issues: list[str] = []
    try:
        distribution_version = package_version("trpc-agent-py")
    except PackageNotFoundError:
        distribution_version = None
        issues.append("the trpc-agent-py distribution metadata is missing")

    if sdk_version != SUPPORTED_SDK_VERSION:
        issues.append(f"SDK reports version {sdk_version!r}; expected {SUPPORTED_SDK_VERSION!r}")
    if distribution_version not in {None, SUPPORTED_SDK_VERSION}:
        issues.append(
            "distribution version "
            f"{distribution_version!r} does not match {SUPPORTED_SDK_VERSION!r}"
        )

    _require_parameters(
        issues,
        "Runner.__init__",
        inspect.signature(Runner),
        {
            "app_name",
            "agent",
            "session_service",
            "memory_service",
            "enable_post_turn_processing",
            "close_session_service_on_close",
            "close_memory_service_on_close",
        },
    )
    _require_parameters(
        issues,
        "Runner.run_async",
        inspect.signature(Runner.run_async),
        {"user_id", "session_id", "new_message", "run_config", "agent_context"},
    )
    _require_parameters(
        issues,
        "FunctionTool.__init__",
        inspect.signature(FunctionTool),
        {"func", "filters_name", "filters"},
    )

    run_config_fields = set(RunConfig.model_fields)
    missing_limits = {
        "max_llm_calls",
        "max_iterations",
        "max_tool_calls",
        "custom_data",
        "streaming",
    } - run_config_fields
    if missing_limits:
        issues.append(f"RunConfig is missing fields: {sorted(missing_limits)}")

    agent_fields = set(LlmAgent.model_fields)
    missing_agent_fields = {"name", "model", "instruction", "tools", "filters"} - agent_fields
    if missing_agent_fields:
        issues.append(f"LlmAgent is missing fields: {sorted(missing_agent_fields)}")

    missing_event_methods = {
        name
        for name in (
            "get_function_calls",
            "get_function_responses",
            "is_error",
            "is_final_response",
        )
        if not callable(getattr(Event, name, None))
    }
    if missing_event_methods:
        issues.append(f"Event is missing methods: {sorted(missing_event_methods)}")

    return CompatibilityReport(
        sdk_version=sdk_version,
        distribution_version=distribution_version,
        issues=tuple(issues),
    )


def require_sdk_compatibility() -> CompatibilityReport:
    """Return a compatibility report or fail closed before serving traffic."""

    report = probe_sdk_compatibility()
    if not report.compatible:
        detail = "; ".join(report.issues)
        raise IncompatibleSdkError(f"unsupported tRPC-Agent-Python runtime: {detail}")
    return report


def _require_parameters(
    issues: list[str],
    label: str,
    signature: inspect.Signature,
    required: set[str],
) -> None:
    missing = required - set(signature.parameters)
    if missing:
        issues.append(f"{label} is missing parameters: {sorted(missing)}")
