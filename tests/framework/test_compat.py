# mypy: disable-error-code="import-untyped"
"""Version and public-surface compatibility probe tests."""

import pytest
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.tools import FunctionTool

from trpc_service.agent import compat
from trpc_service.agent.compat import (
    SUPPORTED_SDK_VERSION,
    CompatibilityReport,
    IncompatibleSdkError,
    probe_sdk_compatibility,
    require_sdk_compatibility,
)


def test_exact_sdk_public_surface_is_compatible() -> None:
    report = require_sdk_compatibility()

    assert report.compatible
    assert report.sdk_version == report.distribution_version == SUPPORTED_SDK_VERSION
    assert all(symbol is not None for symbol in (LlmAgent, RunConfig, Event, Runner, FunctionTool))


def test_probe_is_pure_and_repeatable() -> None:
    assert probe_sdk_compatibility() == probe_sdk_compatibility()


def test_require_probe_fails_closed_with_actionable_issues(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        compat,
        "probe_sdk_compatibility",
        lambda: CompatibilityReport("2.0.0", "2.0.0", ("Runner.run_async changed",)),
    )

    with pytest.raises(IncompatibleSdkError, match=r"Runner\.run_async changed"):
        require_sdk_compatibility()


def test_probe_detects_reported_version_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compat, "sdk_version", "2.0.0")

    report = probe_sdk_compatibility()

    assert not report.compatible
    assert any("SDK reports version" in issue for issue in report.issues)
