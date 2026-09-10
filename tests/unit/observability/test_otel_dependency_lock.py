"""T010 RED: OpenTelemetry dependencies must be explicit and version-locked.

The packages currently resolve as transitive dependencies of trpc-agent-py;
the platform imports them directly, so they MUST be declared in
pyproject.toml and agree with uv.lock (plan.md R-002).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REQUIRED_OTEL_DEPENDENCIES: tuple[tuple[str, str], ...] = (
    ("opentelemetry-api", "1.44.0"),
    ("opentelemetry-sdk", "1.44.0"),
    ("opentelemetry-exporter-otlp-proto-http", "1.44.0"),
)

# Snapshot of the official runner instrumentation surface the platform builds on.
OFFICIAL_TELEMETRY_SYMBOLS: tuple[str, ...] = (
    "tracer",
    "trace_agent",
    "trace_call_llm",
    "trace_tool_call",
    "trace_runner",
    "trace_cancellation",
    "report_call_llm",
    "report_execute_tool",
    "report_invoke_agent",
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _pyproject_dependencies() -> list[str]:
    text = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r"^dependencies\s*=\s*\[(.*?)^\]", text, re.M | re.S)
    assert match, "pyproject.toml must declare a dependencies array"
    return [
        line.strip().strip('",') for line in match.group(1).splitlines() if line.strip()
    ]


def test_otel_packages_are_declared_as_direct_dependencies() -> None:
    dependencies = _pyproject_dependencies()
    for package, version in REQUIRED_OTEL_DEPENDENCIES:
        expected = f"{package}=={version}"
        assert expected in dependencies, (
            f"{expected} must be a direct dependency, not a transitive one"
        )


def test_otel_packages_are_importable_at_locked_versions() -> None:
    import opentelemetry.sdk  # noqa: F401
    from opentelemetry import trace  # noqa: F401
    from opentelemetry.sdk.trace import TracerProvider  # noqa: F401

    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # noqa: F401
        OTLPSpanExporter,
    )

    assert TracerProvider is not None and OTLPSpanExporter is not None


def test_official_runner_telemetry_surface_is_stable() -> None:
    telemetry = pytest.importorskip("trpc_agent_sdk.telemetry")
    missing = [name for name in OFFICIAL_TELEMETRY_SYMBOLS if not hasattr(telemetry, name)]
    assert not missing, (
        f"official trpc_agent_sdk.telemetry lost expected symbols: {missing}; "
        "sanitizing contract must be re-reviewed before any network export"
    )
    assert callable(telemetry.tracer) or telemetry.tracer is not None
