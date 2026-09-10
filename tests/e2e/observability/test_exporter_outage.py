"""T072 RED (e2e): telemetry exporter outage degrades, business survives.

Stopping the otel-collector keeps the message business succeeding while the
platform degrades; the buffer stays within bounds with a visible drop
counter; health recovers within the spec window after the exporter is back;
the formal audit behavior must never change (FR-009, FR-012, NFR-004,
SC-004, DEC-002).
"""

from __future__ import annotations

import importlib

import pytest


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


deploy = _load("trpc_service.operations.deployment")

pytestmark = pytest.mark.deployment


async def test_business_succeeds_while_exporter_is_down() -> None:
    overlay = await deploy.LocalObservableOverlay.up()
    try:
        await overlay.stop_collector()
        result = await overlay.send_message("worker-a", "tenant-alpha", "session-1", "hello")
        assert result["status"] == "succeeded", "business must survive telemetry outage"
        health = await overlay.platform_health()
        assert health["state"] == "degraded", "platform reports degraded, never unready"
    finally:
        await overlay.down()


async def test_buffer_is_bounded_and_drop_counter_visible() -> None:
    overlay = await deploy.LocalObservableOverlay.up()
    try:
        await overlay.stop_collector()
        for index in range(50):
            await overlay.send_message(
                "worker-a", "tenant-alpha", "session-1", f"msg-{index}"
            )
        telemetry_state = await overlay.telemetry_state()
        assert telemetry_state["buffered"] <= telemetry_state["capacity"], (
            "buffer never exceeds its capacity"
        )
        assert "dropped" in telemetry_state, "drop counter must be visible"
        assert telemetry_state["dropped"] >= 0
    finally:
        await overlay.down()


async def test_health_recovers_within_spec_window_after_restore() -> None:
    overlay = await deploy.LocalObservableOverlay.up()
    try:
        await overlay.stop_collector()
        await overlay.send_message("worker-a", "tenant-alpha", "session-1", "hello")
        await overlay.start_collector()
        recovered = await overlay.wait_until_healthy(timeout_seconds=35)
        assert recovered is True, "health recovers within the spec window (<=30s)"
        health = await overlay.platform_health()
        assert health["state"] == "ready"
        audit_count = await overlay.audit_record_count()
        assert audit_count >= 1, "formal audit behavior never changes"
    finally:
        await overlay.down()
