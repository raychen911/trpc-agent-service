"""T071 RED (e2e): minimal observable deployment boots and locates stages.

The overlay brings up core services whose /health/live and /health/ready
are ready, two nodes can continue the same tenant session, and the
Collector debug output locates Adapter/Gateway/Worker/official-Runner/data/
delivery stages by safe trace reference without test-sensitive markers
(FR-026, FR-034, SC-009, DEC-002, DEC-003).
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


async def test_overlay_boots_and_core_services_report_ready() -> None:
    overlay = await deploy.LocalObservableOverlay.up()
    try:
        for service in ("gateway", "worker-a", "worker-b", "collector"):
            live = await overlay.health_live(service)
            assert live == 200, f"{service} must be live"
        ready = await overlay.health_ready("gateway")
        assert ready == 200
        assert overlay.nodes() == ["gateway", "worker-a", "worker-b", "collector"]
    finally:
        await overlay.down()


async def test_two_nodes_continue_the_same_tenant_session() -> None:
    overlay = await deploy.LocalObservableOverlay.up()
    try:
        first = await overlay.send_message("worker-a", "tenant-alpha", "session-1", "hello")
        second = await overlay.send_message("worker-b", "tenant-alpha", "session-1", "follow up")
        assert first["session_id"] == second["session_id"]
        assert second["turn"] == first["turn"] + 1, (
            "the second node continues the same session transparently"
        )
    finally:
        await overlay.down()


async def test_collector_debug_locates_all_stages_by_safe_reference() -> None:
    overlay = await deploy.LocalObservableOverlay.up()
    try:
        result = await overlay.send_message("worker-a", "tenant-alpha", "session-1", "hello")
        trace_ref = result["trace_reference"]
        assert trace_ref.startswith("sha256:"), "only safe trace references"
        stages = await overlay.debug_stages(trace_ref)
        for expected in ("adapter", "gateway", "worker", "runner", "data", "delivery"):
            assert expected in stages, f"collector debug must locate the {expected} stage"
        rendered = repr(stages)
        for sentinel in ("api_key", "im_token", "db_password", "phone", "email", "message_body"):
            assert sentinel not in rendered, "debug output carries no test-sensitive markers"
    finally:
        await overlay.down()
