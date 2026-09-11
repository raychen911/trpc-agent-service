"""Stage 6A1 end-to-end governance flow on real PostgreSQL + Redis + 2 Workers.

Proves with a real model (``requires_model_and_docker``):
- channel/user admission denial returns the fixed 403 text, creates no
  receipt and performs zero Worker/Model execution;
- governance updates take effect per request (hot reload) via version bumps
  without restarting services;
- a ``review``/``deny`` tool policy blocks the tool function: the console SSE
  carries the structured governance verdict, never the real tool result;
- raw external identifiers never appear in service logs.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest

import urllib.error
import urllib.request

from tests.tenant_helpers import make_governance
from .service_topology import (
    ServiceTopology,
    requires_model_and_docker,
)

ACCESS_DENIED_TEXT = "Access is not allowed."


@pytest.fixture(scope="module")
def topology(tmp_path_factory):
    topo = ServiceTopology(tmp_path_factory.mktemp("stage6a1-gov"))
    topo.start()
    try:
        yield topo
    finally:
        topo.stop()


def _set_governance(topo: ServiceTopology, tenant_id: str, **gov_kwargs) -> int:
    """Write a new config version with the given governance through the
    production repository (optimistic concurrency against the head)."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from trpc_service.config.tenant import TenantConfigDraft
    from trpc_service.storage.tenant_repository import SqlTenantConfigRepository

    async def _do():
        repo = SqlTenantConfigRepository(create_async_engine(topo.pg_url))
        try:
            current = await repo.get(tenant_id)
            assert current is not None, f"{tenant_id} missing from seeded tenants"
            desired = TenantConfigDraft(
                enabled=current.enabled,
                app=current.app,
                governance=make_governance(**gov_kwargs),
                backend_profile=current.backend_profile,
                audit_policy=current.audit_policy,
            )
            updated = await repo.update(tenant_id, current.version, desired)
            return updated.version
        finally:
            await repo.close()

    return asyncio.run(_do())


def _console_post(topo: ServiceTopology, tenant_id: str, text_note: str, *, stream: bool = False):
    payload = {
        "tenant_id": tenant_id,
        "user_id": f"ext-user-{uuid.uuid4().hex[:10]}",
        "conversation_id": f"conv-{uuid.uuid4().hex[:8]}",
        "message_id": f"gov-{uuid.uuid4().hex[:12]}",
        "message": text_note,
    }
    path = "/api/console/messages/stream" if stream else "/api/console/messages"
    url = f"{topo.gateway_url}{path}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Tenant-ID": tenant_id,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            status = resp.status
            body = resp.read().decode()
            return {"status": status, "body": body, "payload": payload}
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "body": exc.read().decode(), "payload": payload}


def _receipt_count(topo: ServiceTopology, message_id: str) -> int:
    out = _pg(topo, f"SELECT COUNT(*) FROM message_receipts WHERE message_id='{message_id}'")
    return int(out)


def _pg(topo: ServiceTopology, sql: str) -> str:
    import subprocess

    result = subprocess.run(
        [
            "docker",
            "exec",
            topo.pg_container,
            "psql",
            "-U",
            topo.pg_user,
            "-d",
            topo.pg_db,
            "-t",
            "-A",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


@requires_model_and_docker
class TestStage6A1GovernanceFlow:

    def test_denied_channel_returns_403_zero_receipt_and_fixed_text(self, topology):
        _set_governance(topology, "tenant_a", allowed_channels=("web", ))
        result = _console_post(topology, "tenant_a", "hello governance")

        assert result["status"] == 403
        assert ACCESS_DENIED_TEXT in result["body"]
        assert _receipt_count(topology, result["payload"]["message_id"]) == 0
        assert result["payload"]["user_id"] not in result["body"]

    def test_denied_user_allowlist_zero_execution(self, topology):
        _set_governance(
            topology,
            "tenant_a",
            allowed_channels=("web_console", "web"),
            allowed_user_ids=("usr_v1_" + "c" * 48, ),
        )
        result = _console_post(topology, "tenant_a", "hello again")
        assert result["status"] == 403
        assert ACCESS_DENIED_TEXT in result["body"]
        assert _receipt_count(topology, result["payload"]["message_id"]) == 0

    def test_hot_update_takes_effect_without_restart(self, topology):
        """deny -> 403, then one version bump to allow -> 200 on the same
        running topology (per-request head read, no service restart)."""
        _set_governance(topology, "tenant_b", allowed_channels=("web", ))
        denied = _console_post(topology, "tenant_b", "hi")
        assert denied["status"] == 403

        _set_governance(topology, "tenant_b", allowed_channels=("web_console", "web"))
        allowed = _console_post(topology, "tenant_b", "现在几点？")
        assert allowed["status"] == 200
        # head row carries the newest policy version used by the successful call
        version = _pg(topology, "SELECT version FROM tenant_configs WHERE tenant_id='tenant_b'")
        assert int(version) >= 3

    def test_review_policy_blocks_tool_without_executing_it(self, topology):
        """6A2 contract update (supersedes the 6A1 event shape): with
        get_current_time under review the run PAUSES — SSE shows
        approval -> done with zero public tool events (review tool call and
        result are internal), and a pending approval row exists; the real
        tool output never appears."""
        _set_governance(
            topology,
            "tenant_default",
            allowed_channels=("web_console", "web", "wecom", "feishu"),
            tool_decisions={"get_current_time": "review"},
        )
        saw_pause = False
        for _attempt in range(3):
            result = _console_post(topology, "tenant_default", "现在几点了？请调用工具查询。", stream=True)
            assert result["status"] == 200
            events = []
            for line in result["body"].splitlines():
                if line.startswith("data: "):
                    events.append(json.loads(line[len("data: "):]))
            assert "approval_required" not in result["body"]
            types = [e["type"] for e in events]
            assert "tool" not in types, "review tool events must stay internal (6A2)"
            if "approval" in types:
                assert types[-1] == "done" and types[-2] == "approval"
                appr = next(e for e in events if e["type"] == "approval")
                assert appr["data"]["tool_name"] == "get_current_time"
                assert len(appr["data"]["approval_id"]) >= 32
                pending = _pg(
                    topology,
                    "SELECT count(*) FROM tool_approval_requests"
                    " WHERE tool_name = 'get_current_time' AND state = 'pending'",
                )
                assert int(pending) >= 1
                saw_pause = True
                break
        assert saw_pause, "real model never triggered the review pause across retries"

    def test_logs_never_contain_raw_external_identifiers(self, topology):
        _set_governance(topology, "tenant_a", allowed_channels=("web", ))
        marker_user = f"ext-user-{uuid.uuid4().hex[:12]}"
        marker_msg = f"gov-{uuid.uuid4().hex[:12]}"
        req = urllib.request.Request(
            f"{topology.gateway_url}/api/console/messages",
            data=json.dumps({
                "tenant_id": "tenant_a",
                "user_id": marker_user,
                "conversation_id": "conv-logcheck",
                "message_id": marker_msg,
                "message": "LOGCHECK-BODY",
            }).encode(),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Tenant-ID": "tenant_a"
            },
        )
        try:
            urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as exc:
            assert exc.code == 403

        # give the gateway a moment to flush; then read its captured log
        time.sleep(0.3)
        gateway_log = topology.gateway_log.read_text(errors="replace")
        assert marker_user not in gateway_log
        assert marker_msg not in gateway_log
        assert "LOGCHECK-BODY" not in gateway_log
