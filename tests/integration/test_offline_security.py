from __future__ import annotations

from tests.support import FIXED_UTC, inbound_message_data
from trpc_service.channels.contracts import InboundMessage
from trpc_service.web.app import build_runtime


async def test_success_path_is_offline_and_does_not_emit_secret_or_full_body(runtime_secret_env: dict[str, str], block_external_network, capsys) -> None:
    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    secret_values = tuple(runtime_secret_env.values())
    raw_text = "Remember validation token ALPHA."
    reply = await runtime.gateway.handle_verified_message_for_test(InboundMessage(**inbound_message_data(text=raw_text)))
    output = capsys.readouterr().out + capsys.readouterr().err
    audit_rendered = repr(await runtime.adapters.audit.list_by_tenant(runtime.tenant_scope("tenant-alpha")))
    assert reply.status.value == "succeeded"
    assert all(secret not in output + audit_rendered for secret in secret_values)
    assert raw_text not in output + audit_rendered
    assert runtime.worker.external_model_calls == 0
    await runtime.close()
