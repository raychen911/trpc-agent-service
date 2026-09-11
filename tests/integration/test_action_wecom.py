"""WeCom confirmation result uses the new callback, without finishing it prematurely."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import select
from wecom_aibot_sdk.types.api import WsFrame

from tests.reliability.test_im_ingress import register_test_binding, grant
from tests.reliability.test_actions import proposal, command, spec, worker
from trpc_service.channels.persistence import IMAccounts, IMIngress, ContextCipher
from trpc_service.channels.wecom import WecomAdapter
from trpc_service.governance.actions import ActionService
from trpc_service.governance.action_delivery import ActionNoticeWorker
from trpc_service.persistence import Database
from trpc_service.persistence.models import IMMembershipRow, TenantConfigVersionRow, InboundMessageRow, ActionNoticeRow
from trpc_service.tenant import SessionIdentityFactory, MessageRouter, ChannelBindingRegistry


@pytest.mark.asyncio
async def test_confirming_callback_is_reserved_for_action_result(tmp_path, tenant):
    database = Database(f"sqlite:///{tmp_path / 'wecom-action.db'}")
    database.create_schema()
    binding = tenant.channel_bindings[0]
    register_test_binding(database, tenant, binding)
    identities = SessionIdentityFactory(lambda _: b"k" * 32)
    router = MessageRouter(ChannelBindingRegistry([tenant]), identities)
    cipher = ContextCipher([Fernet.generate_key()])
    ingress = IMIngress(database, cipher, identities)
    lease = IMAccounts(database).acquire(binding, "owner")

    async def reply(headers, stream_id, text, finish):
        return WsFrame(headers=headers, errcode=0)

    client = SimpleNamespace(is_authenticated=True, reply_stream=AsyncMock(side_effect=reply))
    adapter = WecomAdapter(client)

    def event(identity, text):
        return adapter.normalize(
            WsFrame(cmd="aibot_msg_callback",
                    headers={"req_id": identity},
                    body={
                        "aibotid": binding.external_account_id,
                        "msgid": identity,
                        "from": {
                            "userid": "synthetic-user"
                        },
                        "chattype": "single",
                        "msgtype": "text",
                        "text": {
                            "content": text
                        },
                    }), binding)

    try:
        original = event("original-request", "request refund")
        route = router.route_message(original.message)
        grant(database, route)
        with database.sessions.begin() as session:
            session.get(IMMembershipRow,
                        (tenant.tenant_id, binding.binding_id, route.actor_id)).grants_json = ["chat", "approve"]
            session.add(
                TenantConfigVersionRow(tenant_id=tenant.tenant_id,
                                       config_version=tenant.config_version,
                                       status="published",
                                       content_hash="test",
                                       config_json=tenant.model_dump(mode="json"),
                                       created_by="test"))
        receipt = ingress.record(original, binding, lease, route=route)
        with database.sessions() as session:
            execution = session.get(InboundMessageRow, receipt.inbound_id).execution_id
        service = ActionService(database, cipher)
        action = proposal(service, route, execution)
        notices = ActionNoticeWorker(database, lease, adapter, cipher)
        assert await notices.run_once()
        assert client.reply_stream.call_args.args[0] == {"req_id": "original-request"}
        confirmation = event("approval-request", "/action " + command(service, action["action_id"]))
        assert ingress.record(confirmation, binding, lease).disposition == "action_approved"
        assert not await notices.run_once()  # No early final acknowledgement.
        assert await worker(service, route, spec()).run_once()
        assert await notices.run_once()
        assert client.reply_stream.await_count == 2
        assert client.reply_stream.call_args.args[0] == {"req_id": "approval-request"}
        assert "已执行成功" in client.reply_stream.call_args.args[2]
        with database.sessions() as session:
            assert all(row.status == "delivered" for row in session.scalars(select(ActionNoticeRow)))
    finally:
        database.close()
