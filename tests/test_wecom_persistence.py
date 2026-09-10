"""企微通道平台表接线测试：channel_binding 写入 + idempotency 第三层幂等兜底。"""
import base64
import os
import time

import pytest

from trpc_service.agent.runner import RunResult
from trpc_service.channels import wecom_crypto
from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.wecom import WeComAdapter
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig
from trpc_service.tenant.storage.database import Database
from trpc_service.tenant.storage.tables import ChannelBindingRow, IdempotencyRow


@pytest.fixture()
def db(tmp_path):
    url = f"sqlite:///{(tmp_path / 'wecom.db').as_posix()}"
    database = Database(url)
    database.create_all()
    yield database
    database.dispose()


def _make_aes_key() -> str:
    return base64.b64encode(os.urandom(32)).decode()[:43]


def _wrap(encrypt: str, token: str, nonce: str) -> tuple[dict, str]:
    ts = str(int(time.time()))
    sig = wecom_crypto.sha1_signature(token, ts, nonce, encrypt)
    body = (
        "<xml>"
        f"<Encrypt><![CDATA[{encrypt}]]></Encrypt>"
        f"<MsgSignature><![CDATA[{sig}]]></MsgSignature>"
        f"<TimeStamp>{ts}</TimeStamp>"
        f"<Nonce><![CDATA[{nonce}]]></Nonce>"
        "</xml>"
    )
    return {"msg_signature": sig, "timestamp": ts, "nonce": nonce}, body


def _inner_message(user: str, content: str, msg_id: str, chat_id: str = "") -> str:
    chat = f"<ChatId><![CDATA[{chat_id}]]></ChatId>" if chat_id else ""
    return (
        "<xml>"
        "<ToUserName><![CDATA[corp]]></ToUserName>"
        f"<FromUserName><![CDATA[{user}]]></FromUserName>"
        f"<CreateTime>{int(time.time())}</CreateTime>"
        "<MsgType><![CDATA[text]]></MsgType>"
        f"<Content><![CDATA[{content}]]></Content>"
        f"{chat}"
        f"<MsgId>{msg_id}</MsgId>"
        "</xml>"
    )


class FakeRunner:
    app_name = "customer_service"

    def __init__(self):
        self.calls = []

    async def run(self, user_id, session_id, message, files=None, agent_context=None):
        self.calls.append({"session_id": session_id, "message": message})
        return RunResult(text="ok")


def _make_tenant() -> tuple[TenantConfig, FakeRunner]:
    token, aes_key = _make_aes_key(), _make_aes_key()
    tenant = TenantConfig(
        tenant_id="tenant_wecom_db",
        name="接线测试租户",
        channels={
            "wecom": ChannelConfig(
                enabled=True, token=token, encoding_aes_key=aes_key,
                corp_id="corp_abc", bot_id="bot_001",
            )
        },
    )
    return tenant, FakeRunner()


def _build_adapter(tenant: TenantConfig, runner: FakeRunner, db, deduper=None) -> WeComAdapter:
    return WeComAdapter(
        tenant, tenant.channels["wecom"],
        runner_getter=lambda _tid: runner,
        deduper=deduper, database=db,
    )


async def test_binding_and_idempotency_written(db):
    """消息处理后：channel_binding 落一行 + idempotency 落幂等键。"""
    tenant, runner = _make_tenant()
    adapter = _build_adapter(tenant, runner, db)
    nonce = "n1"
    encrypt = wecom_crypto.encrypt_message(
        adapter.channel_config.encoding_aes_key,
        _inner_message("userX", "你好", "90001", chat_id="room5"),
        "corp_abc",
    )
    query, body = _wrap(encrypt, adapter.channel_config.token, nonce)
    resp = await adapter.handle_webhook(
        "tenant_wecom_db", WebhookRequest(method="POST", query=query, body=body)
    )
    assert resp.status_code == 200

    with db.session() as s:
        bindings = s.query(ChannelBindingRow).all()
        assert len(bindings) == 1
        assert bindings[0].tenant_id == "tenant_wecom_db"
        assert bindings[0].channel_type == "wecom"
        assert bindings[0].external_user_id == "userX"
        assert bindings[0].chat_id == "room5"
        assert bindings[0].session_id
        idems = s.query(IdempotencyRow).all()
        assert [i.idempotency_key for i in idems] == ["wecom:90001"]


async def test_sql_idempotency_rejects_after_restart(db):
    """进程内去重状态丢失（模拟重启）后，SQL 唯一索引仍拦截重复投递。"""
    tenant, runner = _make_tenant()
    adapter1 = _build_adapter(tenant, runner, db)
    nonce = "n2"
    encrypt = wecom_crypto.encrypt_message(
        adapter1.channel_config.encoding_aes_key,
        _inner_message("userY", "再发一次", "90002"),
        "corp_abc",
    )
    query, body = _wrap(encrypt, adapter1.channel_config.token, nonce)
    req = WebhookRequest(method="POST", query=query, body=body)
    await adapter1.handle_webhook("tenant_wecom_db", req)

    # 新 adapter（去重缓存为空），共享同一租户凭证与数据库 → 第三层兜底拦截
    adapter2 = _build_adapter(tenant, runner, db)
    resp2 = await adapter2.handle_webhook("tenant_wecom_db", req)
    assert resp2.body == "success"
    assert len(runner.calls) == 1


async def test_sql_failure_does_not_block_message(tmp_path):
    """数据库不可用时消息主链路照常（best-effort 落库，不阻塞会话）。"""
    bad_db = Database(f"sqlite:///{(tmp_path / 'no_dir' / 'x.db').as_posix()}")
    tenant, runner = _make_tenant()
    adapter = _build_adapter(tenant, runner, bad_db)
    nonce = "n3"
    encrypt = wecom_crypto.encrypt_message(
        adapter.channel_config.encoding_aes_key,
        _inner_message("userZ", "数据库挂了还能聊吗", "90003"),
        "corp_abc",
    )
    query, body = _wrap(encrypt, adapter.channel_config.token, nonce)
    resp = await adapter.handle_webhook(
        "tenant_wecom_db", WebhookRequest(method="POST", query=query, body=body)
    )
    assert resp.status_code == 200
    assert resp.delivered_reply == "ok"
    bad_db.dispose()
