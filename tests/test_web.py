# web 模块单元测试（FastAPI TestClient）
import asyncio
import hashlib

import pytest
from fastapi.testclient import TestClient

from trpc_service.channels import ChannelFactory
from trpc_service.channels.wechat_work import _aes_encrypt, derive_aes_key
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import TenantRegistry
from trpc_service.web import build_admin_app, build_gateway_app


async def _load_tenant(tid):
    if tid == "demo":
        return {
            "tenant_id": "demo",
            "name": "演示租户",
            "status": "active",
            "app": {
                "system_prompt": "演示助手"
            },
            "model": {
                "model_name": "mock"
            },
            "tools": {
                "allowlist": ["echo", "calculator"]
            },
            "im": [{
                "channel_type": "web",
                "webhook_path": ""
            }],
            "rate_limit_per_min": 60,
        }
    if tid == "w1":
        return {
            "tenant_id":
            "w1",
            "name":
            "企微租户",
            "status":
            "active",
            "app": {
                "system_prompt": "演示助手"
            },
            "model": {
                "model_name": "mock"
            },
            "tools": {
                "allowlist": ["echo"]
            },
            "im": [{
                "channel_type": "wechat_work",
                "webhook_path": "/webhook/wechat_work/w1__cb1",
                "app_id": "corp1",
                "agent_id": "1000002",
                "token_ref": "wx-token",
                "aes_key_ref": "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C",
                "user_id_mapping": {
                    "wx_user1": "emp_001",
                },
            }],
            "rate_limit_per_min":
            60,
        }
    return None


@pytest.fixture
def gateway():
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant)
    app = build_gateway_app(
        registry=registry,
        storage=storage,
        runtime=Runtime(registry=registry, storage=storage, runner=MockAgentRunner()),
        channel_factory=ChannelFactory(),
    )
    return TestClient(app), storage


def test_healthz(gateway):
    client, _ = gateway
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_readyz_reports_dependency_down():
    """readyz 注入依赖探针：探针失败返回 503 + checks 明细（PRD 5.4）。"""
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant)
    checks = {
        "redis": lambda: (_ for _ in ()).throw(RuntimeError("connection refused")),
        "sql": lambda: None,
    }
    app = build_gateway_app(
        registry=registry,
        storage=storage,
        runtime=Runtime(registry=registry, storage=storage, runner=MockAgentRunner()),
        channel_factory=ChannelFactory(),
        readiness_checks=checks,
    )
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["redis"] == "down: RuntimeError"
    assert body["checks"]["sql"] == "ok"


def test_web_ui_page(gateway):
    client, _ = gateway
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Teneuris" in resp.text


def test_chat_endpoint(gateway):
    client, _ = gateway
    resp = client.post("/chat", json={"tenant_id": "demo", "user_id": "u1", "content": "你好"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["response_type"] == "text"
    assert "你好" in data["content"]
    assert data["trace_id"]


def test_chat_unknown_tenant(gateway):
    client, _ = gateway
    resp = client.post("/chat", json={"tenant_id": "nope", "content": "hi"})
    assert resp.json()["response_type"] == "error"


def test_chat_idempotency(gateway):
    client, _ = gateway
    payload = {"tenant_id": "demo", "content": "重复", "msg_id": "dup-1"}
    client.post("/chat", json=payload)
    resp = client.post("/chat", json=payload)
    assert "duplicate" in resp.json()["content"]


def test_chat_session_auto_generated_and_persisted(gateway):
    """未传 session_id 时确定性生成并落库（PRD 1.3-3 / 2.3 共享后端）。"""
    import asyncio

    client, storage = gateway
    r1 = client.post("/chat", json={"tenant_id": "demo", "user_id": "u1", "content": "第一句"}).json()
    r2 = client.post("/chat", json={"tenant_id": "demo", "user_id": "u1", "content": "第二句"}).json()
    # 同 (tenant, channel, user) 恒定映射到同一 session
    assert r1["session_id"]
    assert r1["session_id"] == r2["session_id"]

    async def load():
        return await storage.session.get_session("demo", r1["session_id"])

    sess = asyncio.run(load())
    assert sess is not None
    history = sess.get("state", {}).get("history", [])
    assert len(history) == 4  # 两轮 user + assistant
    # 不同用户映射到不同 session
    r3 = client.post("/chat", json={"tenant_id": "demo", "user_id": "u2", "content": "你好"}).json()
    assert r3["session_id"] != r1["session_id"]


def test_metrics_endpoint(gateway):
    client, _ = gateway
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "teneuris_agent_requests_total" in resp.text


def test_webhook_verify_echostr(gateway):
    """企微 URL 验证: 加密 echostr + 正确签名 -> 解密回显（PRD 3.4）。"""
    from urllib.parse import quote

    from trpc_service.channels.wechat_work import _aes_encrypt, derive_aes_key

    client, _ = gateway
    # 未配置通道的租户 -> 404
    resp = client.get("/webhook/wechat_work/t1__cb1?echostr=abc123")
    assert resp.status_code == 404

    # 已配置通道: 构造加密 echostr + 正确 msg_signature
    aes_key = derive_aes_key("jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C")
    plain = "hello-world"
    encrypted = _aes_encrypt(plain, aes_key, "corp1")
    ts, nonce = "1409659813", "1372623149"
    sig = hashlib.sha1("".join(sorted(["wx-token", ts, nonce, encrypted])).encode()).hexdigest()
    resp = client.get(
        f"/webhook/wechat_work/w1__cb1?echostr={quote(encrypted)}&msg_signature={sig}&timestamp={ts}&nonce={nonce}")
    assert resp.status_code == 200
    assert resp.text == plain
    # 错误签名 -> 401
    resp = client.get(
        f"/webhook/wechat_work/w1__cb1?echostr={quote(encrypted)}&msg_signature=bad&timestamp={ts}&nonce={nonce}")
    assert resp.status_code == 401


def test_webhook_wecom_encrypted_full_chain(gateway, monkeypatch):
    """企微加密回调全链路: 验签 -> AES 解密 -> Filter -> Runtime -> 回复投递。

    覆盖阶段二遗留「send_message 从未被调用」的接线缺口（PRD 3.2/0.3-5）。
    """
    from trpc_service.channels.wechat_work import WechatWorkAdapter

    client, storage = gateway
    sent: list[dict] = []

    async def fake_send_message(self, tenant_id, msg):
        sent.append({"tenant_id": tenant_id, "content": msg.content, "user_id": msg.metadata.get("user_id")})

    monkeypatch.setattr(WechatWorkAdapter, "send_message", fake_send_message)

    aes_key = derive_aes_key("jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C")
    token = "wx-token"
    inner = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
             "<FromUserName><![CDATA[wx_user1]]></FromUserName>"
             "<CreateTime>1409659813</CreateTime>"
             "<MsgType><![CDATA[text]]></MsgType>"
             "<Content><![CDATA[你好]]></Content><MsgId>e2e-001</MsgId></xml>")
    encrypt = _aes_encrypt(inner, aes_key, "corp1")
    ts, nonce = "1409659813", "1372623149"
    sig = hashlib.sha1("".join(sorted([token, ts, nonce, encrypt])).encode()).hexdigest()
    body = f"<xml><ToUserName><![CDATA[corp1]]></ToUserName><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>"
    url = f"/webhook/wechat_work/w1__cb1?msg_signature={sig}&timestamp={ts}&nonce={nonce}"

    resp = client.post(url, content=body.encode(), headers={"Content-Type": "application/xml"})
    assert resp.status_code == 200
    assert resp.text == "success"  # 企微回调 ack
    # 回复经 send_message 真实投递（含 user_id 用于回投）
    assert len(sent) == 1
    assert "你好" in sent[0]["content"]
    assert sent[0]["user_id"] == "wx_user1"
    # 通过审计/幂等验证链路真实执行
    logs = asyncio.run(storage.audit.query_logs("w1", {}))
    assert any(entry["decision"] == "allow" for entry in logs)


def test_webhook_user_id_mapping(gateway, monkeypatch):
    """身份映射（PRD 3.4）：外部 user_id 映射为内部 id 供审计，回复回投用外部 id。"""
    from trpc_service.channels.wechat_work import WechatWorkAdapter

    client, storage = gateway
    sent: list[dict] = []

    async def fake_send_message(self, tenant_id, msg):
        sent.append({"user_id": msg.metadata.get("user_id"), "content": msg.content})

    monkeypatch.setattr(WechatWorkAdapter, "send_message", fake_send_message)

    aes_key = derive_aes_key("jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C")
    token = "wx-token"
    inner = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
             "<FromUserName><![CDATA[wx_user1]]></FromUserName>"
             "<Content><![CDATA[你好]]></Content><MsgId>map-001</MsgId></xml>")
    encrypt = _aes_encrypt(inner, aes_key, "corp1")
    ts, nonce = "1409659813", "1372623149"
    sig = hashlib.sha1("".join(sorted([token, ts, nonce, encrypt])).encode()).hexdigest()
    body = f"<xml><ToUserName><![CDATA[corp1]]></ToUserName><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>"
    url = f"/webhook/wechat_work/w1__cb1?msg_signature={sig}&timestamp={ts}&nonce={nonce}"

    resp = client.post(url, content=body.encode(), headers={"Content-Type": "application/xml"})
    assert resp.status_code == 200
    # 回复回投使用原始外部 user_id（企微成员 userid）
    assert sent and sent[0]["user_id"] == "wx_user1"
    # 审计使用映射后的内部 user_id（emp_001）
    logs = asyncio.run(storage.audit.query_logs("w1", {}))
    assert any(entry["user_id"] == "emp_001" for entry in logs)


def test_webhook_wecom_duplicate_idempotency(gateway, monkeypatch):
    """企微重复回调: msg_id 幂等去重，不重复回复（PRD 2.3-E）。"""
    from trpc_service.channels.wechat_work import WechatWorkAdapter

    client, _ = gateway
    sent: list[dict] = []

    async def fake_send_message(self, tenant_id, msg):
        sent.append(msg.content)

    monkeypatch.setattr(WechatWorkAdapter, "send_message", fake_send_message)

    aes_key = derive_aes_key("jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C")
    token = "wx-token"
    inner = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
             "<FromUserName><![CDATA[wx_user1]]></FromUserName>"
             "<Content><![CDATA[重复]]></Content><MsgId>e2e-dup-1</MsgId></xml>")
    encrypt = _aes_encrypt(inner, aes_key, "corp1")
    ts, nonce = "1409659813", "1372623149"
    sig = hashlib.sha1("".join(sorted([token, ts, nonce, encrypt])).encode()).hexdigest()
    body = f"<xml><ToUserName><![CDATA[corp1]]></ToUserName><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>"
    url = f"/webhook/wechat_work/w1__cb1?msg_signature={sig}&timestamp={ts}&nonce={nonce}"

    assert client.post(url, content=body.encode()).status_code == 200
    # 同一 msg_id 二次投递 -> 幂等去重，不再触发回复
    assert client.post(url, content=body.encode()).status_code == 200
    assert len(sent) == 1


# ------------------------------------------------------------------
# IM 投递重试（PRD 3.6 失败重试；09-05 缺口 4）
# ------------------------------------------------------------------


def _wecom_encrypted_post(client, msg_id):
    """构造企微加密回调请求（复用测试租户 w1 的密钥），返回响应。"""
    aes_key = derive_aes_key("jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C")
    token = "wx-token"
    inner = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
             "<FromUserName><![CDATA[wx_user1]]></FromUserName>"
             "<CreateTime>1409659813</CreateTime>"
             "<MsgType><![CDATA[text]]></MsgType>"
             f"<Content><![CDATA[你好]]></Content><MsgId>{msg_id}</MsgId></xml>")
    encrypt = _aes_encrypt(inner, aes_key, "corp1")
    ts, nonce = "1409659813", "1372623149"
    sig = hashlib.sha1("".join(sorted([token, ts, nonce, encrypt])).encode()).hexdigest()
    body = f"<xml><ToUserName><![CDATA[corp1]]></ToUserName><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>"
    url = f"/webhook/wechat_work/w1__cb1?msg_signature={sig}&timestamp={ts}&nonce={nonce}"
    return client.post(url, content=body.encode(), headers={"Content-Type": "application/xml"})


def test_webhook_send_retries_on_connect_error(gateway, monkeypatch):
    """未送达类连接错误（ConnectError）重试一次后成功，重试计入指标。"""
    import httpx

    from trpc_service.channels.wechat_work import WechatWorkAdapter
    from trpc_service.metrics.metrics import get_metrics

    client, _ = gateway
    calls = {"n": 0}
    sent: list[str] = []

    async def flaky_send(self, tenant_id, msg):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection refused")
        sent.append(msg.content)

    monkeypatch.setattr(WechatWorkAdapter, "send_message", flaky_send)

    resp = _wecom_encrypted_post(client, "retry-001")
    assert resp.status_code == 200
    assert calls["n"] == 2, "连接错误应恰好重试一次"
    assert len(sent) == 1
    retry_total = get_metrics().im_delivery_retry.labels(tenant_id="w1", channel="wechat_work",
                                                         msg_type="text")._value.get()
    assert retry_total >= 1


def test_webhook_send_no_retry_on_read_timeout(gateway, monkeypatch):
    """响应未知类错误（ReadTimeout）不重试——平台可能已收到，重试会重复投递。"""
    import httpx

    from trpc_service.channels.wechat_work import WechatWorkAdapter

    client, _ = gateway
    calls = {"n": 0}

    async def timeout_send(self, tenant_id, msg):
        calls["n"] += 1
        raise httpx.ReadTimeout("read timed out")

    monkeypatch.setattr(WechatWorkAdapter, "send_message", timeout_send)

    resp = _wecom_encrypted_post(client, "retry-002")
    assert resp.status_code == 502
    assert calls["n"] == 1, "ReadTimeout 不应重试（防重复消息）"


@pytest.fixture
def admin():
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant)
    app = build_admin_app(storage=storage, registry=registry, api_key="secret-key")
    return TestClient(app)


def test_admin_auth_required(admin):
    assert admin.get("/tenants").status_code == 401


def test_admin_crud(admin):
    headers = {"X-Admin-Key": "secret-key"}
    # create
    resp = admin.post("/tenants",
                      headers=headers,
                      json={
                          "tenant_id": "t1",
                          "name": "新租户",
                          "status": "active",
                          "model": {
                              "model_name": "gpt-4o-mini"
                          },
                      })
    assert resp.status_code == 201
    # get
    resp = admin.get("/tenants/t1", headers=headers)
    assert resp.json()["name"] == "新租户"
    # update（热更新）
    resp = admin.put("/tenants/t1", headers=headers, json={"status": "suspended"})
    assert resp.status_code == 200
    # duplicate -> 409
    resp = admin.post("/tenants", headers=headers, json={"tenant_id": "t1", "name": "重复"})
    assert resp.status_code == 409
    # delete
    assert admin.delete("/tenants/t1", headers=headers).json()["status"] == "deleted"
    assert admin.get("/tenants/t1", headers=headers).status_code == 404


def test_admin_secret_not_leaked(admin):
    headers = {"X-Admin-Key": "secret-key"}
    admin.post("/tenants",
               headers=headers,
               json={
                   "tenant_id": "t_key",
                   "name": "带密钥",
                   "model": {
                       "api_key_ref": "sk-super-secret-1234567890"
                   },
                   "im": [{
                       "channel_type": "wechat_work",
                       "token_ref": "tok-abc-123456"
                   }],
               })
    body = admin.get("/tenants/t_key", headers=headers).text
    assert "sk-super-secret" not in body
    assert "tok-abc" not in body
    admin.delete("/tenants/t_key", headers=headers)


def test_admin_audit_query(admin):
    headers = {"X-Admin-Key": "secret-key"}
    resp = admin.get("/audit/t1", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["logs"] == []


def test_admin_write_ops_record_operator_audit():
    """Admin 写操作审计留痕：X-Admin-Operator 头记录操作人（PRD 4.4）。"""
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant)
    app = build_admin_app(storage=storage, registry=registry, api_key="secret-key")

    async def _query():
        return await storage.audit.query_logs("op1", {"decision": "admin_create"})

    with TestClient(app) as client:
        # create 带 operator
        resp = client.post("/tenants",
                           headers={
                               "X-Admin-Key": "secret-key",
                               "X-Admin-Operator": "ops/niuchenxun"
                           },
                           json={
                               "tenant_id": "op1",
                               "name": "操作人审计"
                           })
        assert resp.status_code == 201
        logs = asyncio.run(_query())
        assert len(logs) == 1
        assert logs[0]["payload"]["operator"] == "ops/niuchenxun"

        # update 带 operator -> admin_update
        resp = client.put("/tenants/op1",
                          headers={
                              "X-Admin-Key": "secret-key",
                              "X-Admin-Operator": "ops/niuchenxun"
                          },
                          json={"status": "suspended"})
        assert resp.status_code == 200
        logs = asyncio.run(storage.audit.query_logs("op1", {"decision": "admin_update"}))
        assert len(logs) == 1
        assert logs[0]["payload"]["operator"] == "ops/niuchenxun"

        # rollback 无 operator -> operator=None 不崩
        resp = client.post("/tenants/op1/rollback", headers={"X-Admin-Key": "secret-key"})
        assert resp.status_code == 200
        logs = asyncio.run(storage.audit.query_logs("op1", {"decision": "admin_rollback"}))
        assert len(logs) == 1
        assert logs[0]["payload"]["operator"] is None


@pytest.mark.asyncio
async def test_admin_sql_persistence(tmp_path):
    """Admin 租户 CRUD 持久化到 SQL（PRD 2.5 数据模型）。"""
    from httpx import ASGITransport, AsyncClient

    from trpc_service.storage import SqlTenantStore, create_sql_engine
    from trpc_service.storage.factory import StorageFactory
    from trpc_service.tenant import DataBackendConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/admin.db"
    engine = await create_sql_engine(dsn)
    tenant_store = SqlTenantStore(engine)
    factory = StorageFactory(sql_engine=engine)
    storage = await factory.create("admin", DataBackendConfig(session="inmemory", memory="inmemory", audit="sql"))
    registry = TenantRegistry()
    app = build_admin_app(storage=storage, registry=registry, api_key="k", tenant_store=tenant_store)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        headers = {"X-Admin-Key": "k"}
        # create
        resp = await client.post("/tenants",
                                 headers=headers,
                                 json={
                                     "tenant_id": "sqlt1",
                                     "name": "SQL租户",
                                     "status": "active"
                                 })
        assert resp.status_code == 201
        # 已持久化到 SQL（绕开进程内缓存直接读 store）
        assert (await tenant_store.get("sqlt1")).name == "SQL租户"
        # update
        resp = await client.put("/tenants/sqlt1", headers=headers, json={"status": "suspended"})
        assert resp.status_code == 200
        assert (await tenant_store.get("sqlt1")).status == "suspended"
        # delete
        assert (await client.delete("/tenants/sqlt1", headers=headers)).json()["status"] == "deleted"
        assert await tenant_store.get("sqlt1") is None
    await tenant_store.close()


# ------------------------------------------------------------------
# C5 回归：出站限频（消费 PlatformLimits.rate_limit_per_sec）
# ------------------------------------------------------------------


class _FakeLimits:

    def __init__(self, rate):
        self.rate_limit_per_sec = rate


class _FakeOutboundAdapter:

    def __init__(self, rate):
        self._rate = rate

    def platform_limits(self):
        return _FakeLimits(self._rate)


@pytest.mark.asyncio
async def test_throttle_outbound_spaces_sends():
    """rate=10/s 时连续两次投递应被错峰（第二次等待 ≈ 最小间隔）。"""
    import time as _time

    from trpc_service.web.app import _OUTBOUND_LAST_SEND, _throttle_outbound

    adapter = _FakeOutboundAdapter(10)  # 最小间隔 0.1s
    key = ("t_throttle", "web")
    _OUTBOUND_LAST_SEND.pop(key, None)
    start = _time.monotonic()
    await _throttle_outbound(adapter, "t_throttle", "web")  # 首次：不等待
    await _throttle_outbound(adapter, "t_throttle", "web")  # 第二次：等待一个间隔
    elapsed = _time.monotonic() - start
    assert elapsed >= 0.08, f"第二次投递应被错峰等待，实际 {elapsed:.3f}s"
    _OUTBOUND_LAST_SEND.pop(key, None)


@pytest.mark.asyncio
async def test_throttle_outbound_no_limit_passes():
    """rate=0（不限速）时投递不等待。"""
    import time as _time

    from trpc_service.web.app import _OUTBOUND_LAST_SEND, _throttle_outbound

    adapter = _FakeOutboundAdapter(0)
    _OUTBOUND_LAST_SEND.pop(("t_free", "web"), None)
    start = _time.monotonic()
    for _ in range(3):
        await _throttle_outbound(adapter, "t_free", "web")
    assert _time.monotonic() - start < 0.05, "不限速不应等待"
    _OUTBOUND_LAST_SEND.pop(("t_free", "web"), None)
