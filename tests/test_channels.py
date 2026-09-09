# channels 模块单元测试
import hashlib
import json

import pytest

from trpc_service.channels import (
    ChannelFactory,
    FeishuAdapter,
    WebImAdapter,
    WechatWorkAdapter,
)
from trpc_service.channels.wechat_work import _aes_decrypt, _aes_encrypt, derive_aes_key
from trpc_service.tenant import ImChannelConfig


def test_web_adapter_parse():
    adapter = WebImAdapter()
    parsed = adapter.parse_webhook(json.dumps({"msg_id": "m1", "user_id": "u1", "content": "你好"}).encode(), {})
    ev = parsed.event
    assert ev.channel_type == "web"
    assert ev.content == "你好"
    assert adapter.verify_signature(b"", "")  # 自测通道放行


def test_web_adapter_bad_json():
    adapter = WebImAdapter()
    with pytest.raises(ValueError):
        adapter.parse_webhook(b"not-json", {})


def test_wecom_plain_xml_parse():
    adapter = WechatWorkAdapter()
    body = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
            "<FromUserName><![CDATA[wx1]]></FromUserName>"
            "<Content><![CDATA[你好]]></Content><MsgId>123</MsgId></xml>").encode()
    ev = adapter.parse_webhook(body, {}).event
    assert ev.channel_id == "corp1"
    assert ev.user_id == "wx1"
    assert ev.content == "你好"
    assert ev.msg_id == "123"


def test_wecom_namespaced_xml():
    adapter = WechatWorkAdapter()
    body = ('<xml xmlns="http://www.work.weixin.qq.com">'
            "<ToUserName><![CDATA[corp1]]></ToUserName>"
            "<FromUserName><![CDATA[wx1]]></FromUserName>"
            "<Content><![CDATA[ns]]></Content><MsgId>9</MsgId></xml>").encode()
    ev = adapter.parse_webhook(body, {}).event
    assert ev.content == "ns"


def test_wecom_limits():
    adapter = WechatWorkAdapter()
    limits = adapter.platform_limits()
    assert limits.ack_required
    assert limits.max_message_len == 2048


def test_split_long_message():
    adapter = WechatWorkAdapter()
    chunks = adapter.split_long_message("x" * 5000)
    assert len(chunks) == 3
    assert all(len(c) <= 2048 for c in chunks)


def test_factory_reuses_instance():
    factory = ChannelFactory()
    cfg = ImChannelConfig(channel_type="wechat_work", app_id="corp1", secret_ref="sec1")
    a1 = factory.create("t1", cfg)
    a2 = factory.create("t1", cfg)
    assert a1 is a2
    with pytest.raises(ValueError):
        # 不支持的通道类型应抛 ValueError
        factory.create("t1", ImChannelConfig(channel_type="unsupported_channel"))


# ---------------------------------------------------------------------------
# 企微验签 + AES 加解密（官方算法，PRD 3.4）
# ---------------------------------------------------------------------------

_ENCODING_AES_KEY = "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"
"""43 字符测试 EncodingAESKey（企微官方文档示例）。"""


def _signature_wecom(token, timestamp, nonce, encrypt):
    return hashlib.sha1("".join(sorted([token, timestamp, nonce, encrypt])).encode()).hexdigest()


def test_derive_aes_key_length():
    key = derive_aes_key(_ENCODING_AES_KEY)
    assert len(key) == 32  # 43 字符 base64 + "=" -> 32 字节


def test_wecom_aes_roundtrip():
    aes_key = derive_aes_key(_ENCODING_AES_KEY)
    plaintext = "<xml><ToUserName><![CDATA[corp1]]></ToUserName><Content><![CDATA[你好]]></Content></xml>"
    ciphertext = _aes_encrypt(plaintext, aes_key, "corp1")
    assert _aes_decrypt(ciphertext, aes_key) == plaintext


def test_wecom_signature_verify():
    adapter = WechatWorkAdapter(ImChannelConfig(
        channel_type="wechat_work",
        token_ref="wx-token",
    ))
    token = "wx-token"
    ts, nonce, encrypt = "1409659813", "1372623149", "encrypted-xml"
    adapter._last_timestamp = ts
    adapter._last_nonce = nonce
    adapter._last_encrypt = encrypt
    sig = _signature_wecom(token, ts, nonce, encrypt)
    assert adapter.verify_signature(b"", sig)
    assert not adapter.verify_signature(b"", "deadbeef")


def test_wecom_echostr_verify_and_decrypt():
    """URL 验证: 签名校验 + echostr 密文解密（PRD 3.4 GET）。"""
    aes_key = derive_aes_key(_ENCODING_AES_KEY)
    token = "wx-token"
    ts, nonce = "1409659813", "1372623149"
    echostr = "hello-world"
    encrypted = _aes_encrypt(echostr, aes_key, "corp1")
    adapter = WechatWorkAdapter(
        ImChannelConfig(
            channel_type="wechat_work",
            token_ref=token,
            aes_key_ref=_ENCODING_AES_KEY,
            app_id="corp1",
        ))
    # 正确签名 -> 解密回明文
    sig = _signature_wecom(token, ts, nonce, encrypted)
    assert adapter.verify_echostr(encrypted, ts, nonce, sig)
    assert adapter.decrypt_echostr(encrypted) == echostr
    # 错误签名 -> 拒绝
    assert not adapter.verify_echostr(encrypted, ts, nonce, "bad")
    # 未配置 aes_key（自测）-> 原样回显
    plain_adapter = WechatWorkAdapter(ImChannelConfig(channel_type="wechat_work"))
    assert plain_adapter.decrypt_echostr("abc") == "abc"


def test_wecom_parse_encrypted_xml():
    """加密模式回调: 验签 + AES 解密 + 解析 AgentEvent。"""
    aes_key = derive_aes_key(_ENCODING_AES_KEY)
    token = "wx-token"
    inner = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
             "<FromUserName><![CDATA[wx1]]></FromUserName>"
             "<CreateTime>1409659813</CreateTime>"
             "<MsgType><![CDATA[text]]></MsgType>"
             "<Content><![CDATA[你好]]></Content><MsgId>456</MsgId></xml>")
    encrypt = _aes_encrypt(inner, aes_key, "corp1")
    ts, nonce = "1409659813", "1372623149"
    sig = _signature_wecom(token, ts, nonce, encrypt)
    body = f"<xml><ToUserName><![CDATA[corp1]]></ToUserName><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>".encode()
    adapter = WechatWorkAdapter(
        ImChannelConfig(
            channel_type="wechat_work",
            token_ref=token,
            aes_key_ref=_ENCODING_AES_KEY,
            app_id="corp1",
        ))
    parsed = adapter.parse_webhook(body, {"msg_signature": sig, "timestamp": ts, "nonce": nonce})
    assert adapter.verify_signature(body, sig)
    ev = parsed.event
    assert ev.channel_id == "corp1"
    assert ev.user_id == "wx1"
    assert ev.content == "你好"
    assert ev.msg_id == "456"


# ---------------------------------------------------------------------------
# 飞书（Lark）通道（第三类真实可连 IM，PRD 3.1）
# ---------------------------------------------------------------------------


def _feishu_cfg(encrypt_key="enckey123", **kw):
    return ImChannelConfig(
        channel_type="feishu",
        app_id="cli_test",
        secret_ref="app_secret",
        token_ref="verif_token",
        aes_key_ref=encrypt_key,
        **kw,
    )


def test_feishu_aes_roundtrip():
    from trpc_service.channels.feishu import _feishu_decrypt, _feishu_encrypt

    plain = json.dumps({"text": "你好飞书"}, ensure_ascii=False)
    enc = _feishu_encrypt(plain, "enckey123", "cli_test")
    assert _feishu_decrypt(enc, "enckey123") == plain


def test_feishu_adapter_construct():
    a = FeishuAdapter(_feishu_cfg())
    assert a._app_id == "cli_test"
    assert a._secret == "app_secret"
    assert a._token == "verif_token"
    assert a.channel_type == "feishu"


def test_feishu_parse_plain_message_event():
    a = FeishuAdapter(_feishu_cfg())
    evt = {
        "header": {
            "event_type": "im.message.receive_v1",
            "token": "verif_token",
            "event_id": "e1"
        },
        "event": {
            "message": {
                "message_id": "m1",
                "chat_id": "oc_x",
                "chat_type": "group",
                "content": json.dumps({"text": "ping"}),
            },
            "sender": {
                "sender_id": {
                    "open_id": "ou_u1"
                }
            },
        },
    }
    parsed = a.parse_webhook(json.dumps(evt).encode(), {})
    ev = parsed.event
    assert ev.content == "ping"
    assert ev.user_id == "ou_u1"
    assert ev.msg_id == "m1"
    assert ev.is_group is True
    assert ev.metadata["chat_id"] == "oc_x"
    assert parsed.signature == "verif_token"


def test_feishu_parse_encrypted_event():
    from trpc_service.channels.feishu import _feishu_encrypt

    a = FeishuAdapter(_feishu_cfg())
    evt = {
        "header": {
            "event_type": "im.message.receive_v1",
            "token": "verif_token"
        },
        "event": {
            "message": {
                "message_id": "m2",
                "chat_id": "oc_y",
                "chat_type": "single",
                "content": json.dumps({"text": "secret"}),
            },
            "sender": {
                "sender_id": {
                    "open_id": "ou_u2"
                }
            },
        },
    }
    body = json.dumps({"encrypt": _feishu_encrypt(json.dumps(evt), "enckey123", "cli_test")}).encode()
    parsed = a.parse_webhook(body, {})
    assert parsed.event.content == "secret"
    assert parsed.event.user_id == "ou_u2"


def test_feishu_url_verification():
    a = FeishuAdapter(_feishu_cfg())
    body = json.dumps({"type": "url_verification", "challenge": "abc123", "token": "verif_token"}).encode()
    assert a.url_verification_response(body, {}) == {"challenge": "abc123"}
    # token 不匹配应返回 None（验证失败）
    bad = json.dumps({"type": "url_verification", "challenge": "x", "token": "wrong"}).encode()
    assert a.url_verification_response(bad, {}) is None
    # 普通消息事件应返回 None
    assert a.url_verification_response(json.dumps({"header": {"event_type": "x"}}).encode(), {}) is None


def test_feishu_signature():
    a = FeishuAdapter(_feishu_cfg())
    assert a.verify_signature(b"", "verif_token")
    assert not a.verify_signature(b"", "bad")
    # 未配置 token 放行
    a2 = FeishuAdapter(ImChannelConfig(channel_type="feishu", app_id="x", secret_ref="s"))
    assert a2.verify_signature(b"", "anything")


def test_feishu_hmac_signature_both_forms():
    """X-Lark-Signature HMAC 验签：逗号分隔与 query 两种形态均应通过。"""
    from trpc_service.channels.feishu import _compute_lark_signature

    token = "lark-token"
    body = b'{"type":"url_verification","challenge":"c1","token":"lark-token"}'
    ts, nonce = "1700000000", "nonce123"
    a = FeishuAdapter(ImChannelConfig(channel_type="feishu", app_id="x", secret_ref="s", token_ref=token))

    for fmt in ("sha256={b64},timestamp={ts},nonce={nonce}", "sha256={b64}?timestamp={ts}&nonce={nonce}"):
        header = fmt.format(b64="X", ts=ts, nonce=nonce)
        b64 = _compute_lark_signature(token, body, header)
        good = fmt.format(b64=b64, ts=ts, nonce=nonce)
        assert a.verify_signature(body, good), f"应通过: {fmt}"
        bad = fmt.format(b64="Y" * len(b64), ts=ts, nonce=nonce)
        assert not a.verify_signature(body, bad), f"应拒绝: {fmt}"
    # 非 HMAC 头回落到事件体 token 比对
    assert a.verify_signature(body, token)
    assert not a.verify_signature(body, "wrong-token")


def test_feishu_send_message():
    """send_message 走 tenant_access_token + im/v1/messages，并正确寻址。"""
    from trpc_service.channels.feishu import FeishuAdapter
    from trpc_service.events import AgentResponse

    a = FeishuAdapter(_feishu_cfg())

    class _Resp:

        def __init__(self, data):
            self._d = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._d

    class _Client:

        def __init__(self):
            self.calls = []

        async def post(self, url, **kw):
            self.calls.append((url, kw))
            if "tenant_access_token" in url:
                return _Resp({"code": 0, "tenant_access_token": "T1", "expire": 7200})
            return _Resp({"code": 0, "data": {"message_id": "ok"}})

        async def aclose(self):
            pass

    a._http = _Client()
    msg = AgentResponse.text("你好", tenant_id="demo", channel_type="feishu")
    msg.metadata["user_id"] = "ou_u1"
    import asyncio
    asyncio.run(a.send_message("demo", msg))

    # 第一次调用取 token，第二次发消息
    assert "tenant_access_token" in a._http.calls[0][0]
    send_url, send_kw = a._http.calls[1]
    assert "im/v1/messages" in send_url
    assert send_kw["params"]["receive_id_type"] == "open_id"
    assert send_kw["json"]["receive_id"] == "ou_u1"
    assert send_kw["headers"]["Authorization"] == "Bearer T1"
    assert json.loads(send_kw["json"]["content"])["text"] == "你好"


def test_feishu_factory_build():
    f = ChannelFactory()
    a = f.create("demo", _feishu_cfg(), "feishu")
    assert isinstance(a, FeishuAdapter)
    # 同租户同通道复用实例
    assert f.create("demo", _feishu_cfg(), "feishu") is a


# ----------------------------------------------------------------------
# 企微智能机器人·长连接（wecom_bot）：纯函数用例（无网络）
# ----------------------------------------------------------------------


def _bot_frame_single(content="你好", msgid="m1", userid="u1", req_id="r1"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": req_id
        },
        "body": {
            "msgid": msgid,
            "aibotid": "bot_1",
            "chattype": "single",
            "from": {
                "userid": userid
            },
            "msgtype": "text",
            "text": {
                "content": content
            },
        },
    }


def _bot_frame_group(content="@Teneuris 报销流程是什么", msgid="m2", chatid="chat_9", userid="u1"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": "r2"
        },
        "body": {
            "msgid": msgid,
            "aibotid": "bot_1",
            "chatid": chatid,
            "chattype": "group",
            "from": {
                "userid": userid
            },
            "msgtype": "text",
            "text": {
                "content": content
            },
        },
    }


def test_wecom_bot_frame_to_event_single():
    from trpc_service.channels.wecom_bot import frame_to_event
    ev = frame_to_event(_bot_frame_single(), tenant_id="demo")
    assert ev is not None
    assert ev.channel_type == "wecom_bot"
    assert ev.is_group is False
    assert ev.channel_id == "bot_1"  # 单聊以 bot id 为 channel
    assert ev.user_id == "u1"
    assert ev.msg_id == "m1"
    assert ev.content == "你好"
    assert ev.metadata["req_id"] == "r1"
    assert ev.tenant_id == "demo"


def test_wecom_bot_frame_to_event_group():
    from trpc_service.channels.wecom_bot import frame_to_event
    ev = frame_to_event(_bot_frame_group(), tenant_id="demo")
    assert ev is not None
    assert ev.is_group is True
    assert ev.channel_id == "chat_9"  # 群聊以 chatid 为 channel（隔离不同群）
    assert ev.content == "报销流程是什么"  # @mention 已剥除


def test_wecom_bot_frame_ignore_non_text():
    from trpc_service.channels.wecom_bot import frame_to_event
    frame = _bot_frame_single()
    frame["body"]["msgtype"] = "image"
    assert frame_to_event(frame, tenant_id="demo") is None
    # 群聊 @ 后为空内容（纯 mention）不触发
    frame = _bot_frame_group(content=" @Teneuris  ", msgid="m3")
    assert frame_to_event(frame, tenant_id="demo") is None
    # 非 dict body / 空 body
    assert frame_to_event({"body": None}, tenant_id="demo") is None


def test_wecom_bot_strip_mention():
    from trpc_service.channels.wecom_bot import strip_mention
    assert strip_mention("@Teneuris 你好") == "你好"
    assert strip_mention("  @A @B hi ") == "hi "
    assert strip_mention("no mention") == "no mention"
    assert strip_mention("文本中间@不剥") == "文本中间@不剥"


def test_wecom_bot_split_text():
    from trpc_service.channels.wecom_bot import split_text
    assert split_text("短") == ["短"]
    assert split_text("") == []
    long_text = "长" * 5000
    chunks = split_text(long_text, max_len=2048)
    assert len(chunks) == 3
    assert "".join(chunks) == long_text
    assert all(len(c) <= 2048 for c in chunks)


def test_wecom_bot_build_reply_body():
    from trpc_service.channels.wecom_bot import build_stream_reply, new_stream_id
    body = build_stream_reply("hi", "s_1")
    assert body["msgtype"] == "stream"
    assert body["stream"]["finish"] is True
    assert body["stream"]["id"] == "s_1"
    assert body["stream"]["content"] == "hi"
    assert new_stream_id().startswith("s_")


# ------------------------------------------------------------------
# 全项目审查回归（2026-09-04 OCR findings）
# ------------------------------------------------------------------


def test_split_long_message_bytes_unit_cjk():
    """企微按字节限长分段：纯中文长文本不得超 2048 字节（审查 09-04）。

    缺陷背景：分段按字符数计，1000 个中文 = 3000 字节 > 企微 2048 字节上限，
    会被平台整条拒收。
    """
    from trpc_service.channels.wechat_work import WechatWorkAdapter
    from trpc_service.tenant.models import ImChannelConfig

    adapter = WechatWorkAdapter(ImChannelConfig(channel_type="wechat_work"))
    cjk = "测" * 1000  # UTF-8 每字 3 字节 = 3000 字节
    chunks = adapter.split_long_message(cjk)
    assert len(chunks) >= 2, "3000 字节应被分段"
    for chunk in chunks:
        assert len(chunk.encode("utf-8")) <= 2048, f"分段超字节上限: {len(chunk.encode('utf-8'))}"
    assert "".join(chunks) == cjk, "分段拼接应还原原文（无丢字）"
    # 字符边界：不产生乱码（每段都是合法 UTF-8 —— encode 已隐式验证）


def test_split_long_message_char_unit_unchanged():
    """char 单位（飞书等）行为不变：按字符数切分。"""
    from trpc_service.channels.feishu import FeishuAdapter
    from trpc_service.tenant.models import ImChannelConfig

    adapter = FeishuAdapter(ImChannelConfig(channel_type="feishu"))
    text = "字" * 4500
    chunks = adapter.split_long_message(text)
    assert all(len(c) <= 2000 for c in chunks)
    assert "".join(chunks) == text


def test_tool_gate_blocks_platform_dangerous_without_tenant_listing():
    """平台标记 dangerous 即拦截，不要求租户 dangerous_tools 显式列出（审查 09-04）。"""
    from trpc_service.tool.builder import tool_confirmation_required
    from trpc_service.tool.registry import ToolSpec
    from trpc_service.tenant.models import ToolPermissions

    spec = ToolSpec(name="delete_file", description="d", func=lambda: None, dangerous=True)
    # 租户未把 delete_file 列入 dangerous_tools
    perms = ToolPermissions(allowlist=["delete_file"], dangerous_tools=[])
    assert tool_confirmation_required(spec, perms, frozenset()), "平台 dangerous 应默认拦截"
    # 显式确认后放行
    assert not tool_confirmation_required(spec, perms, frozenset({"delete_file"}))
    # 租户总开关关闭时不拦
    perms_off = ToolPermissions(allowlist=["delete_file"], dangerous_tools=[], require_confirmation=False)
    assert not tool_confirmation_required(spec, perms_off, frozenset())


# ------------------------------------------------------------------
# C2 回归：企微群聊判定（ChatId → is_group + channel_id）
# ------------------------------------------------------------------


def test_wecom_group_chat_plain_xml():
    """明文回调带 ChatId → is_group=True，channel_id 取群 ID。"""
    adapter = WechatWorkAdapter()
    body = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
            "<FromUserName><![CDATA[wx1]]></FromUserName>"
            "<ChatId><![CDATA[wr_group9]]></ChatId>"
            "<Content><![CDATA[群里问]]></Content><MsgId>77</MsgId></xml>").encode()
    ev = adapter.parse_webhook(body, {}).event
    assert ev.is_group is True
    assert ev.channel_id == "wr_group9"
    assert ev.metadata["chat_id"] == "wr_group9"


def test_wecom_single_chat_no_chatid():
    """无 ChatId → 单聊：is_group=False，channel_id 仍为应用 ID（回归）。"""
    adapter = WechatWorkAdapter()
    body = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
            "<FromUserName><![CDATA[wx1]]></FromUserName>"
            "<Content><![CDATA[单聊]]></Content><MsgId>78</MsgId></xml>").encode()
    ev = adapter.parse_webhook(body, {}).event
    assert ev.is_group is False
    assert ev.channel_id == "corp1"


def test_wecom_group_chat_encrypted_xml():
    """加密回调解密后含 ChatId → 同样判定群聊（wecom_bot.py 先例对齐）。"""
    aes_key = derive_aes_key(_ENCODING_AES_KEY)
    inner = ("<xml><ToUserName><![CDATA[corp1]]></ToUserName>"
             "<FromUserName><![CDATA[wx1]]></FromUserName>"
             "<ChatId><![CDATA[wr_group7]]></ChatId>"
             "<Content><![CDATA[加密群聊]]></Content><MsgId>79</MsgId></xml>")
    encrypt = _aes_encrypt(inner, aes_key, "corp1")
    body = (f"<xml><Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>").encode()
    token = "wx-token"
    ts, nonce = "1409659813", "n3"
    sig = _signature_wecom(token, ts, nonce, encrypt)
    adapter = WechatWorkAdapter(
        ImChannelConfig(
            channel_type="wechat_work",
            token_ref=token,
            aes_key_ref=_ENCODING_AES_KEY,
            app_id="corp1",
        ))
    parsed = adapter.parse_webhook(body, {"msg_signature": sig, "timestamp": ts, "nonce": nonce})
    ev = parsed.event
    assert ev.is_group is True
    assert ev.channel_id == "wr_group7"


# ------------------------------------------------------------------
# C6 回归：适配器缓存随租户配置热更新失效
# ------------------------------------------------------------------


def test_factory_invalidate_on_hot_update():
    """invalidate(tenant) 后该租户适配器重建（新配置生效），其他租户不受影响。"""
    from trpc_service.tenant import ImChannelConfig as _Cfg

    factory = ChannelFactory()
    a1 = factory.create("t_inv", _Cfg(channel_type="web"))
    other = factory.create("t_other", _Cfg(channel_type="web"))
    assert factory.get("t_inv", "web") is a1

    factory.invalidate("t_inv")
    assert factory.get("t_inv", "web") is None, "失效后缓存应清空"
    a2 = factory.create("t_inv", _Cfg(channel_type="web"))
    assert a2 is not a1, "失效后应重建新实例（带新配置）"
    assert factory.get("t_other", "web") is other, "其他租户缓存不受影响"
