"""
Unit tests for Advanced Platform Modules:
- KMS Envelope Encryption
- WeChat Customer Service (WeChat KF)
- Rate Limiter & Retry Queue (DLQ)
- Storage Data Migration
- Circuit Breaker & Fallback
- Gray Release & Config Rollback
- Workspace Sandbox Isolation
- Skill Engine
"""

import asyncio
import pytest
from trpc_service.config.kms import kms_client
from trpc_service.config.models import (
    ChannelType,
    TenantConfig,
    Session,
    SessionEvent,
    EventType,
    OutboundMessage,
)
from trpc_service.channels.wechat_kf import WeChatKFChannelAdapter
from trpc_service.channels.rate_limiter import TokenBucketLimiter, ChannelRetryQueue
from trpc_service.storage.memory_adapter import InMemoryStorageAdapter
from trpc_service.storage.migration import migration_manager
from trpc_service.agent.circuit_breaker import CircuitBreaker, CircuitState, fallback_manager
from trpc_service.tenant.gray_release import GrayReleaseRule, gray_release_manager
from trpc_service.workspace.sandbox import workspace_sandbox
from trpc_service.skill.base import skill_registry


def test_kms_envelope_encryption():
    raw_secret = "sk-deepseek-api-key-very-sensitive-9988"
    encrypted = kms_client.encrypt(raw_secret)
    assert encrypted.startswith("enc:kms:v1:")
    assert raw_secret not in encrypted

    decrypted = kms_client.decrypt(encrypted)
    assert decrypted == raw_secret
    assert kms_client.mask_key_preview(raw_secret) == "sk-dee...9988"


def test_wechat_kf_channel_adapter():
    adapter = WeChatKFChannelAdapter()
    payload = b"""{
        "external_userid": "wm_user_12345",
        "open_kfid": "kf_corp_service",
        "msgtype": "text",
        "text": {"content": "Help me reset password"}
    }"""
    inbound = adapter.parse_inbound_message("tenant_kf", {}, {}, payload)
    assert inbound.tenant_id == "tenant_kf"
    assert inbound.channel_type == ChannelType.WECHAT_KF
    assert inbound.raw_user_id == "wm_user_12345"
    assert inbound.content == "Help me reset password"

    outbound = OutboundMessage(
        trace_id="tr_kf",
        tenant_id="tenant_kf",
        channel_type=ChannelType.WECHAT_KF,
        target_user_id="wm_user_12345",
        target_chat_id="kf_corp_service",
        content="Reset link has been dispatched.",
    )
    formatted = adapter.format_outbound_payload(outbound)
    assert formatted["touser"] == "wm_user_12345"
    assert formatted["text"]["content"] == "Reset link has been dispatched."


def test_rate_limiter_and_retry_queue():
    async def _test():
        limiter = TokenBucketLimiter(rate_per_second=10.0, capacity=2.0)
        assert await limiter.acquire() is True
        assert await limiter.acquire() is True
        # Capacity exhausted
        assert await limiter.acquire() is False

        # Retry Queue & DLQ
        rq = ChannelRetryQueue()
        msg = OutboundMessage(
            trace_id="tr_fail",
            tenant_id="t1",
            channel_type=ChannelType.WECOM,
            target_user_id="u1",
            content="test retry",
        )
        await rq.enqueue(msg, "bind_1", error="Network timeout 504")
        assert len(rq.retry_items) == 1

        # Simulate 3 failures to move to DLQ
        def fail_send(m, b):
            return False

        # Fast forward
        item = rq.retry_items[0]
        item.next_retry_time = 0.0
        item.attempt_count = 3  # Hit max attempts
        await rq.process_retries(fail_send)
        assert len(rq.dead_letter_queue) == 1
        assert rq.dead_letter_queue[0].message.trace_id == "tr_fail"

    asyncio.run(_test())


def test_storage_migration():
    async def _test():
        source = InMemoryStorageAdapter()
        target = InMemoryStorageAdapter()

        # Seed data in source
        session = Session(
            session_id="mig_s1",
            tenant_id="t_mig",
            app_id="a1",
            channel_type="wecom",
            external_user_id="u_mig",
        )
        await source.save_session(session)
        evt = SessionEvent(
            session_id="mig_s1",
            tenant_id="t_mig",
            event_type=EventType.USER_MESSAGE,
            payload={"text": "data before migration"},
        )
        await source.append_events("t_mig", "mig_s1", [evt], expected_version=0)

        # Run Migration
        stats = await migration_manager.migrate_sessions("t_mig", source, target, ["mig_s1"])
        assert stats.sessions_migrated == 1
        assert stats.events_migrated == 1

        # Check integrity
        parity = await migration_manager.verify_integrity("t_mig", "mig_s1", source, target)
        assert parity is True

    asyncio.run(_test())


def test_circuit_breaker_transitions():
    cb = CircuitBreaker("test_model", failure_threshold=2, recovery_timeout_seconds=0.1)
    assert cb.state == CircuitState.CLOSED
    assert cb.allow_execution() is True

    cb.record_failure()
    assert cb.state == CircuitState.CLOSED

    cb.record_failure()
    # Tripped to OPEN
    assert cb.state == CircuitState.OPEN
    assert cb.allow_execution() is False

    # Model fallback
    fb = fallback_manager.resolve_model_fallback("openai")
    assert fb == "qwen"


def test_gray_release_and_rollback():
    rule = GrayReleaseRule(
        tenant_id="t_gray",
        canary_percent=50,
        whitelist_users=["vip_tester"],
    )
    gray_release_manager.set_gray_rule(rule)

    assert gray_release_manager.should_use_canary("t_gray", "vip_tester") is True
    # Non-whitelisted depends on hash
    res = gray_release_manager.should_use_canary("t_gray", "random_user_99")
    assert isinstance(res, bool)

    # Config rollback
    cfg_v1 = TenantConfig(tenant_id="t_rb", name="Version 1")
    gray_release_manager.save_config_snapshot(cfg_v1)

    cfg_v2 = TenantConfig(tenant_id="t_rb", name="Version 2 (Broken)")
    gray_release_manager.save_config_snapshot(cfg_v2)

    restored = gray_release_manager.rollback_to_previous("t_rb")
    assert restored is not None
    assert restored.name == "Version 1"


def test_workspace_sandbox():
    ws = workspace_sandbox
    tenant_id = "tenant_test_ws"
    file_path = ws.write_file(tenant_id, "output.txt", "hello sandbox")
    content = ws.read_file(tenant_id, "output.txt")
    assert content == "hello sandbox"

    # Path traversal attack detection
    with pytest.raises(PermissionError):
        ws.write_file(tenant_id, "../../evil.sh", "malicious")

    # Cleanup
    ws.cleanup_workspace(tenant_id)


def test_skill_engine():
    async def _test():
        skill = skill_registry.match_skill("Please code review this function: def add(a, b): return a + b")
        assert skill is not None
        assert skill.name == "code_review_skill"

        res = await skill.run("tenant_dev", "code review", {})
        assert "代码审查报告" in res

    asyncio.run(_test())
