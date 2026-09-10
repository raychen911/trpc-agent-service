"""存储适配与审计服务测试。"""
import pytest

from trpc_service.tenant.audit.model import AuditEvent
from trpc_service.tenant.audit.redact import redact
from trpc_service.tenant.audit.service import AuditService
from trpc_service.config.tenant_config import TenantConfig
from trpc_service.tenant.storage.factory import create_storage


def test_create_storage_in_memory():
    cfg = TenantConfig(tenant_id="t", name="t")
    adapter = create_storage(cfg)
    assert adapter.backend == "in_memory"
    assert adapter.session_service is not None
    assert adapter.memory_service is not None


def test_create_storage_sql_requires_url():
    cfg = TenantConfig(tenant_id="t", name="t", storage={"session_backend": "sql"})
    with pytest.raises(ValueError):
        create_storage(cfg)


def test_create_storage_redis_requires_url():
    cfg = TenantConfig(tenant_id="t", name="t", storage={"session_backend": "redis", "redis_url": ""})
    with pytest.raises(ValueError):
        create_storage(cfg)


def test_redact_masks_secret_values():
    payload = {"api_key": "sk-abcdef1234567890", "note": "手机 13812345678"}
    cleaned = redact(payload)
    assert cleaned["api_key"] == "***"
    assert "13812345678" not in cleaned["note"]


def test_audit_emit_and_tail(tmp_path):
    service = AuditService(audit_dir=tmp_path)
    service.emit(
        AuditEvent(tenant_id="t1", channel="web", user_id="u", session_id="s", agent_name="a", tool_name="x")
    )
    service.emit(AuditEvent(tenant_id="t1", decision="block", error_type="budget_exceeded"))
    records = service.tail(10)
    assert len(records) == 2
    assert records[0]["tenant_id"] == "t1"
    assert records[1]["decision"] == "block"


def test_audit_cleanup_by_retention(tmp_path):
    """过期行被删、未过期行保留。"""
    import json as _json
    from datetime import datetime as _dt, timedelta as _td

    service = AuditService(audit_dir=tmp_path)
    service.emit(AuditEvent(tenant_id="t1"))
    service.emit(AuditEvent(tenant_id="t2"))

    # 把第一行手动改成 400 天前，第二行保持新鲜
    lines = service._file.read_text(encoding="utf-8").splitlines()
    records = [_json.loads(line) for line in lines]
    records[0]["created_at"] = (_dt.now() - _td(days=400)).isoformat()
    service._file.write_text(
        "\n".join(_json.dumps(r, ensure_ascii=False) for r in records) + "\n", encoding="utf-8"
    )

    removed = service.cleanup({"t1": 90, "t2": 90})
    assert removed == 1
    remaining = service.tail(10)
    assert len(remaining) == 1
    assert remaining[0]["tenant_id"] == "t2"
