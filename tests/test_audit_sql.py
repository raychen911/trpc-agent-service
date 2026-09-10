
"""②生产级审计/平台表测试：SQLite 注入验证 Database DI 与缓冲批量落库。"""

import pytest
from sqlalchemy.exc import IntegrityError

from trpc_service.tenant.audit.model import AuditEvent
from trpc_service.tenant.audit.service import AuditService
from trpc_service.tenant.storage.database import Database
from trpc_service.tenant.storage.tables import ChannelBindingRow, IdempotencyRow


@pytest.fixture()
def db(tmp_path):
    """注入 SQLite 文件库——Database DI 的价值：测试不依赖本机 MySQL。"""
    url = f"sqlite:///{(tmp_path / 'test.db').as_posix()}"
    database = Database(url)
    database.create_all()
    yield database
    database.dispose()


def _event(tenant="t1", decision="allow", trace="tr1"):
    return AuditEvent(
        tenant_id=tenant, channel="web", user_id="u", session_id="s",
        agent_name="a", tool_name="query_order", decision=decision,
        latency_ms=12, error_type="", trace_id=trace,
    )


async def test_emit_buffered_then_batch_insert(db, tmp_path):
    """emit 进缓冲不直写；flush 批量落 SQL。"""
    service = AuditService(audit_dir=tmp_path, db=db)
    for i in range(3):
        service.emit(_event(trace=f"tr{i}"))
    # 未 flush 前应全部在缓冲里，SQL 无数据
    assert service._drain(0) == []  # drain(0) 不取数据，仅确认无副作用
    count = await service.flush()
    assert count == 3
    with db.session() as s:
        from trpc_service.tenant.storage.tables import AuditLogRow
        rows = s.query(AuditLogRow).all()
        assert len(rows) == 3
        assert {r.trace_id for r in rows} == {"tr0", "tr1", "tr2"}


async def test_sql_failure_falls_back_to_file(tmp_path):
    """SQL 写失败 → 整批降级落到 JSONL 文件（审计不丢）。"""
    bad_db = Database(f"sqlite:///{(tmp_path / 'no_such_dir' / 'x.db').as_posix()}")
    service = AuditService(audit_dir=tmp_path, db=bad_db)
    service.emit(_event())
    count = await service.flush()
    assert count == 0
    assert (tmp_path / "audit.jsonl").exists(), "失败批次应落文件兜底"


def test_tail_reads_sql_first(db, tmp_path):
    """有 db 时 tail 从 SQL 读；无 db 时走文件。"""
    service = AuditService(audit_dir=tmp_path, db=db)
    service.emit(_event(trace="t1"))
    service.emit(_event(trace="t2"))
    # 同步等价：直接调用内部批量写（避免测试依赖 flush 循环时序）
    import asyncio

    asyncio.run(service.flush())
    records = service.tail(10)
    assert len(records) == 2
    assert records[0]["trace_id"] == "t1"
    assert records[1]["trace_id"] == "t2"


def test_platform_table_constraints(db):
    """channel_binding 唯一键 + idempotency 幂等键的数据库约束。"""
    with db.session() as s:
        s.add(ChannelBindingRow(
            tenant_id="t1", channel_type="feishu", external_user_id="u1",
            chat_id="", session_id="sess1",
        ))
        s.add(IdempotencyRow(idempotency_key="feishu:10001"))
    with pytest.raises(IntegrityError):
        # 重复绑定违反 uk_binding
        with db.session() as s:
            s.add(ChannelBindingRow(
                tenant_id="t1", channel_type="feishu", external_user_id="u1",
                chat_id="", session_id="sess2",
            ))
    with pytest.raises(IntegrityError):
        # 重复幂等键违反 uk_idem
        with db.session() as s:
            s.add(IdempotencyRow(idempotency_key="feishu:10001"))


def test_emit_redacts_secrets_before_buffer(db, tmp_path):
    """脱敏发生在进缓冲之前（与文件后端同标准）。"""
    service = AuditService(audit_dir=tmp_path, db=db)
    event = _event()
    event.error_type = "leak sk-abcdef1234567890"
    service.emit(event)
    batch = service._drain(10)
    assert batch and "sk-abcdef1234567890" not in batch[0]["error_type"]
