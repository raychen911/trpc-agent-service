# tenant.budget 模块单元测试（PRD 6-10 成本闭环的持久化层）
import pytest

from trpc_service.tenant.budget import BudgetTracker


class OkTenantStore:

    def __init__(self):
        self.deltas: list[float] = []

    async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
        self.deltas.append(delta_usd)


class BoomTenantStore:

    async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
        raise RuntimeError("db down")


class RegistrySpy:

    def __init__(self):
        self.invalidated: list[str] = []

    def invalidate(self, tenant_id: str) -> None:
        self.invalidated.append(tenant_id)


class BroadcasterSpy:

    def __init__(self):
        self.published: list[str] = []

    async def publish_invalidated(self, tenant_id: str) -> None:
        self.published.append(tenant_id)


@pytest.mark.asyncio
async def test_record_noop_when_non_positive_cost():
    """cost<=0 不触发任何写入（无消耗不必记账）。"""
    store = OkTenantStore()
    await BudgetTracker(tenant_store=store).record("t1", 0)
    await BudgetTracker(tenant_store=store).record("t1", -1)
    assert store.deltas == []


@pytest.mark.asyncio
async def test_record_persists_invalidates_broadcasts():
    """正常路径: SQL 原子累加 + 本地缓存失效 + 跨节点广播。"""
    store = OkTenantStore()
    registry = RegistrySpy()
    broadcaster = BroadcasterSpy()
    tracker = BudgetTracker(tenant_store=store, registry=registry, broadcaster=broadcaster)

    await tracker.record("t1", 3.14)

    assert store.deltas == [3.14]
    assert registry.invalidated == ["t1"]
    assert broadcaster.published == ["t1"]


@pytest.mark.asyncio
async def test_record_without_tenant_store_is_noop():
    """无 tenant_store（纯内存 demo）时 no-op，不抛错。"""
    registry = RegistrySpy()
    await BudgetTracker(registry=registry).record("t1", 1.0)
    assert registry.invalidated == [], "无持久化时不应误失效（配置本身不在 SQL）"


@pytest.mark.asyncio
async def test_record_persist_failure_is_swallowed():
    """持久化失败仅告警，不抛错（记账不阻塞主链路）。"""
    tracker = BudgetTracker(tenant_store=BoomTenantStore())
    await tracker.record("t1", 1.0)  # 不抛异常即通过


@pytest.mark.asyncio
async def test_record_persist_failure_still_invalidates():
    """持久化失败也要失效缓存/广播（审查 09-04 语义反转）。

    旧语义「失败保持旧预算快照」会让各节点一直拿旧预算；新语义失效后
    下一请求回源（拿到的仍是当前真实值），不会因失效而放大错误。
    """
    registry = RegistrySpy()
    broadcaster = BroadcasterSpy()
    tracker = BudgetTracker(tenant_store=BoomTenantStore(), registry=registry, broadcaster=broadcaster)
    await tracker.record("t1", 1.0)  # 不抛异常
    assert registry.invalidated == ["t1"], "失败路径也应失效本地缓存"
    assert broadcaster.published == ["t1"]


class FlakyTenantStore:
    """首次抛错、第二次成功的假存储（验证重试语义）。"""

    def __init__(self):
        self.deltas: list[float] = []
        self.fail_first = True

    async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
        if self.fail_first:
            self.fail_first = False
            raise RuntimeError("db down transient")
        self.deltas.append(delta_usd)


@pytest.mark.asyncio
async def test_record_retries_once_on_transient_failure():
    """持久化首次失败自动重试一次，第二次成功则完成累加（防瞬时 DB 抖动）。"""
    store = FlakyTenantStore()
    await BudgetTracker(tenant_store=store).record("t1", 2.5)
    assert store.deltas == [2.5], "重试后应完成一次累加"


class AlwaysFailStore:

    async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
        raise RuntimeError("db down")


@pytest.mark.asyncio
async def test_record_gives_up_after_retry_without_raise():
    """重试仍失败：告警返回不抛错（预算尽力而为，不阻塞调用方）。"""
    await BudgetTracker(tenant_store=AlwaysFailStore()).record("t1", 1.0)  # 不抛异常即通过
