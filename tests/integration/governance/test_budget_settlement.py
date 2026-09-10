import asyncio
from decimal import Decimal
from trpc_service.governance.budget import InMemoryBudgetRepository, UsageVector
def test_budget_settlement_is_idempotent():
    async def run():
        r=InMemoryBudgetRepository({'request':Decimal('5')}); await r.reserve_maximum(tenant_id='t',execution_id='e',maximum=UsageVector(request=Decimal('2')),owner_generation=1); a=await r.settle(tenant_id='t',execution_id='e',actuals=UsageVector(request=Decimal('1')),owner_generation=1); b=await r.settle(tenant_id='t',execution_id='e',actuals=UsageVector(request=Decimal('1')),owner_generation=1); return a.reservation.status,b.reservation.status,r.settled['request']
    a,b,total=asyncio.run(run()); assert a.value=='settled' and b.value=='settled' and total==Decimal('1')
