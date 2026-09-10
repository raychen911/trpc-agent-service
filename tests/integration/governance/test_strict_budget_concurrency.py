import asyncio
from decimal import Decimal
from trpc_service.governance.budget import InMemoryBudgetRepository, UsageVector
from trpc_service.governance.errors import BudgetExhausted

def test_budget_concurrency_is_atomic():
    async def run():
        r=InMemoryBudgetRepository({'request':Decimal('1')}); m=UsageVector(request=Decimal('1'))
        async def one(i):
            try: await r.reserve_maximum(tenant_id='t',execution_id=str(i),maximum=m,owner_generation=1); return True
            except BudgetExhausted: return False
        return await asyncio.gather(*(one(i) for i in range(20)))
    assert sum(asyncio.run(run())) == 1
