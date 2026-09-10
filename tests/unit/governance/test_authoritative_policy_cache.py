import asyncio
from trpc_service.governance.policy import InMemoryGovernancePolicyRepository
from trpc_service.governance.models import PolicyDocument

def test_active_pointer_is_authoritative_after_new_generation():
    async def run():
        r=InMemoryGovernancePolicyRepository(); a=await r.create_version(tenant_id='t',scope='tenant',document=PolicyDocument(allowed_tools={'read'}),actor_digest='a'*64); await r.activate(tenant_id='t',policy_id=a.policy_id,expected_generation=0)
        b=await r.create_version(tenant_id='t',scope='tenant',document=PolicyDocument(allowed_tools=set()),actor_digest='a'*64); await r.activate(tenant_id='t',policy_id=b.policy_id,expected_generation=1)
        return (await r.get_active(tenant_id='t',agent_name='a',binding_id='b')).document.allowed_tools
    assert asyncio.run(run()) == frozenset()
