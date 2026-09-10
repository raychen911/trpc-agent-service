import asyncio
from datetime import datetime, timezone
from trpc_service.governance.principal import issue_principal, InMemoryPrincipalGrantRepository
from trpc_service.governance.models import PrincipalGrant

def test_unauthorized_principal_is_denied_before_session():
    async def run():
        p=issue_principal(tenant_id='t',channel='feishu',binding_id='b',provider_subject='u'); r=InMemoryPrincipalGrantRepository(); d=await r.evaluate(principal=p,agent_name='a',binding_id='b',at=datetime.now(timezone.utc)); return d.allowed
    assert asyncio.run(run()) is False
