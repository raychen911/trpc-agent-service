from typing import Dict, List, Optional
from datetime import datetime
from .models import Tenant
from .store import TenantStore


class MemoryTenantStore(TenantStore):
    def __init__(self):
        self._tenants: Dict[str, Tenant] = {}

    async def create(self, tenant: Tenant) -> Tenant:
        self._tenants[tenant.tenant_id] = tenant
        return tenant

    async def get(self, tenant_id: str) -> Optional[Tenant]:
        return self._tenants.get(tenant_id)

    async def list_all(self) -> List[Tenant]:
        return list(self._tenants.values())

    async def update(self, tenant_id: str, data: dict) -> Optional[Tenant]:
        tenant = self._tenants.get(tenant_id)
        if tenant:
            tenant.config.update(data)
            tenant.updated_at = datetime.now()
            return tenant
        return None

    async def delete(self, tenant_id: str) -> bool:
        if tenant_id in self._tenants:
            del self._tenants[tenant_id]
            return True
        return False
