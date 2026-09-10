from .models import Tenant
from abc import ABC, abstractmethod
from typing import List, Optional


class TenantStore(ABC):
    @abstractmethod
    async def create(self, tenant: Tenant) -> Tenant:
        pass

    @abstractmethod
    async def get(self, tenant_id: str) -> Optional[Tenant]:
        pass

    @abstractmethod
    async def list_all(self) -> List[Tenant]:
        pass

    @abstractmethod
    async def update(self, tenant_id: str, data: dict) -> Optional[Tenant]:
        pass

    @abstractmethod
    async def delete(self, tenant_id: str) -> bool:
        pass
