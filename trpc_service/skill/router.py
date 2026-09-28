"""Tenant-visible catalog for reviewed, knowledge-only platform Skills."""

from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from trpc_service.admin.auth import ManagementActor, require_tenant_admin

router = APIRouter(prefix="/tenants/{tenant_id}/skills", tags=["skills"])


class SkillCatalogItem(BaseModel):
    """Safe Skill metadata; executable implementation details stay server-side."""

    name: str
    description: str


class SkillCatalogList(BaseModel):
    items: list[SkillCatalogItem]
    total: int


@router.get("", response_model=SkillCatalogList)
async def list_skills(
        tenant_id: UUID,
        request: Request,
        _: ManagementActor = Depends(require_tenant_admin),
) -> SkillCatalogList:
    """List the small reviewed catalog available for explicit Agent grants."""

    del tenant_id  # Tenant scope is validated by the authorization dependency.
    items = [
        SkillCatalogItem(name=item.name, description=item.description)
        for item in request.app.state.container.skills.summaries()
    ]
    return SkillCatalogList(items=items, total=len(items))
