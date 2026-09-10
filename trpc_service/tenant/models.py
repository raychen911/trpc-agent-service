from pydantic import BaseModel, Field
from datetime import datetime


class Tenant(BaseModel):
    tenant_id: str
    name: str
    status: str = "active"
    config: dict = {}
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
