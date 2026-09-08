"""Model pricing policy kept separate from application assembly."""

from __future__ import annotations

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from trpc_service.tenant import ModelPricingConfig
from trpc_service.tenant import Tenant


class DefaultModelPricing(BaseModel):
    """Fallback prices for legacy models with no tenant-owned price table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    deepseek_input_per_mtok: float = Field(default=0.22, ge=0)
    deepseek_output_per_mtok: float = Field(default=0.66, ge=0)

    def for_tenant(self, tenant: Tenant) -> dict[str, ModelPricingConfig]:
        """Return explicit tenant prices or provider defaults when absent."""
        if tenant.model.pricing:
            return dict(tenant.model.pricing)
        names = [tenant.model.model_name]
        if tenant.model.fallback_model:
            names.append(tenant.model.fallback_model)
        return {
            name:
            ModelPricingConfig(
                input_per_mtok=self.deepseek_input_per_mtok,
                output_per_mtok=self.deepseek_output_per_mtok,
            )
            for name in names if name.lower().startswith("deepseek-")
        }
