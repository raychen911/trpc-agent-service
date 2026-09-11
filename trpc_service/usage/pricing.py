"""Strict model-profile pricing configuration (Stage 6C).

Prices live in a JSON file (default ``data/model_pricing.json``, override
with ``TRPC_MODEL_PRICING_PATH``) instead of product code because they
change often.  Validation is deliberately unforgiving:

* unknown fields, ``float``/bool/negative/zero prices and duplicate JSON
  object keys are all rejected (a duplicate key silently overwrites the
  first price — unacceptable for accounting);
* a missing file means "no known prices": tokens keep being metered, cost
  state stays ``unknown``, and any tenant with a hard cost budget is
  fail-closed blocked;
* a present-but-invalid file is a startup error (never a silent
  "everything unknown" fallback that could quietly disable budgets).

Costs are integer micro-currency units.  Per-call cost is computed with
ceiling integer division so unknown precision is never reported cheaper
than reality: ``ceil(tokens * microunits_per_1k / 1000)`` per side.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, StrictInt, ValidationError

DEFAULT_PRICING_PATH: Final[str] = "data/model_pricing.json"
_PRICING_SCHEMA_VERSION: Final[int] = 1


class UsagePricingConfigurationError(ValueError):
    """The pricing file is present but invalid."""


class _ProfilePriceModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    input_microunits_per_1k: StrictInt = Field(ge=0)
    output_microunits_per_1k: StrictInt = Field(ge=0)


class _PricingFileModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: StrictInt
    pricing: dict[str, _ProfilePriceModel]


class ModelPricing:
    """Immutable price table keyed by ``model_profile``."""

    def __init__(self, prices: Mapping[str, tuple[int, int]]) -> None:
        self._prices: dict[str, tuple[int, int]] = dict(prices)

    @classmethod
    def from_path(cls, path: str | Path) -> "ModelPricing":
        p = Path(path)
        if not p.exists():
            return cls({})
        return cls.from_text(p.read_text(encoding="utf-8"))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "ModelPricing":
        import os

        values = os.environ if environ is None else environ
        return cls.from_path(values.get("TRPC_MODEL_PRICING_PATH", DEFAULT_PRICING_PATH))

    @classmethod
    def from_text(cls, text: str) -> "ModelPricing":
        try:
            raw = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
        except (ValueError, TypeError):
            # json/scanner errors can embed offending fragments; never leak
            raise UsagePricingConfigurationError("model pricing file is not valid JSON") from None
        try:
            model = _PricingFileModel.model_validate(raw)
        except ValidationError:
            raise UsagePricingConfigurationError("model pricing file violates the strict schema") from None
        if model.schema_version != _PRICING_SCHEMA_VERSION:
            raise UsagePricingConfigurationError("unsupported model pricing schema_version")
        return cls({
            profile: (price.input_microunits_per_1k, price.output_microunits_per_1k)
            for profile, price in model.pricing.items()
        })

    def price_for(self, model_profile: str) -> tuple[int, int] | None:
        return self._prices.get(model_profile)

    def cost_microunits(self, model_profile: str, input_tokens: int | None, output_tokens: int | None) -> int | None:
        """Total cost or None (= unknown).  Zero tokens reported by the
        provider legitimately produce zero cost — that is a KNOWN zero,
        distinct from the unknown case."""
        price = self.price_for(model_profile)
        if price is None or input_tokens is None or output_tokens is None:
            return None
        input_price, output_price = price
        return (input_tokens * input_price + 999) // 1000 + (output_tokens * output_price + 999) // 1000


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


__all__ = [
    "DEFAULT_PRICING_PATH",
    "ModelPricing",
    "UsagePricingConfigurationError",
]
