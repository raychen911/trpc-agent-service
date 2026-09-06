"""Strict immutable snapshots for published tenant configuration."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ImmutableTenantConfig(BaseModel):
    """A frozen, JSON-stable boundary object for config publication/auditing."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    tenant_id: str = Field(min_length=1)
    config_version: int = Field(gt=0)
    status: str = Field(min_length=1)
    isolation_mode: str = Field(min_length=1)
    payload_json: str = Field(min_length=2)

    @classmethod
    def from_config(cls, config: Any) -> ImmutableTenantConfig:
        payload = config.to_dict()
        return cls(
            tenant_id=str(payload["tenant_id"]),
            config_version=int(payload["config_version"]),
            status=str(payload["status"]),
            isolation_mode=str(payload["isolation_mode"]),
            payload_json=json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    def payload(self) -> dict[str, Any]:
        return json.loads(self.payload_json)
