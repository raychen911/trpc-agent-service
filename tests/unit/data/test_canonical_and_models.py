from __future__ import annotations

import json
from datetime import datetime, timezone

from trpc_service.storage import data_models


def test_canonical_json_is_deterministic_and_digestable() -> None:
    canonical = getattr(data_models, "canonical_json", None)
    digest = getattr(data_models, "content_digest", None)
    assert callable(canonical)
    assert callable(digest)
    left = canonical({"b": 1, "a": "é"})
    right = canonical({"a": "é", "b": 1})
    assert left == right == '{"a":"é","b":1}'
    assert digest(left) == digest(right)


def test_data_models_require_utc_and_versioned_content() -> None:
    assert hasattr(data_models, "DataScope")
    assert hasattr(data_models, "SessionStream")
    event = data_models.SessionEvent(
        tenant_id="tenant-a", key="session-a", event_id="event-a", sequence=1,
        value={}, version=1, updated_at=datetime.now(timezone.utc),
    )
    assert getattr(event, "content_digest", None)
    assert event.updated_at.tzinfo is not None


def test_memory_size_boundary_is_explicit() -> None:
    memory = getattr(data_models, "MemoryRecord")
    assert "max_bytes" in memory.model_fields
