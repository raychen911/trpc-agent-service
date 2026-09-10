from __future__ import annotations

from trpc_service.storage.models import AdapterOwnershipPhase, AdapterOwnershipState
from trpc_service.web.app import adapter_readiness_payload


def test_adapter_readiness_payload_omits_identity_and_credentials() -> None:
    state = AdapterOwnershipState(
        identity_digest="a" * 64,
        owner_node_id="adapter-a",
        generation=3,
        phase=AdapterOwnershipPhase.READY,
        expires_in_ms=9000,
    )

    payload = adapter_readiness_payload(state)

    assert payload == {
        "readiness": "ready",
        "owner_node_id": "adapter-a",
        "generation": 3,
        "expires_in_ms": 9000,
    }
    assert state.identity_digest not in repr(payload)
