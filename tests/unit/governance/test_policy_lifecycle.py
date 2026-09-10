from __future__ import annotations

import pytest

from trpc_service.governance import policy


def test_policy_document_rejects_unknown_or_dangerous_outside_allowlist() -> None:
    with pytest.raises(ValueError):
        policy.parse_policy_document({"allowed_tools": ["lookup"], "dangerous_tools": ["delete"]})


def test_policy_repository_versions_are_immutable_and_active_generation_is_cas() -> None:
    repository = policy.InMemoryGovernancePolicyRepository()
    document = policy.parse_policy_document({"allowed_tools": ["lookup"]})
    version = repository.create_version_sync("tenant-a", document, actor_digest="a" * 64)
    active = repository.activate_sync("tenant-a", version.policy_id, expected_generation=0)
    assert active.version == version.version
    with pytest.raises(Exception):
        repository.activate_sync("tenant-a", version.policy_id, expected_generation=0)


def test_missing_policy_is_fail_closed() -> None:
    repository = policy.InMemoryGovernancePolicyRepository()
    with pytest.raises(Exception) as exc:
        repository.get_active_sync("tenant-a", "agent", "binding")
    assert getattr(exc.value, "code", "") == "policy_missing"
