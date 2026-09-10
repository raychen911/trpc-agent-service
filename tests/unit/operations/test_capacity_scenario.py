"""T063 RED: the formal capacity scenario is immutable and pinned (FR-023).

The formal acceptance manifest fixes 2 tenants, 2 workers, 100 concurrent
sessions, 10 ordered messages per session (1,000 total), a fixed seed,
message size buckets, duplication ratio, tool ratio and data read/write
ratio, plus warm-up and measurement rounds (DEC-005).
"""

from __future__ import annotations

import importlib


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


models = _load("trpc_service.operations.models")
capacity = _load("trpc_service.operations.capacity")


def test_formal_scenario_is_pinned_to_specification() -> None:
    scenario = capacity.formal_scenario()
    assert scenario.tenant_count == 2
    assert scenario.worker_count == 2
    assert scenario.concurrent_sessions == 100
    assert scenario.messages_per_session == 10
    assert scenario.total_messages() == 1_000
    assert scenario.is_formal_acceptance()
    assert scenario.seed > 0, "the load generator must be seeded deterministically"
    assert scenario.message_size_bucket in models.CapacityScenario.__dataclass_fields__[
        "message_size_bucket"
    ].type or scenario.message_size_bucket in ("small", "medium", "large")
    assert scenario.warmup_rounds >= 1
    assert scenario.measurement_rounds >= 1


def test_scenario_manifest_is_immutable() -> None:
    scenario = capacity.formal_scenario()
    mutated = None
    try:
        scenario.concurrent_sessions = 200  # type: ignore[misc]
        mutated = scenario.concurrent_sessions
    except Exception:
        mutated = "frozen"
    assert mutated == "frozen", "frozen dataclass must reject attribute assignment"


def test_load_generation_is_deterministic_per_seed() -> None:
    scenario = capacity.formal_scenario()
    first = capacity.generate_load(scenario, seed=scenario.seed)
    second = capacity.generate_load(scenario, seed=scenario.seed)
    assert first == second, "same seed must reproduce the identical load plan"
    assert len(first) == scenario.concurrent_sessions
    total = sum(len(session) for session in first)
    assert total == 1_000, "the load plan covers the pinned 1,000 messages"
    third = capacity.generate_load(scenario, seed=scenario.seed + 1)
    assert third != first, "different seeds produce different plans"


def test_load_plan_carries_no_sensitive_values() -> None:
    scenario = capacity.formal_scenario()
    plan = capacity.generate_load(scenario, seed=scenario.seed)
    rendered = repr(plan)
    for sentinel in ("api_key", "im_token", "db_password", "response_url", "secret"):
        assert sentinel not in rendered, "load plans must never embed secrets"
    for session in plan:
        for message in session:
            assert message["size_bucket"] in ("small", "medium", "large")
            assert isinstance(message["ordinal"], int)


def test_load_plan_respects_duplication_and_tool_ratios() -> None:
    scenario = models.CapacityScenario(
        scenario_version="v1",
        tenant_count=2,
        worker_count=2,
        concurrent_sessions=10,
        messages_per_session=10,
        duplication_rate=0.2,
        tool_ratio=0.3,
        seed=7,
        warmup_rounds=1,
        measurement_rounds=1,
    )
    plan = capacity.generate_load(scenario, seed=7)
    all_messages = [message for session in plan for message in session]
    duplicates = [m for m in all_messages if m.get("duplicate_of") is not None]
    tools = [m for m in all_messages if m.get("uses_tool")]
    assert 0 < len(duplicates) <= len(all_messages) * 0.3
    assert 0 < len(tools) <= len(all_messages) * 0.4
