"""T006 RED: central taxonomy for stages, outcomes, components and roles."""

from __future__ import annotations

import importlib

from tests.observability_support import OBS_ROLES


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


taxonomy = _load("trpc_service.observability.taxonomy")

EXPECTED_STAGES: tuple[str, ...] = (
    "adapter.receive",
    "binding.resolve",
    "gateway.accept",
    "idempotency.claim",
    "session.lock",
    "governance.evaluate",
    "worker.dispatch",
    "runner.invoke",
    "data.access",
    "reply.compose",
    "delivery.queue",
    "delivery.attempt",
    "delivery.result",
    "recovery.reconcile",
)

EXPECTED_OUTCOMES: tuple[str, ...] = (
    "success",
    "rejected",
    "failed",
    "unknown",
    "recovered",
    "not_applicable",
)


def test_taxonomy_module_exists() -> None:
    assert taxonomy is not None, "trpc_service.observability.taxonomy is not implemented yet"


def test_stage_enum_covers_full_pipeline() -> None:
    stages = getattr(taxonomy, "STAGES", None) if taxonomy else None
    assert stages is not None
    assert tuple(stages) == EXPECTED_STAGES


def test_outcome_enum_includes_not_applicable() -> None:
    outcomes = getattr(taxonomy, "OUTCOMES", None) if taxonomy else None
    assert outcomes is not None
    assert tuple(outcomes) == EXPECTED_OUTCOMES
    assert "not_applicable" in outcomes
    assert "not_applicable" != "success"


def test_role_enum_covers_five_platform_roles() -> None:
    roles = getattr(taxonomy, "ROLES", None) if taxonomy else None
    assert roles is not None
    assert tuple(roles) == OBS_ROLES


def test_component_and_stage_validation_reject_unknown_values() -> None:
    validate_stage = getattr(taxonomy, "validate_stage", None) if taxonomy else None
    validate_role = getattr(taxonomy, "validate_role", None) if taxonomy else None
    assert callable(validate_stage)
    assert callable(validate_role)
    assert validate_stage("adapter.receive") == "adapter.receive"
    assert validate_role("gateway") == "gateway"
    for bad in ("rogue.stage", "adapter.receive ", ""):
        try:
            validate_stage(bad)
        except ValueError:
            continue
        raise AssertionError(f"validate_stage must reject unknown stage {bad!r}")
    try:
        validate_role("supernode")
    except ValueError:
        pass
    else:
        raise AssertionError("validate_role must reject unknown role")


def test_stage_of_component_lookup_is_centralised() -> None:
    stage_component = getattr(taxonomy, "stage_component", None) if taxonomy else None
    assert callable(stage_component)
    assert stage_component("runner.invoke") in {"worker", "gateway"}
