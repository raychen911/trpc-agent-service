from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

import scripts.kubernetes_runtime_gate as runtime_gate
import scripts.release_gate as release_gate
from tests.unit.test_release_gate import _valid_kubernetes_report

_WORKER = "trpc-worker"
_EXTRA_DEPLOYMENT = "trpc-unexpected-deployment"
_OLD_DIGEST = "sha256:" + "c" * 64


def _apply_mutation(report: dict[str, Any], mutation: str) -> None:
    candidate = report["candidate"]
    checks = candidate["checks"]
    rolling = checks["rolling_upgrade"]["image_ids"]
    canonical = candidate["runtime_attestation"]["image_ids"]

    if mutation == "missing_checks_initial":
        checks.pop("initial_image_ids")
    elif mutation == "missing_checks_rolling":
        checks["rolling_upgrade"].pop("image_ids")
    elif mutation == "missing_checks_rolling_initial":
        rolling.pop("initial")
    elif mutation == "missing_checks_rolling_upgrade":
        rolling.pop("upgrade")
    elif mutation == "missing_canonical_initial":
        canonical.pop("initial")
    elif mutation == "missing_canonical_upgrade":
        canonical.pop("upgrade")
    elif mutation == "extra_checks_initial":
        checks["initial_image_ids"][_EXTRA_DEPLOYMENT] = [_OLD_DIGEST]
    elif mutation == "extra_checks_rolling_initial":
        rolling["initial"][_EXTRA_DEPLOYMENT] = [_OLD_DIGEST]
    elif mutation == "extra_checks_rolling_upgrade":
        rolling["upgrade"][_EXTRA_DEPLOYMENT] = [_OLD_DIGEST]
    elif mutation == "extra_canonical_initial":
        canonical["initial"][_EXTRA_DEPLOYMENT] = [_OLD_DIGEST]
    elif mutation == "extra_canonical_upgrade":
        canonical["upgrade"][_EXTRA_DEPLOYMENT] = [_OLD_DIGEST]
    elif mutation == "stale_checks_initial":
        checks["initial_image_ids"][_WORKER] = [_OLD_DIGEST]
    elif mutation == "stale_checks_rolling_initial":
        rolling["initial"][_WORKER] = [_OLD_DIGEST]
    elif mutation == "stale_checks_rolling_upgrade":
        rolling["upgrade"][_WORKER] = [_OLD_DIGEST]
    elif mutation == "stale_canonical_initial":
        canonical["initial"][_WORKER] = [_OLD_DIGEST]
    elif mutation == "stale_canonical_upgrade":
        canonical["upgrade"][_WORKER] = [_OLD_DIGEST]
    elif mutation == "changed_missing":
        rolling.pop("changed")
    elif mutation == "changed_extra":
        rolling["changed"][_EXTRA_DEPLOYMENT] = True
    elif mutation == "changed_false":
        rolling["changed"][_WORKER] = False
    elif mutation == "changed_malformed":
        rolling["changed"][_WORKER] = "true"
    elif mutation == "malformed_checks_initial":
        checks["initial_image_ids"][_WORKER] = ["not-a-digest"]
    elif mutation == "malformed_checks_rolling_initial":
        rolling["initial"][_WORKER] = "not-a-list"
    elif mutation == "malformed_checks_rolling_upgrade":
        rolling["upgrade"][_WORKER] = ["not-a-digest"]
    elif mutation == "malformed_canonical_initial":
        canonical["initial"][_WORKER] = ["not-a-digest"]
    elif mutation == "malformed_canonical_upgrade":
        canonical["upgrade"][_WORKER] = "not-a-list"
    else:
        raise AssertionError(f"unknown image evidence mutation: {mutation}")


def _isolated_valid_report() -> dict[str, Any]:
    """Return valid evidence with duplicated maps deliberately de-aliased."""

    report = deepcopy(_valid_kubernetes_report())
    candidate = report["candidate"]
    checks = candidate["checks"]
    rolling = checks["rolling_upgrade"]["image_ids"]
    canonical = candidate["runtime_attestation"]["image_ids"]
    checks["initial_image_ids"] = deepcopy(checks["initial_image_ids"])
    rolling["initial"] = deepcopy(rolling["initial"])
    rolling["upgrade"] = deepcopy(rolling["upgrade"])
    rolling["changed"] = deepcopy(rolling["changed"])
    canonical["initial"] = deepcopy(canonical["initial"])
    canonical["upgrade"] = deepcopy(canonical["upgrade"])
    return report


def test_valid_dealiased_image_evidence_is_accepted() -> None:
    report = _isolated_valid_report()

    runtime_ok, runtime_reasons = runtime_gate._runtime_attestation_contract(report["candidate"])
    assert runtime_ok, runtime_reasons
    assert release_gate._validate_kubernetes_semantics(report, report["evidence"]) == (None, None)


@pytest.mark.parametrize(
    "mutation",
    (
        "missing_checks_initial",
        "missing_checks_rolling",
        "missing_checks_rolling_initial",
        "missing_checks_rolling_upgrade",
        "missing_canonical_initial",
        "missing_canonical_upgrade",
        "extra_checks_initial",
        "extra_checks_rolling_initial",
        "extra_checks_rolling_upgrade",
        "extra_canonical_initial",
        "extra_canonical_upgrade",
        "stale_checks_initial",
        "stale_checks_rolling_initial",
        "stale_checks_rolling_upgrade",
        "stale_canonical_initial",
        "stale_canonical_upgrade",
        "changed_missing",
        "changed_extra",
        "changed_false",
        "changed_malformed",
        "malformed_checks_initial",
        "malformed_checks_rolling_initial",
        "malformed_checks_rolling_upgrade",
        "malformed_canonical_initial",
        "malformed_canonical_upgrade",
    ),
)
def test_runtime_and_release_gates_fail_closed_on_image_evidence_mismatch(
    mutation: str,
) -> None:
    report = _isolated_valid_report()
    _apply_mutation(report, mutation)

    runtime_ok, runtime_reasons = runtime_gate._runtime_attestation_contract(report["candidate"])
    assert runtime_ok is False, mutation
    assert runtime_reasons, mutation

    release_status, release_reason = release_gate._validate_kubernetes_semantics(
        report, report["evidence"]
    )
    assert release_status in {"not_run", "fail"}, mutation
    assert release_reason, mutation
