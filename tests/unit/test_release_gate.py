from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

import scripts.im_online_gate as im_online_gate
import scripts.kubernetes_runtime_gate as kubernetes_runtime_gate
import scripts.real_runtime_gate as real_runtime_gate
import scripts.release_gate as release_gate
import trpc_service.storage.migration as migration_storage
from scripts.evidence_lineage import canonical_sha256, runtime_fingerprint
from scripts.release_gate import (
    FAULT_REQUIRED_MARKERS,
    FINGERPRINT_MAX_BYTES,
    FINGERPRINT_MAX_FILES,
    FUNCTIONAL_DR_REPORT,
    PRODUCTION_EVIDENCE_PRODUCERS,
    REPORTS,
    _current_candidate_source_fingerprint,
    _status,
    main,
)
from scripts.release_manifest import build_manifest, validate_manifest
from scripts.report_io import atomic_write_json


def test_release_gate_uses_current_production_evidence() -> None:
    assert REPORTS["performance"] == ("real-performance.json", True)
    assert REPORTS["deployment"] == ("kubernetes-runtime.json", True)
    assert REPORTS["fault_injection"] == ("fault-injection.json", True)
    assert REPORTS["online_im"] == ("im-online.json", True)
    assert REPORTS["migration_full_acceptance"] == ("migration-full-acceptance.json", False)
    assert REPORTS["im_resilience_contract"] == ("im-resilience-offline.json", False)
    assert REPORTS["privacy_leak"] == ("privacy-leak-offline.json", False)


def test_release_gate_does_not_select_legacy_performance_report() -> None:
    assert REPORTS["performance"][0] != "real-performance-safe-ramp-10.json"


def test_release_contract_constants_match_runtime_producers() -> None:
    assert tuple(kubernetes_runtime_gate._REQUIRED_RUNTIME_CHECKS) == tuple(
        release_gate.K8S_REQUIRED_CHECKS
    )
    assert kubernetes_runtime_gate._EXPECTED_REDIS_STREAM == "trpc:session-ready:v2"
    assert kubernetes_runtime_gate._EXPECTED_REDIS_GROUP == "trpc-session-ready-v2"
    assert tuple(migration_storage._TARGET_EMPTY_TABLES) == tuple(
        release_gate.MIGRATION_TARGET_EMPTY_TABLES
    )


@pytest.mark.parametrize(
    ("expected", "observed", "matches"),
    (
        ("0.0.0.0:15432", "0.0.0.0:15432", True),
        ("0.0.0.0:15432", "[::]:15432", True),
        ("[::]:15432", "0.0.0.0:15432", True),
        ("0.0.0.0:15432", "[::]:16379", False),
        ("0.0.0.0:15432", "127.0.0.1:15432", False),
        ("0.0.0.0:15432", "[::1]:15432", False),
        ("db.internal:15432", "0.0.0.0:15432", False),
    ),
)
def test_release_gate_toxiproxy_listen_normalization_is_port_and_host_strict(
    expected: str, observed: str, matches: bool
) -> None:
    assert release_gate._toxiproxy_listen_matches(expected, observed) is matches


def test_release_gate_does_not_double_count_real_faults_or_pass_caveats(tmp_path) -> None:
    assert REPORTS["fault_contract"] == ("fault-offline.json", False)

    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "production_gate": "pass",
                "production_rejection_reasons": ["scope caveat, not a failed gate"],
            }
        ),
        encoding="utf-8",
    )
    status, reasons = _status(report, production_field=True)
    assert status == "not_run"
    assert reasons == ["production evidence is missing current-candidate lineage"]

    report.write_text(json.dumps({"production_gate": "not_run"}), encoding="utf-8")
    status, reasons = _status(report, production_field=True)
    assert status == "not_run"
    assert reasons == ["report.json reported production_gate=not_run"]


def _current_evidence(producer: str = "scripts.real_performance_gate") -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "current_candidate",
        "producer": producer,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "run_id": "test-release-gate-run",
        "run_nonce": hashlib.sha256(producer.encode("utf-8")).hexdigest()[:32],
        "source_fingerprint": _current_candidate_source_fingerprint(),
        "runtime_fingerprint": {
            "algorithm": "sha256",
            "status": "available",
            "value": "1" * 64,
        },
        "release_binding": {
            "release_id": "test-release-bundle",
            "nonce_sha256": hashlib.sha256(b"r" * 32).hexdigest(),
        },
    }


def _current_performance_evidence() -> dict[str, object]:
    return _current_evidence()


def _runtime_fingerprint(
    *,
    mode: str,
    workers: list[object],
    participating: list[object] | None = None,
    stream: str,
    group: str,
    parameters: dict[str, object],
) -> dict[str, object]:
    worker_hash = release_gate.canonical_sha256(workers)
    stream_group_hash = release_gate.canonical_sha256({"group": group, "stream": stream})
    parameters_hash = release_gate.canonical_sha256(parameters)
    material = {
        "mode": mode,
        "worker_identity_summary_sha256": worker_hash,
        "stream_group_sha256": stream_group_hash,
        "parameters_sha256": parameters_hash,
    }
    if participating is not None:
        participating_hash = release_gate.canonical_sha256(participating)
        material["participating_identity_summary_sha256"] = participating_hash
    return {
        "algorithm": "sha256",
        "status": "available",
        "value": release_gate.canonical_sha256(material),
        "mode": mode,
        "worker_count": len(workers),
        **material,
        **(
            {
                "participating_service_count": len(
                    {
                        item["role"]
                        for item in participating
                        if isinstance(item, dict) and isinstance(item.get("role"), str)
                    }
                ),
                "participating_container_count": len(participating),
            }
            if participating is not None
            else {}
        ),
    }


def _valid_backend_report() -> dict[str, object]:
    evidence = _current_evidence("scripts.contract_gate")
    evidence["runtime_fingerprint"] = {
        "algorithm": "sha256",
        "status": "available",
        "value": "1" * 64,
        "mode": "backend_contract",
        "worker_count": 1,
        "worker_identity_summary_sha256": "2" * 64,
        "parameters_sha256": "3" * 64,
        "stream_group_sha256": "4" * 64,
    }
    counts = {
        "tests": 4,
        "passed": 4,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }
    image_digest = "sha256:" + "a" * 64
    return {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "run_id": evidence["run_id"],
        "candidate": {
            "kind": "backend",
            "selectors": ["tests/integration"],
            "exit_code": 0,
            "duration_seconds": 1.0,
            "test_counts": counts,
            "backend_identities": {
                "postgres": {"endpoint_sha256": "5" * 64, "resource_sha256": "8" * 64},
                "redis": {"endpoint_sha256": "6" * 64, "resource_sha256": "9" * 64},
                "s3": {"endpoint_sha256": "7" * 64, "resource_sha256": "a" * 64},
            },
            "lineage": {
                "status": "pass",
                "image_digest": image_digest,
                "run_id": evidence["run_id"],
                "runtime_fingerprint_sha256": "1" * 64,
            },
            "runtime_attestation": {
                "status": "pass",
                "run_id": evidence["run_id"],
                "selectors": ["tests/integration"],
                "junit_counts": counts,
                "image_digest": image_digest,
                "runtime_fingerprint_sha256": "1" * 64,
            },
        },
        "case_deltas": {"failed_processes": 0},
        "evidence": evidence,
    }


def _materialize_fault_children(payload: dict[str, object], report_path: Path) -> None:
    """Create the trusted run-scoped child artifacts used by release fixtures."""

    evidence = payload["evidence"]
    assert isinstance(evidence, dict)
    generated_at = str(evidence["generated_at"])
    scenarios = payload["candidate"]["scenarios"]  # type: ignore[index]
    assert isinstance(scenarios, dict)
    trusted_root = report_path.parent / "fault-evidence"
    stage_cases: list[dict[str, object]] = []
    stage_provenance: dict[str, object] | None = None
    stage_nonce = hashlib.sha256(b"fixture-stage-nonce").hexdigest()
    for scenario_name, stage in {
        "worker_enqueue": "enqueue",
        "worker_tool": "tool",
        "worker_commit": "commit_txn_open",
    }.items():
        item = scenarios[scenario_name]
        assert isinstance(item, dict)
        case_identity = {
            "case_id": item["case_id"],
            "run_id": "fault-stage-run",
            "tenant_id": f"fault-{scenario_name}",
            "session_id": f"session-{scenario_name}",
            "inbound_id": f"inbound-{scenario_name}",
            "message_id": f"message-{scenario_name}",
        }
        control_id = f"control-{scenario_name}"
        killed_container = "worker-container"
        markers = [
            {"name": marker, "status": "pass", "observed_at": generated_at}
            for marker in FAULT_REQUIRED_MARKERS[scenario_name]
        ]
        stage_cases.append(
            {
                "stage": stage,
                "status": "pass",
                "case": case_identity,
                "control_id": control_id,
                "killed_container_id": killed_container,
                "markers": markers,
            }
        )
        item["case_identity_sha256"] = canonical_sha256(case_identity)
        item["control_id_sha256"] = hashlib.sha256(control_id.encode()).hexdigest()
        item["killed_container_sha256"] = hashlib.sha256(killed_container.encode()).hexdigest()
        stage_provenance = {
            "schema_version": 1,
            "run_id": "fault-stage-run",
            "project": "trpc-fault-fixture",
            "worker_container": killed_container,
            "scheduler_version": "v2",
            "redis_stream": "trpc:session-ready:v2",
            "redis_group": "trpc-session-ready-v2",
            "nonce_sha256": stage_nonce,
            "pid": 1234,
        }
        item["child_nonce_sha256"] = stage_nonce
        item["child_provenance"] = {
            "schema_version": 1,
            "run_id": "fault-stage-run",
            "scheduler_version": "v2",
            "redis_stream": "trpc:session-ready:v2",
            "redis_group": "trpc-session-ready-v2",
            "nonce_sha256": stage_nonce,
            "project_sha256": hashlib.sha256(b"trpc-fault-fixture").hexdigest(),
            "worker_container_sha256": hashlib.sha256(killed_container.encode()).hexdigest(),
            "pid": 1234,
        }
    stage_child = {
        "schema_version": 1,
        "mode": "fault_stage_acceptance",
        "run_id": "fault-stage-run",
        "run_nonce_sha256": stage_nonce,
        "started_at": generated_at,
        "ended_at": generated_at,
        "gate": "pass",
        "production_gate": "pass",
        "execution_provenance": stage_provenance,
        "cases": stage_cases,
    }
    stage_scope = trusted_root / "fault-stage-run"
    stage_scope.mkdir(parents=True, exist_ok=True)
    atomic_write_json(stage_scope / "fault-stage.child.json", stage_child)

    for scenario_name, item_value in scenarios.items():
        if scenario_name in {"worker_enqueue", "worker_tool", "worker_commit"}:
            item = item_value
            assert isinstance(item, dict)
            item["child_report"] = str(stage_scope / "fault-stage.child.json")
            item["child_report_path_scope"] = str(stage_scope)
            item["child_report_sha256"] = canonical_sha256(stage_child)
            item["child_report_mtime_ns"] = (
                (stage_scope / "fault-stage.child.json").stat().st_mtime_ns
            )
            item["child_report_started_at"] = generated_at
            item["child_report_ended_at"] = generated_at
            continue
        item = item_value
        assert isinstance(item, dict)
        run_id = str(item["child_run_id"])
        nonce = hashlib.sha256(f"fixture-{scenario_name}".encode()).hexdigest()
        expected_phase = (
            "ambiguous"
            if scenario_name == "ambiguous"
            else "load"
            if scenario_name == "fencing"
            else "fault"
        )
        child_phase: dict[str, object] = {
            "status": "pass",
            "stage_markers": item["stage_markers"],
        }
        if scenario_name == "ambiguous":
            child_phase.update(item["evidence"])  # type: ignore[arg-type]
        elif scenario_name == "fencing":
            child_phase["fencing_takeover"] = item["evidence"]
        else:
            component = {
                "redis_interrupt": "redis",
                "republish": "redis",
                "dlq": "dlq",
            }[scenario_name]
            child_phase[component] = item["evidence"]
            if scenario_name == "republish":
                child_phase["republish_duplicate_publish_probe"] = item["evidence"][
                    "duplicate_publish_probe"
                ]  # type: ignore[index]
        child = {
            "schema_version": 1,
            "run_id": run_id,
            "run_nonce": f"fixture-{scenario_name}",
            "started_at": generated_at,
            "ended_at": generated_at,
            "gate": "pass",
            "production_gate": "not_run",
            "candidate": {"faults" if expected_phase == "fault" else expected_phase: child_phase},
            "case_deltas": {"requested_phase": expected_phase},
        }
        scope = trusted_root / run_id
        scope.mkdir(parents=True, exist_ok=True)
        child_path = scope / f"{scenario_name}.child.json"
        atomic_write_json(child_path, child)
        item["child_report"] = str(child_path)
        item["child_report_path_scope"] = str(scope)
        item["child_report_sha256"] = canonical_sha256(child)
        item["child_nonce_sha256"] = nonce
        item["child_report_mtime_ns"] = child_path.stat().st_mtime_ns
        item["child_report_started_at"] = generated_at
        item["child_report_ended_at"] = generated_at
        item["child_phase"] = expected_phase
        item["child_phase_status"] = "pass"
        item["child_production_gate"] = "not_run"


def _valid_fault_report(report_path: Path | None = None) -> dict[str, object]:
    evidence = _current_evidence("scripts.fault_injection_gate")
    fault_scope = str(Path.cwd() / "fault-evidence")
    fault_child_report = str(Path(fault_scope) / "child.json")
    worker_stages = {
        "worker_enqueue": "enqueue",
        "worker_tool": "tool",
        "worker_commit": "commit_txn_open",
    }
    scenarios: dict[str, object] = {}
    for scenario, markers in FAULT_REQUIRED_MARKERS.items():
        item: dict[str, object] = {
            "status": "pass",
            "stage_markers": [
                {
                    "name": marker,
                    "status": "pass",
                    "observed_at": evidence["generated_at"],
                }
                for marker in markers
            ],
        }
        if scenario in worker_stages:
            item.update(
                {
                    "mode": "real_fault_stage_acceptance",
                    "stage": worker_stages[scenario],
                    "child_schema_version": 1,
                    "child_mode": "fault_stage_acceptance",
                    "child_gate": "pass",
                    "child_production_gate": "pass",
                    "exit_code": 0,
                    "case_status": "pass",
                    "run_id": "fault-stage-run",
                    "child_run_id": "fault-stage-run",
                    "case_id": f"case-{scenario}",
                    "child_started_at": evidence["generated_at"],
                    "child_ended_at": evidence["generated_at"],
                    "child_report_started_at": evidence["generated_at"],
                    "child_report_ended_at": evidence["generated_at"],
                    "child_report": fault_child_report,
                    "child_report_path_scope": fault_scope,
                    "child_report_path_confined": True,
                    "child_report_sha256": "9" * 64,
                    "child_nonce_sha256": "6" * 64,
                    "observed_exit_code": 0,
                    "child_identity_verified": True,
                    "child_timestamps_verified": True,
                    "case_identity_sha256": "3" * 64,
                    "control_id_sha256": "4" * 64,
                    "killed_container_sha256": "5" * 64,
                    "child_provenance": {
                        "schema_version": 1,
                        "run_id": "fault-stage-run",
                        "scheduler_version": "v2",
                        "redis_stream": "trpc:session-ready:v2",
                        "redis_group": "trpc-session-ready-v2",
                        "nonce_sha256": "6" * 64,
                        "project_sha256": "7" * 64,
                        "worker_container_sha256": "8" * 64,
                        "pid": 1234,
                    },
                }
            )
        else:
            execution_evidence: dict[str, object] = {"status": "pass"}
            if scenario in {"redis_interrupt", "republish"}:
                execution_evidence.update(
                    {
                        "completion_after_restore": {"status": "pass"},
                        "duplicate_turns_verified": True,
                    }
                )
            if scenario == "republish":
                execution_evidence["duplicate_publish_probe"] = {"status": "pass"}
            if scenario == "fencing":
                execution_evidence.update(
                    {
                        "takeover_observed": True,
                        "takeover_owner_differs": True,
                        "old_token_rejection": {"status": "pass"},
                    }
                )
            if scenario == "dlq":
                execution_evidence.update(
                    {
                        "retry_attempts_increased": True,
                        "terminal_path": "exhausted_retry_terminal_path",
                    }
                )
            if scenario == "ambiguous":
                execution_evidence.update(
                    {
                        "manual_confirmation_required": True,
                        "automatic_replay_count": 0,
                        "confirmed_replay_status": "pass",
                        "provider_ledger": {
                            "accepted_count": 1,
                            "side_effect_count": 1,
                            "duplicate_replay_count": 1,
                        },
                    }
                )
            item.update(
                {
                    "run_id": f"real-child-{scenario}",
                    "child_run_id": f"real-child-{scenario}",
                    "child_started_at": evidence["generated_at"],
                    "child_ended_at": evidence["generated_at"],
                    "child_report_started_at": evidence["generated_at"],
                    "child_report_ended_at": evidence["generated_at"],
                    "child_report": fault_child_report,
                    "child_report_path_scope": fault_scope,
                    "child_report_path_confined": True,
                    "child_report_sha256": "9" * 64,
                    "child_nonce_sha256": "6" * 64,
                    "observed_exit_code": 0,
                    "child_identity_verified": True,
                    "child_timestamps_verified": True,
                    "child_gate": "pass",
                    "exit_code": 0,
                    "child_report_mtime_ns": 1,
                    "evidence": execution_evidence,
                }
            )
        scenarios[scenario] = item
    inventory = list(FAULT_REQUIRED_MARKERS)
    payload: dict[str, object] = {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "run_id": evidence["run_id"],
        "candidate": {
            "mode": "real_compose_fault_injection",
            "requested_scenario": "all",
            "scenarios": scenarios,
            "lineage": {"status": "pass", "image_digest": "sha256:" + "a" * 64},
        },
        "case_deltas": {"requested": inventory, "passed": inventory},
        "evidence": evidence,
    }
    if report_path is not None:
        _materialize_fault_children(payload, report_path)
    return payload


def _install_release_probe_trust(tmp_path, monkeypatch) -> dict[str, str]:
    public_key = bytes(range(1, 33))
    document = {
        "schema_version": 1,
        "probe_url": "https://probe.example.test",
        "key_id": "fixture-key",
        "ed25519_public_key": base64.b64encode(public_key).decode("ascii"),
    }
    trust_path = tmp_path / "deploy" / "im-probe-trust.json"
    atomic_write_json(trust_path, document)
    monkeypatch.setattr(release_gate, "IM_PROBE_TRUST_PATH", trust_path)
    return {
        "probe_url": document["probe_url"],
        "key_id": document["key_id"],
        "key_sha256": hashlib.sha256(public_key).hexdigest(),
        "config_sha256": canonical_sha256(document),
        "file_sha256": hashlib.sha256(trust_path.read_bytes()).hexdigest(),
    }


def _valid_online_im_report(trust: dict[str, str] | None = None) -> dict[str, object]:
    evidence = _current_evidence("scripts.im_online_gate")
    nonce = "online-im-run-nonce-123456"
    observed_at = evidence["generated_at"]
    evidence["runtime_fingerprint"] = {
        "algorithm": "sha256",
        "status": "available",
        "value": "a" * 64,
    }

    def observation(case: str, channel: str) -> dict[str, object]:
        result: dict[str, object] = {
            "status": "pass",
            "run_nonce": nonce,
            "provider_event_id_hash": "2" * 64,
            "observed_at": observed_at,
        }
        result.update(
            {
                "round_trip": {
                    "callback_event_id_hash": "3" * 64,
                    "outbound_request_id_hash": "4" * 64,
                    "provider_code": "ok",
                },
                "idempotency": {
                    "duplicate_event_id_hash": "5" * 64,
                    "unique_inbound_id_hash": "6" * 64,
                    "duplicate_count": 1,
                },
                "media": {"bytes": 1024},
                "reconnect": (
                    {
                        "failed_endpoint_id_hash": "7" * 64,
                        "replacement_endpoint_id_hash": "8" * 64,
                        "endpoint_set_observed": True,
                        "received_after_failover_event_id_hash": "9" * 64,
                        "outbound_request_id_hash": "0" * 64,
                        "acknowledged_request_id_hash": "0" * 64,
                        "ready_endpoint_count": 4,
                        "unready_endpoint_count": 0,
                        "terminating_endpoint_count": 0,
                    }
                    if channel == "feishu"
                    else {
                        "disconnect_event_id_hash": "7" * 64,
                        "reconnect_event_id_hash": "8" * 64,
                        "received_after_reconnect_event_id_hash": "9" * 64,
                        "lock_takeover_event_id_hash": "0" * 64,
                        "old_lock_owner_released": True,
                        "new_lock_owner_acquired": True,
                        "lock_epoch": 2,
                    }
                ),
                "rate_limit_retry_after": {
                    "provider_error_code": "99991400",
                    "retry_after_seconds": 2.0,
                    "retry_request_id_hash": "a" * 64,
                    "retry_attempts": 2,
                    "retry_elapsed_seconds": 2.0,
                },
                "credential_rotation": {
                    "old_credential_event_id_hash": "b" * 64,
                    "new_credential_event_id_hash": "c" * 64,
                    "post_rotation_event_id_hash": "d" * 64,
                    "old_credential_rejected": True,
                },
                "prolonged_outage": {
                    "outage_event_id_hash": "e" * 64,
                    "recovery_event_id_hash": "f" * 64,
                    "outage_seconds": 120.0,
                },
                "ambiguous": {
                    "ambiguous_event_id_hash": "1" * 64,
                    "manual_review_id_hash": "2" * 64,
                    "drop_response_observed": True,
                    "auto_replay_count": 0,
                },
            }[case]
        )
        return result

    contracts = {
        "feishu": ("feishu_api_and_webhook", ["provider_callback", "provider_send_ack"], 4),
        "wecom": ("wecom_ws_and_send_ack", ["provider_ws_event", "provider_send_ack"], 2),
    }
    cases = (
        "round_trip",
        "idempotency",
        "media",
        "reconnect",
        "rate_limit_retry_after",
        "credential_rotation",
        "prolonged_outage",
        "ambiguous",
    )
    channels: dict[str, object] = {}
    for channel, (source, paths, credential_count) in contracts.items():
        response_digest = ("1" if channel == "feishu" else "2") * 64
        artifact_attestation = {
            "schema_version": 1,
            "runner_sha256": "b" * 64,
            "runner_contract_version": 1,
            "driver_sha256": ("c" if channel == "feishu" else "d") * 64,
            "driver_contract_version": 1,
        }
        trust_values = trust or {
            "probe_url": "https://probe.example.test",
            "key_id": "fixture-key",
            "key_sha256": "8" * 64,
            "config_sha256": "9" * 64,
            "file_sha256": "a" * 64,
        }
        observations = {case: observation(case, channel) for case in cases}
        if channel == "wecom":
            observations["rate_limit_retry_after"]["provider_error_code"] = "45009"
            for case in ("reconnect", "credential_rotation"):
                observations[case].update(
                    {
                        "outbound_request_id_hash": "4" * 64,
                        "acknowledged_request_id_hash": "4" * 64,
                        "provider_code": "0",
                    }
                )
            observations["prolonged_outage"].update(
                {
                    "outage_mode": "service_failover",
                    "failed_instance_id_hash": "1" * 64,
                    "takeover_instance_id_hash": "2" * 64,
                    "old_lock_owner_released": True,
                    "new_lock_owner_acquired": True,
                    "connection_epoch": 3,
                    "event_during_outage_id_hash": "3" * 64,
                    "reply_for_event_id_hash": "3" * 64,
                    "outbound_request_id_hash": "4" * 64,
                    "acknowledged_request_id_hash": "4" * 64,
                    "reply_count": 1,
                    "ack_count": 1,
                    "pending_count": 0,
                    "dlq_count": 0,
                }
            )
        channels[channel] = {
            "status": "pass",
            "runtime_attestation": {
                "status": "pass",
                "run_nonce": nonce,
                "image_digest": "sha256:" + "a" * 64,
                "release_id": evidence["release_binding"]["release_id"],  # type: ignore[index]
                "release_nonce_sha256": evidence["release_binding"][  # type: ignore[index]
                    "nonce_sha256"
                ],
                "source_fingerprint": evidence["source_fingerprint"]["value"],  # type: ignore[index]
            },
            "artifact_attestation": dict(artifact_attestation),
            "cases": {case: {"status": "pass"} for case in cases},
            "provider_evidence": {
                "source": source,
                "independent_paths": paths,
                "run_nonce": nonce,
                "artifact_attestation": dict(artifact_attestation),
                "run_started_at": observed_at,
                "account_fingerprint": "3" * 64,
                "credential_attestation": {
                    "status": "pass",
                    "run_nonce": nonce,
                    "credential_count": credential_count,
                },
                "observations": observations,
            },
            "signature_response": {
                "algorithm": "sha256",
                "response_sha256": response_digest,
                "binding_sha256": release_gate._im_response_digest_binding(
                    channel=channel,
                    run_id=evidence["run_id"],
                    run_nonce=nonce,
                    response_sha256=response_digest,
                    trust=trust_values,
                ),
            },
        }
    return {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "run_id": evidence["run_id"],
        "candidate": {
            "mode": "real_feishu_wecom_online",
            "runtime_configured": True,
            "runtime_image_digest": "a" * 64,
            "probe": {
                "status": "pass",
                "endpoint_configured": True,
                "endpoint_allowlisted": True,
                "identity_attestation": {
                    "status": "pass",
                    "run_nonce": nonce,
                    "identity_sha256": "7" * 64,
                    "identity_source": "TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256",
                    "channels": ["feishu", "wecom"],
                    "signature_verified": True,
                    "signed_channels": ["feishu", "wecom"],
                    "signature_algorithm": "ed25519",
                    "trust_key_id": trust_values["key_id"],
                    "trust_probe_url": trust_values["probe_url"],
                    "trust_key_sha256": trust_values["key_sha256"],
                    "trust_config_sha256": trust_values["config_sha256"],
                    "trust_file_sha256": trust_values["file_sha256"],
                },
            },
            "channels": channels,
        },
        "case_deltas": {"failed_cases": []},
        "evidence": evidence,
    }


def _sanitized_feishu_probe_evidence(*, run_nonce: str, observed_at: str) -> dict[str, object]:
    common = {"status": "pass", "run_nonce": run_nonce, "observed_at": observed_at}
    observations: dict[str, dict[str, object]] = {
        "round_trip": {
            **common,
            "provider_event_id": "feishu-round-trip",
            "callback_event_id": "feishu-callback",
            "outbound_request_id": "feishu-round-trip-request",
            "provider_code": 0,
        },
        "idempotency": {
            **common,
            "provider_event_id": "feishu-idempotency",
            "duplicate_event_id": "feishu-delivery",
            "unique_inbound_id": "feishu-inbound",
            "duplicate_count": 1,
            "original_event_id": "feishu-delivery",
            "provider_delivery_count": 2,
        },
        "media": {
            **common,
            "provider_event_id": "feishu-media",
            "media_id_hash": "1" * 64,
            "sha256": "2" * 64,
            "bytes": 1024,
        },
        "reconnect": {
            **common,
            "provider_event_id": "feishu-reconnect",
            "failed_endpoint_id": "feishu-gateway-old",
            "replacement_endpoint_id": "feishu-gateway-new",
            "endpoint_set_observed": True,
            "received_after_failover_event_id": "feishu-after-failover",
            "outbound_request_id": "feishu-failover-request",
            "acknowledged_request_id": "feishu-failover-request",
            "ready_endpoint_count": 4,
            "unready_endpoint_count": 0,
            "terminating_endpoint_count": 0,
        },
        "rate_limit_retry_after": {
            **common,
            "provider_event_id": "feishu-rate-limit",
            "provider_error_code": 429,
            "retry_after_seconds": 1.0,
            "retry_request_id": "feishu-retry-request",
            "retry_attempts": 2,
            "retry_elapsed_seconds": 1.0,
        },
        "credential_rotation": {
            **common,
            "provider_event_id": "feishu-rotation",
            "old_credential_event_id": "feishu-old-credential",
            "new_credential_event_id": "feishu-new-credential",
            "post_rotation_event_id": "feishu-post-rotation",
            "old_credential_rejected": True,
        },
        "prolonged_outage": {
            **common,
            "provider_event_id": "feishu-outage",
            "outage_event_id": "feishu-outage-event",
            "recovery_event_id": "feishu-recovery-event",
            "outage_seconds": 60.0,
        },
        "ambiguous": {
            **common,
            "provider_event_id": "feishu-ambiguous",
            "ambiguous_event_id": "feishu-ambiguous-event",
            "manual_review_id": "feishu-manual-review",
            "drop_response_observed": True,
            "auto_replay_count": 0,
        },
    }
    fingerprints = {
        "FEISHU_APP_ID": "3" * 64,
        "FEISHU_APP_SECRET": "4" * 64,
        "FEISHU_VERIFICATION_TOKEN": "5" * 64,
        "FEISHU_ENCRYPT_KEY": "6" * 64,
    }
    response = {
        "credential_attestation": {
            "status": "pass",
            "run_nonce": run_nonce,
            "fingerprints": fingerprints,
        },
        "provider_evidence": {
            "source": "feishu_api_and_webhook",
            "independent_paths": ["provider_callback", "provider_send_ack"],
            "run_nonce": run_nonce,
            "artifact_attestation": {
                "schema_version": 1,
                "runner_sha256": "b" * 64,
                "runner_contract_version": 1,
                "driver_sha256": "c" * 64,
                "driver_contract_version": 1,
            },
            "account_fingerprint": fingerprints["FEISHU_APP_ID"],
            "observations": observations,
        },
    }
    current = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    sanitized, errors = im_online_gate._validate_provider_evidence(
        "feishu",
        response,
        run_nonce=run_nonce,
        credential_fingerprints=fingerprints,
        run_started_at=current,
        now=current,
    )
    assert errors == []
    assert sanitized is not None
    return sanitized


def _performance_candidate() -> dict[str, object]:
    run_id = "test-release-gate-run"
    inbound_ids = [f"inbound-{index}" for index in range(200)]
    session_ids = [f"session-{index}" for index in range(200)]
    burst_inbound_ids = [f"burst-inbound-{index}" for index in range(200)]
    burst_session_ids = [f"burst-session-{index}" for index in range(200)]
    source = _current_candidate_source_fingerprint()
    source_value = source["value"]
    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    workers = [
        {
            "container_id": f"container-{index}",
            "pid": 1000 + index,
            "image_id": "sha256:" + "a" * 64,
            "source_fingerprint": source_value,
        }
        for index in range(4)
    ]
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 200},
            "turn_statuses": {"committed": 200},
            "scheduler_version": "v2",
            "published_scheduler_outbox": 200,
            "leased_sessions": 0,
            "mailbox_expected_count": 200,
            "mailbox_row_count": 200,
            "mailbox_idle_count": 200,
            "mailbox_settled_count": 200,
            "mailbox_unresolved_item_count": 0,
        },
    }
    sustained = {
        "ingress_mode": "synthetic_encrypted_feishu_http",
        "requested_callbacks": 200,
        "offered_callback_rate_per_second": 100.0,
        "accepted_callbacks": 200,
        "accepted_inbound_ids": inbound_ids,
        "session_ids": session_ids,
        "unique_inbound_id_count": 200,
        "duplicate_inbound_id_count": 0,
        "unique_session_id_count": 200,
        "duplicate_session_id_count": 0,
        "ack_p95_ms": 50.0,
        "actual_submission_start_rate_per_second": 100.0,
        "accepted_external_message_id_count": 200,
        "gateway": {"host_class": "loopback", "scheme": "http", "port": 18080},
        "http_status_counts": {"200": 200},
        "http_failure_counts": {},
        "timed_out": False,
        "callback_submission_started_at": timestamp,
        "callback_submission_last_started_at": timestamp,
        "authoritative_lookup": {
            "status": "pass",
            "requested_count": 200,
            "row_count": 200,
            "missing_count": 0,
            "duplicate_count": 0,
            "inbound_ids": inbound_ids,
            "session_ids": session_ids,
        },
        "completion": completion,
        "gate": {"status": "pass"},
    }
    burst = {
        "requested_concurrent_turns": 200,
        "accepted_callbacks": 200,
        "accepted_inbound_ids": burst_inbound_ids,
        "session_ids": burst_session_ids,
        "unique_inbound_id_count": 200,
        "duplicate_inbound_id_count": 0,
        "unique_session_id_count": 200,
        "duplicate_session_id_count": 0,
        "ack_p95_ms": 50.0,
        "actual_submission_start_rate_per_second": 100.0,
        "max_turn_overlap_observed": 200,
        "errors": 0,
        "timed_out": False,
        "callback_submission_started_at": timestamp,
        "completion": completion,
        "gate": {"status": "pass"},
    }
    memory_roles = {
        "worker": [
            {
                "role": "worker",
                "pid": 1000 + index,
                "container_id": f"container-{index}",
                "rss_peak_bytes": 64 * 1024**2,
                "cgroup_peak_bytes": None,
                "cgroup_limit_bytes": None,
            }
            for index in range(4)
        ],
        "outbox-dispatcher": [
            {
                "role": "outbox-dispatcher",
                "pid": 3001,
                "container_id": "outbox-1",
                "rss_peak_bytes": 32 * 1024**2,
                "cgroup_peak_bytes": None,
                "cgroup_limit_bytes": None,
            }
        ],
    }
    memory_observation = {
        "status": "pass",
        "sampling_method": "kernel_peak_counters",
        "sample_count": 1,
        "sampling_interval_seconds": 0.25,
        "required_roles": list(memory_roles),
        "role_observations": {
            role: {
                "identity_count": len(items),
                "observed_count": len(items),
                "observations": items,
            }
            for role, items in memory_roles.items()
        },
        "coverage_complete": True,
        "observed_identity_count": 5,
        "cgroup_identity_count": 0,
        "cgroup_scope_count": 0,
        "peak_rss_bytes": 288 * 1024**2,
        "peak_cgroup_bytes": None,
        "peak_bytes": 288 * 1024**2,
        "safety_threshold_bytes": 8 * 1024**3,
        "threshold_source": "available_memory",
        "within_safety_threshold": True,
        "observed_at": timestamp,
    }
    return {
        "mode": "real_postgresql_redis_multiprocess",
        "run_id": run_id,
        "parameters": {
            "db_pool_size": 32,
            "min_workers": 4,
            "max_inflight": 64,
            "timeout_seconds": 300.0,
            "callbacks": 200,
            "callback_rate_per_second": 100.0,
            "burst_turns": 200,
            "target_max_turn_overlap": 200,
            "max_inflight_accepts": 64,
            "db_pool_scope": "load_generator_only",
            "scheduler_version": "v2",
            "redis_stream": "trpc:session-ready:v2",
            "redis_group": "trpc-session-ready-v2",
        },
        "preflight": {
            "status": "pass",
            "worker_count": 4,
            "worker_concurrency": 50,
            "worker_processes": workers,
            "source_fingerprint": source,
            "resources": {
                "cpu_count": 4,
                "available_memory_bytes": 8 * 1024**3,
                "required_memory_bytes": 2 * 1024**3,
                "estimated_runtime_connections": 102,
                "max_estimated_runtime_connections": 128,
            },
            "worker_image_attestation": {
                "status": "pass",
                "worker_count": 4,
                "image_count": 1,
                "source_fingerprint_matches": True,
            },
            "participating_processes": {
                role: [
                    {
                        "role": item["role"],
                        "pid": item["pid"],
                        "container_id": item.get("container_id"),
                    }
                    for item in items
                ]
                for role, items in memory_roles.items()
            },
        },
        "pool_prewarmed": True,
        "memory_observation": memory_observation,
        "warmup": {
            "passed": True,
            "excluded_from_burst_overlap": True,
            "stages": [
                {
                    "requested": count,
                    "accepted": count,
                    "errors": 0,
                    "completion": {"status": "pass"},
                }
                for count in (1, 4, 8)
            ],
        },
        "sustained": sustained,
        "burst": burst,
        "redis": {
            "baseline": {"pending": 0},
            "after_burst": {"pending": 0},
            "baseline_pending_is_zero": True,
            "final_pending_is_zero": True,
        },
    }


def _valid_performance_report() -> dict[str, object]:
    candidate = _performance_candidate()
    evidence = _current_performance_evidence()
    preflight = candidate["preflight"]  # type: ignore[index]
    workers = preflight["worker_processes"]  # type: ignore[index]
    parameters = candidate["parameters"]  # type: ignore[index]
    evidence["runtime_fingerprint"] = _runtime_fingerprint(
        mode="real_postgresql_redis_multiprocess",
        workers=workers,  # type: ignore[arg-type]
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
        parameters=parameters,  # type: ignore[arg-type]
    )
    return {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "candidate": candidate,
        "evidence": evidence,
        "rejection_reasons": [],
        "production_rejection_reasons": [],
    }


def _valid_kubernetes_performance_report() -> dict[str, object]:
    report = _valid_performance_report()
    candidate = report["candidate"]
    assert isinstance(candidate, dict)
    preflight = candidate["preflight"]
    assert isinstance(preflight, dict)
    source = _current_candidate_source_fingerprint()["value"]

    worker_processes = [
        {
            "role": "worker",
            "pod_name": f"worker-{index}",
            "pod_uid": f"pod-uid-worker-{index}",
            "container_name": "worker",
            "container_id": f"containerd://worker-{index}",
            "image_id": "sha256:" + "a" * 64,
            "source_fingerprint": None,
            "memory_limit_bytes": 2 * 1024**3,
        }
        for index in range(4)
    ]
    outbox_processes = [
        {
            "role": "outbox-dispatcher",
            "pod_name": "outbox-0",
            "pod_uid": "pod-uid-outbox-0",
            "container_name": "outbox-dispatcher",
            "container_id": "containerd://outbox-0",
            "image_id": "sha256:" + "a" * 64,
            "source_fingerprint": source,
            "memory_limit_bytes": 1 * 1024**3,
        }
    ]
    participating = {"worker": worker_processes, "outbox-dispatcher": outbox_processes}
    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    role_observations = {
        "worker": {
            "identity_count": len(worker_processes),
            "observed_count": len(worker_processes),
            "observations": [
                {
                    **identity,
                    "memory_bytes": 128 * 1024**2,
                    "sampled_memory_bytes": 128 * 1024**2,
                    "sampling_source": "kubernetes_metrics_api",
                    "metrics_timestamp": timestamp,
                    "metrics_window": "15s",
                }
                for identity in worker_processes
            ],
        },
        "outbox-dispatcher": {
            "identity_count": len(outbox_processes),
            "observed_count": len(outbox_processes),
            "observations": [
                {
                    **outbox_processes[0],
                    "memory_bytes": 64 * 1024**2,
                    "sampled_memory_bytes": 64 * 1024**2,
                    "sampling_source": "kubernetes_metrics_api",
                    "metrics_timestamp": timestamp,
                    "metrics_window": "15s",
                }
            ],
        },
    }
    memory_observation = {
        "status": "pass",
        "sampling_method": "kubernetes_metrics_api",
        "metrics_api": "metrics.k8s.io/v1beta1",
        "sample_count": 1,
        "sampling_interval_seconds": 15.0,
        "sample_timestamps": [timestamp],
        "required_roles": ["worker", "outbox-dispatcher"],
        "role_observations": role_observations,
        "coverage_complete": True,
        "observed_identity_count": 5,
        "sampled_memory_bytes": 576 * 1024**2,
        "peak_bytes": 576 * 1024**2,
        "safety_threshold_bytes": 9 * 1024**3,
        "threshold_source": "pod_resource_limits",
        "within_safety_threshold": True,
        "observed_at": timestamp,
    }
    preflight["worker_processes"] = worker_processes
    preflight["participating_processes"] = participating
    preflight["memory_observation"] = memory_observation
    preflight["kubernetes"] = {
        "namespace": "acceptance",
        "context": "test-context",
        "metrics_api": "metrics.k8s.io/v1beta1",
        "namespace_bound": True,
        "memory_limit_bytes": None,
    }
    candidate["memory_observation"] = memory_observation
    evidence = report["evidence"]
    assert isinstance(evidence, dict)
    parameters = candidate["parameters"]
    assert isinstance(parameters, dict)
    evidence["runtime_fingerprint"] = _runtime_fingerprint(
        mode="real_postgresql_redis_multiprocess",
        workers=cast(list[object], worker_processes),
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
        parameters=parameters,
    )
    return report


REAL_RUNTIME_LOAD_STAGE_NAMES = (
    "acceptance.persisted",
    "turn.processing_observed",
    "worker.kill_requested",
    "worker.kill_completed",
    "worker.survivors_observed",
    "lease.takeover_observed",
    "stale_token_rejection_verified",
    "turn.commit_verified",
)
REAL_RUNTIME_FAULT_STAGE_NAMES = (
    "toxiproxy.proxies_verified",
    "proxy.disable_requested",
    "proxy.disabled",
    "acceptance.persisted",
    "work_pending_while_disabled",
    "proxy.restore_requested",
    "proxy.restored",
    "post_restore.commit_verified",
    "duplicate_turn_verified",
    "duplicate_publish_verified",
    "dlq.dead_letter_verified",
)


def _runtime_stage_markers() -> list[dict[str, object]]:
    observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return [
        {"name": name, "status": "pass", "observed_at": observed_at}
        for name in (*REAL_RUNTIME_LOAD_STAGE_NAMES, *REAL_RUNTIME_FAULT_STAGE_NAMES)
    ]


def _runtime_batch(
    *, count: int, duplicates: int = 0, prefix: str = "runtime"
) -> dict[str, object]:
    inbound_ids = [f"{prefix}-inbound-{index}" for index in range(count)]
    return {
        "accepted_calls": count + duplicates,
        "unique_inbound_ids": inbound_ids,
        "duplicate_calls": duplicates,
        "session_id": f"{prefix}-session",
        "tenant_id": "runtime-tenant",
        "message_order": list(range(count)),
    }


def _runtime_completion(*, count: int) -> dict[str, object]:
    mailbox = {
        "status": "pass",
        "schema_version": 2,
        "mailbox_row_present": True,
        "status_value": "IDLE",
        "accepted_sequence": count,
        "resolved_sequence": count,
        "processing_sequence": None,
        "queue_generation": 1,
        "lease_epoch": count,
        "item_count": count,
        "resolved_item_count": count,
        "unresolved_item_count": 0,
        "published_ready_outbox": 1,
        "completion_verified": True,
    }
    return {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": count},
            "turn_count": count,
            "lease_epoch": count,
            "lease_owner_present": False,
            "event_count": count,
            "event_sequences_contiguous": True,
            "scheduler_version": "v2",
            "published_inbound_outbox": 1,
            "published_scheduler_outbox": 1,
            "mailbox_v2": mailbox,
            "mailbox_v2_completion": mailbox,
        },
    }


def _runtime_phase_markers(*names: str) -> list[dict[str, object]]:
    observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return [{"name": name, "status": "pass", "observed_at": observed_at} for name in names]


def _real_runtime_candidate(*, run_id: str = "test-release-gate-run") -> dict[str, object]:
    source = _current_candidate_source_fingerprint()
    source_value = source["value"]
    workers = [
        {
            "container_id": f"runtime-worker-{index}",
            "pid": 2000 + index,
            "image_id": "sha256:" + "a" * 64,
            "source_fingerprint": source_value,
            "status": "running",
            "health": "healthy",
            "connection_env": {
                "valid": True,
                "role": "worker",
                "database": {"host": "toxiproxy", "port": 15432},
                "worker_database": {
                    "role": "trpc_worker",
                    "host": "toxiproxy",
                    "port": 15432,
                },
                "redis": {"host": "toxiproxy", "port": 16379},
            },
        }
        for index in range(4)
    ]
    load_batch = _runtime_batch(count=200, duplicates=20)
    load_completion = _runtime_completion(count=200)
    worker_after = workers[1:]
    kill_timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    load = {
        "status": "pass",
        "batch": load_batch,
        "completion": load_completion,
        "worker_kill": {
            "status": "pass",
            "killed_container_id": workers[0]["container_id"],
            "active_worker_id": "worker-runtime-worker-0",
            "active_turns_observed_before_kill": 1,
            "kill_requested_at": kill_timestamp,
            "kill_completed_at": kill_timestamp,
            "killed_container_pid": workers[0]["pid"],
            "killed_container_image_id": workers[0]["image_id"],
            "killed_container_source_fingerprint": workers[0]["source_fingerprint"],
            "old_owner": "worker-runtime-worker-0",
            "old_lease_epoch": 1,
            "old_fencing_token": 1,
            "old_token_rejected": True,
            "new_owner": "worker-runtime-worker-1",
            "new_lease_epoch": 2,
        },
        "fencing_takeover": {
            "status": "pass",
            "attempts_after_takeover": 2,
            "surviving_worker_containers": 3,
            "surviving_healthy_worker_containers": [
                worker["container_id"] for worker in worker_after
            ],
            "killed_container_excluded": True,
            "takeover_observed": True,
            "takeover_owner": "worker-runtime-worker-1",
            "killed_owner": "worker-runtime-worker-0",
            "takeover_owner_differs": True,
            "lease_epoch_before": 1,
            "lease_epoch_after": 2,
            "lease_epoch_monotonic": True,
            "takeover_owner_mapped_to_healthy_survivor": True,
            "all_survivors_running_healthy": True,
            "old_token_rejection": {
                "status": "pass",
                "fencing_conflict_caught": True,
                "owner_still_current": True,
                "old_owner": "worker-runtime-worker-0",
                "new_owner": "worker-runtime-worker-1",
                "old_lease_epoch": 1,
                "new_lease_epoch": 2,
                "old_token_rejected": True,
                "old_fencing_token": 1,
            },
            "old_token_rejected": True,
            "old_fencing_token": 1,
            "new_owner": "worker-runtime-worker-1",
            "new_lease_epoch": 2,
            "final_commit_contiguous": True,
        },
        "worker_containers_after": worker_after,
        "stage_markers": _runtime_phase_markers(*REAL_RUNTIME_LOAD_STAGE_NAMES),
    }
    fault_completion = _runtime_completion(count=8)

    def dependency_fault(component: str) -> dict[str, object]:
        expected = {
            "redis": ("0.0.0.0:16379", "redis:6379"),
            "postgres": ("0.0.0.0:15432", "postgres:5432"),
        }[component]
        return {
            "status": "pass",
            "batch": _runtime_batch(count=8, prefix=f"runtime-{component}"),
            "disable": {
                "status": "pass",
                "api_endpoint": "http://127.0.0.1:8474",
                "name": component,
                "enabled": False,
                "listen": expected[0],
                "upstream": expected[1],
            },
            "enable": {
                "status": "pass",
                "api_endpoint": "http://127.0.0.1:8474",
                "name": component,
                "enabled": True,
                "listen": expected[0],
                "upstream": expected[1],
            },
            "state_while_down": {"inbound_statuses": {"pending": 8}},
            "uncommitted_while_proxy_down": 8,
            "completion_after_restore": fault_completion,
            "expected_turn_count": 8,
            "observed_turn_count": 8,
            "duplicate_turns_verified": True,
            "stage_markers": [],
        }

    faults = {
        "status": "pass",
        "toxiproxy": {
            "status": "pass",
            "api_endpoint": "http://127.0.0.1:8474",
            "proxies": ["postgres", "redis"],
            "proxy_details": {
                "postgres": {
                    "enabled": True,
                    "listen": "0.0.0.0:15432",
                    "upstream": "postgres:5432",
                },
                "redis": {
                    "enabled": True,
                    "listen": "0.0.0.0:16379",
                    "upstream": "redis:6379",
                },
            },
            "proxy_endpoints": {
                "postgres": {
                    "api_endpoint": "http://127.0.0.1:8474",
                    "enabled": True,
                    "listen": "0.0.0.0:15432",
                    "upstream": "postgres:5432",
                },
                "redis": {
                    "api_endpoint": "http://127.0.0.1:8474",
                    "enabled": True,
                    "listen": "0.0.0.0:16379",
                    "upstream": "redis:6379",
                },
            },
        },
        "redis": dependency_fault("redis"),
        "postgres": dependency_fault("postgres"),
        "dlq_seed": {
            "attempts_before": 4,
            "retry_limit": 5,
            "terminal_path": "exhausted_retry_terminal_path",
        },
        "dlq": {
            "status": "pass",
            "dead_letter": {"status": "open", "source_id": "runtime-dlq-outbound"},
            "attempts_before": 4,
            "attempts_after": 5,
            "retry_attempts_increased": True,
            "terminal_status_open": True,
            "terminal_path": "exhausted_retry_terminal_path",
        },
        "republish_duplicate_publish_probe": {
            "status": "pass",
            "stream": "trpc:session-ready:v2",
            "group": "trpc-session-ready-v2",
            "outbox_id": "runtime-redis-outbox",
            "inbound_id": "runtime-redis-inbound-0",
            "session_id": "runtime-redis-session",
            "duplicate_stream_id": "2-0",
            "pending_duplicate": False,
            "turn_count": 1,
            "turn_count_exactly_one": True,
        },
        "stage_markers": _runtime_phase_markers(*REAL_RUNTIME_FAULT_STAGE_NAMES),
    }
    return {
        "mode": "real_compose_postgresql_redis",
        "run_id": run_id,
        "parameters": {
            "phase": "all",
            "workers": 4,
            "messages": 200,
            "duplicates": 20,
            "fault_messages": 8,
            "use_toxiproxy": True,
            "kill_worker": True,
            "compose_up": True,
            "compose_start_mode": "wrapper-prestarted-owned",
            "republish_probe": True,
            "timeout_seconds": 300.0,
            "redis_stream": "trpc:session-ready:v2",
            "redis_group": "trpc-session-ready-v2",
        },
        "preflight": {
            "status": "pass",
            "worker_containers": workers,
            "image_attestation": {
                "status": "pass",
                "worker_count": 4,
                "image_id": "sha256:" + "a" * 64,
                "source_fingerprint": source_value,
            },
            "participating_services": {},
        },
        "database_role_evidence": {
            "schema_version": 1,
            "status": "pass",
            "required_functions": list(real_runtime_gate.GLOBAL_WORKER_FUNCTION_SIGNATURES),
            "runtime_allowed_functions": list(
                real_runtime_gate.RUNTIME_ROUTING_FUNCTION_SIGNATURES
            ),
            "runtime": {
                "expected_role": "trpc_runtime",
                "role_snapshot": {
                    "current_user": "trpc_runtime",
                    "session_user": "trpc_runtime",
                    "role_name": "trpc_runtime",
                    "role_superuser": False,
                    "role_bypassrls": False,
                    "functions": {
                        signature: {
                            "exists": True,
                            "execute": signature
                            in real_runtime_gate.RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                        }
                        for signature in (
                            *real_runtime_gate.RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                            *real_runtime_gate.GLOBAL_WORKER_FUNCTION_SIGNATURES,
                        )
                    },
                },
                "global_function_probe": {
                    "function": "public.list_channel_bindings(text)",
                    "expected_access": "denied",
                    "observed_access": "denied",
                    "denied": True,
                },
            },
            "global_worker": {
                "expected_role": "trpc_worker",
                "role_snapshot": {
                    "current_user": "trpc_worker",
                    "session_user": "trpc_worker",
                    "role_name": "trpc_worker",
                    "role_superuser": False,
                    "role_bypassrls": True,
                    "functions": {
                        signature: {
                            "exists": True,
                            "execute": signature
                            in (
                                *real_runtime_gate.RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                                *real_runtime_gate.GLOBAL_WORKER_FUNCTION_SIGNATURES,
                            ),
                        }
                        for signature in (
                            *real_runtime_gate.RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                            *real_runtime_gate.GLOBAL_WORKER_FUNCTION_SIGNATURES,
                        )
                    },
                },
                "global_function_probe": {
                    "function": "public.list_channel_bindings(text)",
                    "expected_access": "allowed",
                    "observed_access": "allowed",
                    "denied": False,
                },
            },
        },
        "load": load,
        "faults": faults,
        "stage_markers": _runtime_stage_markers(),
    }


def _valid_real_runtime_report() -> dict[str, object]:
    evidence = _current_evidence("scripts.real_runtime_gate")
    candidate = _real_runtime_candidate(run_id=str(evidence["run_id"]))
    run_nonce = "real-runtime-nonce-1234567890"
    started_at = evidence["generated_at"]
    ended_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    evidence["run_nonce"] = run_nonce
    evidence["report_schema_version"] = 1
    candidate.update(
        {
            "run_id": evidence["run_id"],
            "run_nonce": run_nonce,
            "generated_at": evidence["generated_at"],
            "started_at": started_at,
            "ended_at": ended_at,
        }
    )

    def bind_markers(value: object) -> None:
        if isinstance(value, dict):
            markers = value.get("stage_markers")
            if isinstance(markers, list):
                for marker in markers:
                    if isinstance(marker, dict):
                        marker["run_id"] = evidence["run_id"]
                        marker["run_nonce"] = run_nonce
            for child in value.values():
                bind_markers(child)
        elif isinstance(value, list):
            for child in value:
                bind_markers(child)

    bind_markers(candidate)
    preflight = candidate["preflight"]  # type: ignore[index]
    workers = preflight["worker_containers"]  # type: ignore[index]
    source_value = _current_candidate_source_fingerprint()["value"]
    participating: dict[str, object] = {"worker": workers}
    for role in (
        "outbox-dispatcher",
        "channel-dispatcher",
        "post-turn-projector",
        "session-recovery",
    ):
        participating[role] = [
            {
                "container_id": f"runtime-{role}",
                "pid": 3000 + len(participating),
                "image_id": "sha256:" + "a" * 64,
                "source_fingerprint": source_value,
                "status": "running",
                "health": "healthy",
                "connection_env": {
                    "valid": True,
                    "role": role,
                    "database": {"host": "toxiproxy", "port": 15432},
                    "worker_database": {
                        "role": "trpc_worker",
                        "host": "toxiproxy",
                        "port": 15432,
                    },
                    "redis": {"host": "toxiproxy", "port": 16379},
                },
            }
        ]
    preflight["participating_services"] = participating  # type: ignore[index]
    identities = [
        {
            "container_id": worker["container_id"],
            "pid": worker["pid"],
            "image_id": worker["image_id"],
            "source_fingerprint": worker["source_fingerprint"],
        }
        for worker in workers  # type: ignore[union-attr]
    ]
    participating_identities = [
        {
            "role": role,
            "container_id": container["container_id"],
            "pid": container["pid"],
            "image_id": container["image_id"],
            "source_fingerprint": container["source_fingerprint"],
        }
        for role in (
            "worker",
            "outbox-dispatcher",
            "channel-dispatcher",
            "post-turn-projector",
            "session-recovery",
        )
        for container in participating[role]  # type: ignore[index]
    ]
    parameters = candidate["parameters"]  # type: ignore[index]
    evidence["runtime_fingerprint"] = _runtime_fingerprint(
        mode="real_compose_postgresql_redis",
        workers=identities,
        participating=participating_identities,
        stream=str(parameters["redis_stream"]),  # type: ignore[index]
        group=str(parameters["redis_group"]),  # type: ignore[index]
        parameters=parameters,  # type: ignore[arg-type]
    )
    return {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "run_id": evidence["run_id"],
        "run_nonce": run_nonce,
        "generated_at": evidence["generated_at"],
        "started_at": started_at,
        "ended_at": ended_at,
        "source_fingerprint": evidence["source_fingerprint"],
        "candidate": candidate,
        "evidence": evidence,
        "rejection_reasons": [],
        "production_rejection_reasons": [],
    }


@pytest.mark.parametrize(
    ("path", "observed"),
    (
        ("candidate.faults.toxiproxy.proxy_details.postgres.listen", "[::]:15432"),
        ("candidate.faults.toxiproxy.proxy_endpoints.postgres.listen", "[::]:15432"),
        ("candidate.faults.postgres.disable.listen", "[::]:15432"),
        ("candidate.faults.postgres.enable.listen", "[::]:15432"),
        ("candidate.faults.toxiproxy.proxy_details.redis.listen", "[::]:16379"),
        ("candidate.faults.toxiproxy.proxy_endpoints.redis.listen", "[::]:16379"),
        ("candidate.faults.redis.disable.listen", "[::]:16379"),
        ("candidate.faults.redis.enable.listen", "[::]:16379"),
    ),
)
def test_real_runtime_accepts_ipv6_wildcard_listen_in_all_attestation_paths(
    tmp_path, path: str, observed: str
) -> None:
    report_value = _valid_real_runtime_report()
    target: object = report_value
    components = path.split(".")
    for component in components[:-1]:
        target = target[component]  # type: ignore[index]
    target[components[-1]] = observed  # type: ignore[index]
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


@pytest.mark.parametrize(
    ("path", "observed"),
    (
        ("candidate.faults.toxiproxy.proxy_details.postgres.listen", "[::]:16379"),
        ("candidate.faults.toxiproxy.proxy_endpoints.redis.listen", "127.0.0.1:16379"),
        ("candidate.faults.redis.disable.listen", "[::1]:16379"),
        ("candidate.faults.postgres.enable.listen", "postgres:15432"),
    ),
)
def test_real_runtime_rejects_non_equivalent_listen_attestation(
    tmp_path, path: str, observed: str
) -> None:
    report_value = _valid_real_runtime_report()
    target: object = report_value
    components = path.split(".")
    for component in components[:-1]:
        target = target[component]  # type: ignore[index]
    target[components[-1]] = observed  # type: ignore[index]
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "fail"
    assert reasons
    assert any(marker in reasons[0].lower() for marker in ("toxiproxy", "readback"))


def _valid_kubernetes_report() -> dict[str, object]:
    evidence = _current_evidence("scripts.kubernetes_runtime_gate")
    run_id = str(evidence["run_id"])
    namespace = "trpc-runtime-gate-abcdef1234"
    nonce = "a" * 32
    cluster_fingerprint = "c" * 64
    driver_sha256 = hashlib.sha256(
        (release_gate.ROOT / release_gate.K8S_HPA_DRIVER_RELATIVE_PATH).read_bytes()
    ).hexdigest()
    initial_digest = "sha256:" + "a" * 64
    upgrade_digest = "sha256:" + "b" * 64
    initial = {name: [initial_digest] for name in release_gate.K8S_REQUIRED_DEPLOYMENTS}
    upgrade = {name: [upgrade_digest] for name in release_gate.K8S_REQUIRED_DEPLOYMENTS}
    hpa_observation = {
        "status": "pass",
        "observed_live": True,
        "source": "kubectl_api",
        "hpa_name": "trpc-worker",
        "metric_name": "trpc_session_ready_backlog",
        "run_nonce": nonce,
        "namespace": namespace,
        "cluster_identity": {"fingerprint_sha256": cluster_fingerprint},
        "trigger": {"kind": "controlled_backlog", "source": "bounded-driver"},
        "driver_evidence": {
            "load": {
                "api_observed": True,
                "job_name": f"trpc-hpa-load-{nonce[:20]}",
                "job_uid": "job-uid-1",
                "job_labels": {
                    "trpc.io/hpa-gate": "bounded-job-driver",
                    "trpc.io/hpa-run": nonce,
                    "trpc.io/hpa-phase": "load",
                    "trpc.io/hpa-cluster": cluster_fingerprint[:63],
                },
                "namespace": namespace,
                "run_nonce": nonce,
                "cluster_fingerprint": cluster_fingerprint,
                "phase": "load",
            },
            "clear": {
                "api_observed": True,
                "job_name": f"trpc-hpa-load-{nonce[:20]}",
                "job_uid": "job-uid-1",
                "job_labels": {
                    "trpc.io/hpa-gate": "bounded-job-driver",
                    "trpc.io/hpa-run": nonce,
                    "trpc.io/hpa-phase": "load",
                    "trpc.io/hpa-cluster": cluster_fingerprint[:63],
                },
                "namespace": namespace,
                "run_nonce": nonce,
                "cluster_fingerprint": cluster_fingerprint,
                "phase": "load",
                "job_deleted": True,
            },
        },
        "scale_up_timeout_seconds": 60,
        "scale_down_timeout_seconds": 60,
        "before": {
            "metric_value": 0,
            "desired_replicas": 2,
            "current_replicas": 2,
            "ready_replicas": 2,
        },
        "during": {
            "metric_value": 100,
            "desired_replicas": 4,
            "current_replicas": 4,
            "ready_replicas": 4,
        },
        "after": {
            "metric_value": 0,
            "desired_replicas": 2,
            "current_replicas": 2,
            "ready_replicas": 2,
        },
    }
    external_path = (
        f"/apis/external.metrics.k8s.io/v1beta1/namespaces/{namespace}/trpc_session_ready_backlog"
    )
    for phase in ("before", "during", "after"):
        observation = hpa_observation[phase]
        metric_value = observation["metric_value"]
        observation["external_metric"] = {
            "api_observed": True,
            "api_version": "v1beta1",
            "api_path": external_path,
            "metric_name": "trpc_session_ready_backlog",
            "namespace": namespace,
            "label_namespace": namespace,
            "item_count": 1,
            "value": metric_value,
        }
    post_ready = {name: {"status": "pass"} for name in release_gate.K8S_REQUIRED_DEPLOYMENTS}
    node_eviction = {
        "status": "pass",
        "preflight": {
            "node_label_verified": True,
            "node_ready": True,
            "node_schedulable": True,
        },
        "drain": {
            "cordon": {"status": "pass"},
            "post_cordon_preflight": {
                "node_label_verified": True,
                "node_ready": True,
                "gate_namespace_pod_count": 8,
            },
            "drain": {"status": "pass"},
            "post_drain": {"node_cordoned": True},
            "uncordon": {"status": "pass"},
        },
        "uncordon_observed": True,
        "post_drain_readiness": post_ready,
    }
    checks = {name: {"status": "pass"} for name in release_gate.K8S_REQUIRED_CHECKS}
    checks["hpa_load_observation"] = {
        "status": "pass",
        "observed_live": True,
        "observation": hpa_observation,
    }
    checks["hpa_driver_trust"] = {
        "status": "pass",
        "driver_sha256": driver_sha256,
        "kubeconfig_sha256": "2" * 64,
        "subject_sha256": "3" * 64,
        "driver_context_sha256": "4" * 64,
        "cluster_fingerprint_sha256": cluster_fingerprint,
        "identity_verified": True,
        "rule_audit": {
            "complete": True,
            "scope": "target_namespace_jobs_pods_only",
            "target_namespace": namespace,
            "target_rules_sha256": "5" * 64,
            "default_rules_sha256": "6" * 64,
            "kube_system_rules_sha256": "7" * 64,
            "cluster_rules_sha256": "8" * 64,
        },
        "dedicated_kubeconfig": True,
        "scope": "namespace_jobs_only",
        "rbac_verified": True,
        "reasons": [],
    }
    checks["node_eviction"] = node_eviction
    checks["rolling_upgrade"] = {
        "status": "pass",
        "upgrade_image_supplied": True,
        "image_ids": {
            "initial": initial,
            "upgrade": upgrade,
            "changed": {name: True for name in initial},
        },
        "rollback": {
            "status": "pass",
            "deployment": "trpc-worker",
            "failure_injected": True,
            "failure_observed": True,
            "undo_observed": True,
            "readiness_recovered": True,
            "restored_image_ids": upgrade["trpc-worker"],
        },
    }
    checks["initial_image_ids"] = initial
    candidate = {
        "mode": "live_kubernetes_control_plane",
        "enabled": True,
        "topology": {
            "scope": "unified_cluster_runtime",
            "im_deployment": "production_cluster",
            "production_deployments": list(release_gate.K8S_PRODUCTION_DEPLOYMENTS),
            "tested_deployments": list(release_gate.K8S_REQUIRED_DEPLOYMENTS),
            "provider_disabled_deployments": ["trpc-wecom-connector"],
        },
        "namespace": namespace,
        "run_nonce": nonce,
        "controlled_node": {"fingerprint_sha256": "d" * 64},
        "checks": checks,
        "runtime_attestation": {
            "status": "pass",
            "namespace_isolated": True,
            "namespace": namespace,
            "run_nonce": nonce,
            "cluster_identity": {
                "context_sha256": "e" * 64,
                "fingerprint_sha256": cluster_fingerprint,
                "server_observed": True,
            },
            "node_identity": {"fingerprint_sha256": "d" * 64},
            "actions": {name: True for name in release_gate.K8S_REQUIRED_ACTIONS},
            "image_ids": {"initial": initial, "upgrade": upgrade},
            "eviction_scope": "namespace_pod_eviction+controlled_node",
            "node_eviction_status": "pass",
        },
        "lineage": {
            "status": "pass",
            "checkout_current": True,
            "producer": "scripts.kubernetes_runtime_gate",
            "image_digest": initial_digest,
        },
    }
    evidence["runtime_fingerprint"] = _runtime_fingerprint(
        mode="kubernetes_runtime",
        workers=[initial_digest],
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
        parameters={
            "required_checks": len(release_gate.K8S_REQUIRED_CHECKS),
            "image_digest": initial_digest,
        },
    )
    return {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "run_id": run_id,
        "baseline": {
            "required_checks": list(release_gate.K8S_REQUIRED_CHECKS),
            "required_runtime_actions": list(release_gate.K8S_REQUIRED_ACTIONS),
            "hpa_load_policy": {
                "metric": "trpc_session_ready_backlog",
                "required_phases": ["before", "during", "after"],
            },
        },
        "candidate": candidate,
        "case_deltas": {"failed_checks": 0, "not_run_checks": 0},
        "evidence": evidence,
        "rejection_reasons": [],
        "production_rejection_reasons": [],
    }


def _valid_disaster_recovery_report(directory) -> dict[str, object]:
    source = _current_candidate_source_fingerprint()
    release_binding = _current_evidence()["release_binding"]
    repository = "registry.example/acme/trpc-agent-service"
    initial_digest = "sha256:" + "a" * 64
    upgrade_digest = "sha256:" + "b" * 64
    binding = {
        "schema_version": 1,
        "kind": "registry_candidate_binding",
        "release_binding": release_binding,
        "source_fingerprint": source,
        "repository": repository,
        "image_digest": initial_digest,
        "images": {
            "initial": {
                "digest": initial_digest,
                "reference": f"{repository}@{initial_digest}",
            },
            "upgrade": {
                "digest": upgrade_digest,
                "reference": f"{repository}@{upgrade_digest}",
            },
        },
    }
    lock = {
        "schema_version": 1,
        "kind": "release_candidate_lock",
        "release_binding": release_binding,
        "source_fingerprint": source,
        "binding_sha256": release_gate.canonical_sha256(binding),
        "repository": repository,
        "image_digest": initial_digest,
        "images": binding["images"],
    }
    binding_path = directory / "registry-image-binding.json"
    lock_path = directory / "candidate-lock.json"
    atomic_write_json(binding_path, binding)
    atomic_write_json(lock_path, lock)
    components = {
        name: {
            "status": "pass",
            "run_id": f"{name}-run",
            "rpo_seconds": 30,
            "rto_seconds": 120,
        }
        for name in ("postgres_pitr", "artifact_restore", "key_restore")
    }
    return {
        "schema_version": 1,
        "gate": "pass",
        "baseline": {
            "required_components": ["postgres_pitr", "artifact_restore", "key_restore"],
            "max_rpo_seconds": 300,
            "max_rto_seconds": 3_600,
        },
        "candidate": {
            "mode": "isolated_restore_drill",
            "lineage": {"image_digest": initial_digest},
            "candidate_lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
            "components": components,
        },
        "case_deltas": {"failed_components": []},
        "production_gate": "pass",
        "evidence": _current_evidence("scripts.disaster_recovery_gate"),
    }


def _valid_functional_disaster_recovery_report(directory) -> dict[str, object]:
    destructive = _valid_disaster_recovery_report(directory)
    candidate = cast(dict[str, object], destructive["candidate"])
    lineage = cast(dict[str, object], candidate["lineage"])
    image_digest = cast(str, lineage["image_digest"])
    lock_path = directory / "candidate-lock.json"
    components = {
        name: {
            "status": "pass",
            "run_id": f"functional-{name}-run",
            "rpo_seconds": 0,
            "rto_seconds": 1,
            "backend": "postgresql" if name == "postgres_pitr" else "minio",
            "restore_mode": {
                "postgres_pitr": "logical_snapshot",
                "artifact_restore": "object_version",
                "key_restore": "synthetic_key_version",
            }[name],
        }
        for name in ("postgres_pitr", "artifact_restore", "key_restore")
    }
    required = ("postgres_pitr", "artifact_restore", "key_restore")
    runtime_jobs = [
        {
            "component": name,
            "image_digest": image_digest,
            "job_uid_sha256": str(index) * 64,
            "pod_uid_sha256": "4" * 64,
        }
        for index, name in enumerate(required, start=7)
    ]
    runtime_parameters = {
        "candidate_lock_binding_sha256": json.loads(lock_path.read_text(encoding="utf-8"))[
            "binding_sha256"
        ],
        "components": list(required),
        "image_digest": image_digest,
    }
    evidence = _current_evidence("scripts.functional_disaster_recovery_gate")
    evidence["runtime_fingerprint"] = runtime_fingerprint(
        mode="same_cluster_zero_cost_functional",
        worker_identities=runtime_jobs,
        stream="3" * 64,
        group="2" * 64,
        parameters=runtime_parameters,
    )
    return {
        "schema_version": 1,
        "baseline": {
            "required_components": ["postgres_pitr", "artifact_restore", "key_restore"],
            "max_rto_seconds": 300,
            "production_requirements_excluded": [
                "remote redundancy",
                "WAL PITR",
                "external KMS",
            ],
        },
        "candidate": {
            "mode": "same_cluster_zero_cost_functional",
            "platform": "kubernetes",
            "lineage": {"image_digest": image_digest},
            "candidate_lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
            "components": components,
            "runtime_attestation": {
                "cluster_uid_sha256": "3" * 64,
                "namespace_uid_sha256": "2" * 64,
                "jobs": runtime_jobs,
            },
            "orchestration": {
                "failure_stage": None,
                "failure_code": None,
                "namespace_sha256": "1" * 64,
                "namespace_uid_sha256": "2" * 64,
                "namespace_created": True,
                "jobs_submitted_together": True,
                "cleanup_completed": True,
            },
        },
        "case_deltas": {"failed_components": []},
        "evidence": evidence,
        "gate": "pass",
        "production_gate": "not_run",
        "rejection_reasons": [],
        "production_rejection_reasons": ["functional evidence is not disaster-redundant"],
    }


def _valid_migration_report() -> dict[str, object]:
    run_id = "test-release-gate-run"
    tenant_id = "tenant-prod-42"
    migration_id = "migration-prod-42"
    checksum = "a" * 64
    source_snapshot_id = "redis-snapshot-prod-42"
    observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    phase_controls = {
        "prepare": (False, "source", False, False, "ready"),
        "backfill": (False, "source", False, False, "ready"),
        "shadow-read": (False, "source", False, False, "ready"),
        "dual-write": (True, "source", False, False, "dual-write"),
        "cutover": (True, "target", False, False, "target"),
        "verify": (True, "target", False, False, "target"),
        "cleanup": (False, "target", True, False, "target"),
        "rollback": (False, "source", False, True, "source"),
    }
    phases: dict[str, object] = {}
    for phase, state in phase_controls.items():
        dual_write, active_profile, cleaned, rolled_back, mailbox_v2 = state
        case_deltas: dict[str, object] = {
            "phase": phase,
            "source_count": 2,
            "target_count": 2,
            "checksum": checksum,
            "target_checksum": checksum,
            "differences": [],
        }
        phases[phase] = {
            "tenant_id": tenant_id,
            "migration_id": migration_id,
            "run_id": run_id,
            "phase": phase,
            "source_snapshot_id": source_snapshot_id,
            "source_count": 2,
            "source_checksum": checksum,
            "started_at": observed_at,
            "completed_at": observed_at,
            "gate": "pass",
            "case_deltas": case_deltas,
            "control_state": {
                "status": "pass",
                "dual_write": dual_write,
                "active_profile": active_profile,
                "cleaned": cleaned,
                "rolled_back": rolled_back,
                "mailbox_v2": mailbox_v2,
                **(
                    {"atomic_cutover": True, "rollback_verified": True}
                    if phase == "cleanup"
                    else {"rollback_verified": True}
                    if phase == "rollback"
                    else {}
                ),
            },
        }
    evidence = _current_evidence("scripts.migrate_data")
    evidence["runtime_fingerprint"] = {
        "algorithm": "sha256",
        "status": "available",
        "value": "1" * 64,
        "mode": "real_migration",
        "worker_count": 1,
        "worker_identity_summary_sha256": "2" * 64,
        "stream_group_sha256": "3" * 64,
        "parameters_sha256": release_gate.canonical_sha256(
            {
                "phase_count": len(release_gate.MIGRATION_REQUIRED_PHASES),
                "kinds": ["session", "memory"],
            }
        ),
    }
    return {
        "schema_version": 1,
        "gate": "pass",
        "production_gate": "pass",
        "run_id": run_id,
        "candidate": {
            "mode": "real_redis_to_postgresql",
            "scope": "production",
            "tenant_id": tenant_id,
            "migration_id": migration_id,
            "is_simulation": False,
            "run_id": run_id,
            "source": {
                "kind": "redis",
                "is_real": True,
                "backend_id": "redis-source",
                "endpoint_identity": "d" * 64,
            },
            "target": {
                "kind": "postgresql",
                "is_real": True,
                "backend_id": "postgresql-target",
                "endpoint_identity": "e" * 64,
            },
            "phase_order": list(release_gate.MIGRATION_REQUIRED_PHASES),
            "phases": {
                phase: {"status": "pass", "completed": True, "order": index}
                for index, phase in enumerate(release_gate.MIGRATION_REQUIRED_PHASES, start=1)
            },
            "verification": {
                "status": "pass",
                "source_count": 2,
                "target_count": 2,
                "source_checksum": checksum,
                "target_checksum": checksum,
                "differences": [],
            },
            "control": {
                "status": "pass",
                "tenant_id": tenant_id,
                "migration_id": migration_id,
                "tenant_scoped": True,
                "atomic_cutover": True,
                "dual_write_verified": True,
                "cleanup_after_verify": True,
                "rollback_verified": True,
            },
            "operator_attestation": {
                "status": "pass",
                "scope": "production",
                "operator_id": "b" * 64,
                "attested_at": observed_at,
                "source_target_reviewed": True,
                "checksums_reviewed": True,
                "control_reviewed": True,
            },
            # The release manifest extracts the immutable image binding from
            # the common candidate lineage location used by every production
            # report.  The migration-specific semantic validator still uses
            # migration_evidence.lineage below; keep both views identical.
            "lineage": {
                "status": "pass",
                "checkout_current": True,
                "producer": "scripts.migrate_data",
                "run_id": run_id,
                "source_fingerprint": evidence["source_fingerprint"]["value"],  # type: ignore[index]
                "runtime_fingerprint": evidence["runtime_fingerprint"]["value"],  # type: ignore[index]
                "image_digest": "sha256:" + "a" * 64,
            },
        },
        "migration_evidence": {
            "status": "pass",
            "scope": "production",
            "is_simulation": False,
            "run_id": run_id,
            "run_started_at": observed_at,
            "run_finished_at": observed_at,
            "source": {
                "kind": "redis",
                "is_real": True,
                "endpoint_sha256": "d" * 64,
                "snapshot_id": source_snapshot_id,
                "source_count": 2,
                "source_checksum": checksum,
            },
            "target": {
                "kind": "postgresql",
                "is_real": True,
                "endpoint_sha256": "e" * 64,
                "target_count": 2,
                "target_checksum": checksum,
            },
            "manifest": {
                "tenant_id": tenant_id,
                "migration_id": migration_id,
                "source_kind": "redis",
                "kinds": ["session", "memory"],
                "source_snapshot_id": source_snapshot_id,
                "source_count": 2,
                "source_checksum": checksum,
                "app_id": "app-prod-42",
                "app_revision": 1,
                "config_version": 1,
                "binding_id": "binding-prod-42",
                "binding_revision": 1,
            },
            "phases": phases,
            "target_empty_preflight": {
                "tenant_id": tenant_id,
                "checked_tables": list(release_gate.MIGRATION_TARGET_EMPTY_TABLES),
                "table_counts": {table: 0 for table in release_gate.MIGRATION_TARGET_EMPTY_TABLES},
                "non_empty_tables": [],
                "empty": True,
            },
            "control": {
                "complete": True,
                "rollback_supported": True,
                "rollback_observed": True,
                "phase_count": 8,
                "factory": "production_migration_control.create",
            },
            "operator_confirmation": {
                "status": "confirmed",
                "method": "cli_flag_and_environment_acknowledgement",
                "operator_id_sha256": "b" * 64,
                "change_ticket_sha256": "c" * 64,
                "confirmed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            },
            "lineage": {
                "status": "pass",
                "checkout_current": True,
                "producer": "scripts.migrate_data",
                "run_id": run_id,
                "source_fingerprint": (
                    evidence["source_fingerprint"]["value"]  # type: ignore[index]
                ),
                "runtime_fingerprint": (
                    evidence["runtime_fingerprint"]["value"]  # type: ignore[index]
                ),
                "image_digest": "sha256:" + "a" * 64,
            },
        },
        "evidence": {**evidence, "run_id": run_id},
        "rejection_reasons": [],
        "production_rejection_reasons": [],
    }


def test_real_runtime_pass_accepts_complete_formal_report(tmp_path) -> None:
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(_valid_real_runtime_report()), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


@pytest.mark.parametrize(
    "service_name",
    (
        "worker",
        "outbox-dispatcher",
        "channel-dispatcher",
        "post-turn-projector",
        "session-recovery",
    ),
)
def test_real_runtime_requires_trpc_worker_toxiproxy_route_for_each_participant(
    tmp_path, service_name: str
) -> None:
    report_value = _valid_real_runtime_report()
    participants = report_value["candidate"]["preflight"]["participating_services"]  # type: ignore[index]
    participants[service_name][0]["connection_env"]["worker_database"] = {  # type: ignore[index]
        "role": "trpc_runtime",
        "host": "toxiproxy",
        "port": 15432,
    }
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("trpc_worker" in reason for reason in reasons)


def test_real_runtime_rejects_missing_worker_database_attestation(tmp_path) -> None:
    report_value = _valid_real_runtime_report()
    worker = report_value["candidate"]["preflight"]["worker_containers"][0]  # type: ignore[index]
    worker["connection_env"].pop("worker_database")  # type: ignore[index]
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("trpc_worker" in reason for reason in reasons)


@pytest.mark.parametrize("mutation", ("missing_pid", "duplicate_pid", "duplicate_container"))
def test_real_runtime_rejects_global_participating_process_identity_mutations(
    tmp_path, mutation: str
) -> None:
    report_value = _valid_real_runtime_report()
    participating = report_value["candidate"]["preflight"]["participating_services"]  # type: ignore[index]
    recovery = participating["session-recovery"][0]
    outbox = participating["outbox-dispatcher"][0]
    if mutation == "missing_pid":
        recovery.pop("pid")
    elif mutation == "duplicate_pid":
        recovery["pid"] = outbox["pid"]
    else:
        recovery["container_id"] = outbox["container_id"]
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status in {"not_run", "fail"}
    assert reasons
    assert any(
        marker in reasons[0].lower() for marker in ("identity", "duplicated", "pid", "container")
    )


def test_real_runtime_rejects_recovery_identity_not_bound_by_fingerprint(tmp_path) -> None:
    report_value = _valid_real_runtime_report()
    recovery = report_value["candidate"]["preflight"]["participating_services"][  # type: ignore[index]
        "session-recovery"
    ][0]
    recovery["pid"] += 100
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("runtime fingerprint" in reason for reason in reasons)


def test_real_runtime_rejects_missing_participating_identity_fingerprint(tmp_path) -> None:
    report_value = _valid_real_runtime_report()
    report_value["evidence"]["runtime_fingerprint"].pop(  # type: ignore[index]
        "participating_identity_summary_sha256"
    )
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("runtime fingerprint" in reason for reason in reasons)


def test_kubernetes_pass_requires_complete_live_attestation(tmp_path) -> None:
    report = tmp_path / REPORTS["deployment"][0]
    report.write_text(json.dumps(_valid_kubernetes_report()), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


@pytest.mark.parametrize(
    ("mutation", "reason_fragment"),
    (
        ("schema", "invalid schema_version"),
        ("topology_location", "reviewed unified-cluster runtime topology"),
        ("topology_disabled", "reviewed unified-cluster runtime topology"),
        ("nonce", "run_nonce"),
        ("hpa", "HPA desired replicas"),
        ("hpa_driver_trust", "hpa_driver_trust"),
        ("uncordon", "node_eviction.drain.uncordon"),
        ("runtime_fingerprint_value", "runtime_fingerprint"),
        ("runtime_fingerprint_workers", "runtime_fingerprint"),
        ("runtime_fingerprint_parameters", "runtime_fingerprint"),
        ("image_overlap", "did not change for trpc-worker"),
        ("image_multi", "image_ids.initial.trpc-worker"),
        ("checks_image_stale", "checks.initial_image_ids"),
        ("checks_image_extra", "deployment set"),
        ("checks_changed_false", "changed does not match"),
        ("checks_image_missing", "deployment set"),
    ),
)
def test_kubernetes_pass_rejects_incomplete_or_replayed_runtime_evidence(
    tmp_path, mutation: str, reason_fragment: str
) -> None:
    # Round-trip through JSON so the report's duplicated evidence maps do not
    # share Python object identity in this fixture.
    value = json.loads(json.dumps(_valid_kubernetes_report()))
    if mutation == "schema":
        value["schema_version"] = 0
    elif mutation == "topology_location":
        value["candidate"]["topology"]["im_deployment"] = "external_host"  # type: ignore[index]
    elif mutation == "topology_disabled":
        value["candidate"]["topology"]["provider_disabled_deployments"] = []  # type: ignore[index]
    elif mutation == "nonce":
        value["candidate"]["run_nonce"] = "b" * 32  # type: ignore[index]
    elif mutation == "hpa":
        value["candidate"]["checks"]["hpa_load_observation"]["observation"]["during"][
            "desired_replicas"
        ] = 2  # type: ignore[index]
    elif mutation == "hpa_driver_trust":
        value["candidate"]["checks"]["hpa_driver_trust"][  # type: ignore[index]
            "dedicated_kubeconfig"
        ] = False
    elif mutation == "runtime_fingerprint_value":
        value["evidence"]["runtime_fingerprint"]["value"] = "0" * 64  # type: ignore[index]
    elif mutation == "runtime_fingerprint_workers":
        value["evidence"]["runtime_fingerprint"]["worker_identity_summary_sha256"] = "0" * 64  # type: ignore[index]
    elif mutation == "runtime_fingerprint_parameters":
        value["evidence"]["runtime_fingerprint"]["parameters_sha256"] = "0" * 64  # type: ignore[index]
    elif mutation == "image_overlap":
        initial_id = value["candidate"]["runtime_attestation"]["image_ids"]["initial"][  # type: ignore[index]
            "trpc-worker"
        ][0]
        value["candidate"]["runtime_attestation"]["image_ids"]["upgrade"][  # type: ignore[index]
            "trpc-worker"
        ] = [initial_id]
    elif mutation == "image_multi":
        value["candidate"]["runtime_attestation"]["image_ids"]["initial"][  # type: ignore[index]
            "trpc-worker"
        ].append("sha256:" + "9" * 64)
    elif mutation == "checks_image_stale":
        value["candidate"]["checks"]["initial_image_ids"]["trpc-worker"] = [  # type: ignore[index]
            "sha256:" + "9" * 64
        ]
    elif mutation == "checks_image_extra":
        value["candidate"]["checks"]["rolling_upgrade"]["image_ids"]["upgrade"][  # type: ignore[index]
            "unexpected-deployment"
        ] = ["sha256:" + "9" * 64]
    elif mutation == "checks_changed_false":
        value["candidate"]["checks"]["rolling_upgrade"]["image_ids"]["changed"][  # type: ignore[index]
            "trpc-worker"
        ] = False
    elif mutation == "checks_image_missing":
        value["candidate"]["checks"]["rolling_upgrade"]["image_ids"]["initial"].pop(  # type: ignore[index]
            "trpc-worker"
        )
    else:
        value["candidate"]["checks"]["node_eviction"]["drain"]["uncordon"]["status"] = "fail"  # type: ignore[index]
    report = tmp_path / REPORTS["deployment"][0]
    report.write_text(json.dumps(value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status in {"not_run", "fail"}
    assert any(reason_fragment in reason for reason in reasons)


def test_migration_pass_accepts_only_complete_real_attested_report(tmp_path) -> None:
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(_valid_migration_report()), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


def test_migration_pass_requires_candidate_contract_after_real_evidence(tmp_path) -> None:
    value = _valid_migration_report()
    value["candidate"]["control"]["rollback_verified"] = False  # type: ignore[index]
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("candidate.control.rollback_verified" in reason for reason in reasons)


@pytest.mark.parametrize(
    ("mutation", "expected_status", "reason_fragment"),
    (
        ("schema", "fail", "schema_version"),
        ("phase_time", "fail", "time window"),
        ("target_table_scope", "fail", "target tenant"),
        ("rollback_observed", "not_run", "rollback_observed"),
        ("runtime_phase_count", "not_run", "runtime_fingerprint"),
    ),
)
def test_migration_rejects_unbound_production_evidence(
    tmp_path, mutation: str, expected_status: str, reason_fragment: str
) -> None:
    value = _valid_migration_report()
    if mutation == "schema":
        value["schema_version"] = 0
    elif mutation == "phase_time":
        value["migration_evidence"]["phases"]["verify"]["started_at"] = (  # type: ignore[index]
            "2020-01-01T00:00:00Z"
        )
    elif mutation == "target_table_scope":
        value["migration_evidence"]["target_empty_preflight"]["checked_tables"].pop()  # type: ignore[index]
    elif mutation == "rollback_observed":
        value["migration_evidence"]["control"]["rollback_observed"] = False  # type: ignore[index]
    else:
        value["evidence"]["runtime_fingerprint"]["parameters_sha256"] = "0" * 64  # type: ignore[index]
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == expected_status
    assert any(reason_fragment in reason for reason in reasons)


def test_migration_candidate_fallback_is_rejected(tmp_path) -> None:
    value = _valid_migration_report()
    value.pop("migration_evidence")
    value["candidate"] = {"mode": "real_redis_to_postgresql"}
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "candidate fallback is not accepted" in reasons[0]


@pytest.mark.parametrize("invalid_count", [True, -1, "0"])
def test_migration_target_empty_preflight_requires_ordered_nonnegative_counts(
    tmp_path, invalid_count: object
) -> None:
    value = _valid_migration_report()
    preflight = value["migration_evidence"]["target_empty_preflight"]  # type: ignore[index]
    preflight["table_counts"][  # type: ignore[index]
        release_gate.MIGRATION_TARGET_EMPTY_TABLES[0]
    ] = invalid_count
    if invalid_count is True:
        preflight["checked_tables"] = list(  # type: ignore[index]
            reversed(release_gate.MIGRATION_TARGET_EMPTY_TABLES)
        )
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(value), encoding="utf-8")
    status, reasons = _status(report, production_field=True)
    assert status in {"not_run", "fail"}
    assert any(
        "target empty preflight" in reason or "target tenant" in reason for reason in reasons
    )


def test_migration_rejects_empty_source_and_unobserved_rollback(tmp_path) -> None:
    value = _valid_migration_report()
    value["migration_evidence"]["source"]["source_count"] = 0  # type: ignore[index]
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status in {"not_run", "fail"}
    assert "source_count" in reasons[0]

    value = _valid_migration_report()
    value["migration_evidence"]["phases"].pop("rollback")  # type: ignore[index]
    report.write_text(json.dumps(value), encoding="utf-8")
    status, reasons = _status(report, production_field=True)
    assert status == "not_run"
    assert "missing rollback" in reasons[0]


def test_migration_rejects_old_operator_confirmation(tmp_path) -> None:
    value = _valid_migration_report()
    value["migration_evidence"]["operator_confirmation"]["confirmed_at"] = "2020-01-01T00:00:00Z"  # type: ignore[index]
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "confirmed_at" in reasons[0]


def test_migration_pass_rejects_simulation_tenant(tmp_path) -> None:
    report_value = _valid_migration_report()
    report_value["migration_evidence"]["manifest"]["tenant_id"] = (  # type: ignore[index]
        "migration-acceptance-20260823"
    )
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "production scope" in reasons[0]


def test_migration_pass_rejects_incomplete_phase_evidence(tmp_path) -> None:
    report_value = _valid_migration_report()
    del report_value["migration_evidence"]["phases"]["cleanup"]  # type: ignore[index]
    report = tmp_path / REPORTS["migration"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "phases missing cleanup" in reasons[0]


def test_real_runtime_canary_cannot_promote_even_when_both_gates_claim_pass(tmp_path) -> None:
    report_value = _valid_real_runtime_report()
    parameters = report_value["candidate"]["parameters"]  # type: ignore[index]
    parameters.update({"messages": 20, "duplicates": 2, "fault_messages": 4})
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("messages" in reason for reason in reasons)


@pytest.mark.parametrize(
    ("path", "value", "expected_status", "reason_fragment"),
    (
        ("candidate.parameters.compose_start_mode", "none", "not_run", "compose_start_mode"),
        ("candidate.parameters.republish_probe", False, "not_run", "republish"),
        ("candidate.parameters.use_toxiproxy", False, "not_run", "Toxiproxy"),
        ("candidate.parameters.kill_worker", False, "not_run", "kill"),
        ("candidate.stage_markers", [], "not_run", "stage"),
        ("candidate.load.batch.accepted_calls", 20, "fail", "accepted_calls"),
        (
            "evidence.runtime_fingerprint.stream_group_sha256",
            "0" * 64,
            "not_run",
            "runtime fingerprint",
        ),
        (
            "evidence.runtime_fingerprint.worker_identity_summary_sha256",
            "0" * 64,
            "not_run",
            "runtime fingerprint",
        ),
        (
            "evidence.runtime_fingerprint.parameters_sha256",
            "0" * 64,
            "not_run",
            "runtime fingerprint",
        ),
        (
            "candidate.faults.toxiproxy.api_endpoint",
            "http://user:secret@127.0.0.1:8474",
            "not_run",
            "Toxiproxy",
        ),
        (
            "candidate.faults.redis.disable.api_endpoint",
            "http://127.0.0.1:9999",
            "fail",
            "readback",
        ),
    ),
)
def test_real_runtime_pass_rejects_missing_required_formal_evidence(
    tmp_path, path: str, value: object, expected_status: str, reason_fragment: str
) -> None:
    report_value = _valid_real_runtime_report()
    target: object = report_value
    components = path.split(".")
    for component in components[:-1]:
        target = target[component]  # type: ignore[index]
    target[components[-1]] = value  # type: ignore[index]
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == expected_status
    assert any(reason_fragment.lower() in reason.lower() for reason in reasons)


def test_performance_pass_requires_current_candidate_evidence(tmp_path) -> None:
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps({"production_gate": "pass"}), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["real performance evidence is missing current-candidate lineage"]


def test_performance_pass_rejects_historical_or_mismatched_evidence(tmp_path) -> None:
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(
        json.dumps(
            {
                "production_gate": "pass",
                "evidence": {
                    "schema_version": 1,
                    "kind": "historical",
                    "source_fingerprint": {
                        "algorithm": "sha256",
                        "status": "available",
                        "value": "0" * 64,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["real performance evidence is not marked current_candidate"]

    report.write_text(
        json.dumps(
            {
                "production_gate": "pass",
                "evidence": {
                    "schema_version": 1,
                    "kind": "current_candidate",
                    "producer": "tests.test_release_gate",
                    "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    "run_id": "test-release-gate-mismatch",
                    "run_nonce": "mismatch-run-nonce-123456",
                    "source_fingerprint": {
                        "algorithm": "sha256",
                        "status": "available",
                        "value": "0" * 64,
                    },
                    "runtime_fingerprint": {
                        "algorithm": "sha256",
                        "status": "available",
                        "value": "1" * 64,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["real performance evidence belongs to a different candidate"]


def test_performance_pass_rejects_expired_current_candidate_evidence(tmp_path) -> None:
    report_value = _valid_performance_report()
    report_value["evidence"]["generated_at"] = "2020-01-01T00:00:00Z"  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["production evidence has expired"]


def test_performance_pass_rejects_complete_evidence_from_another_checkout(tmp_path) -> None:
    report_value = _valid_performance_report()
    report_value["evidence"]["source_fingerprint"] = {  # type: ignore[index]
        "algorithm": "sha256",
        "status": "available",
        "value": "0" * 64,
    }
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["real performance evidence belongs to a different candidate"]


def test_performance_pass_rejects_missing_release_binding(tmp_path) -> None:
    report_value = _valid_performance_report()
    report_value["evidence"].pop("release_binding")  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["production evidence release_binding is missing"]


def test_performance_pass_accepts_matching_current_candidate_evidence(tmp_path) -> None:
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(_valid_performance_report()), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("db_pool_size", 16),
        ("max_inflight", 32),
    ),
)
def test_performance_promotion_requires_formal_producer_topology(
    tmp_path: Path, field: str, value: int
) -> None:
    report_value = _valid_performance_report()
    candidate = report_value["candidate"]
    assert isinstance(candidate, dict)
    parameters = candidate["parameters"]
    assert isinstance(parameters, dict)
    parameters[field] = value
    preflight = candidate["preflight"]
    assert isinstance(preflight, dict)
    workers = preflight["worker_processes"]
    assert isinstance(workers, list)
    evidence = report_value["evidence"]
    assert isinstance(evidence, dict)
    evidence["runtime_fingerprint"] = _runtime_fingerprint(
        mode="real_postgresql_redis_multiprocess",
        workers=workers,
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
        parameters=parameters,
    )
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "fail"
    assert field in reasons[0]


@pytest.mark.parametrize(
    ("field", "value", "expected_status"),
    (
        ("worker_count", None, "not_run"),
        ("worker_count", 3, "fail"),
        ("worker_concurrency", None, "not_run"),
        ("worker_concurrency", 49, "fail"),
    ),
)
def test_performance_promotion_requires_auditable_worker_topology(
    tmp_path: Path,
    field: str,
    value: int | None,
    expected_status: str,
) -> None:
    report_value = _valid_performance_report()
    candidate = report_value["candidate"]
    assert isinstance(candidate, dict)
    preflight = candidate["preflight"]
    assert isinstance(preflight, dict)
    if value is None:
        preflight.pop(field)
    else:
        preflight[field] = value
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == expected_status
    assert field in reasons[0]


def test_performance_promotion_accepts_attested_kubernetes_gateway_service(
    tmp_path: Path,
) -> None:
    report_value = _valid_performance_report()
    candidate = report_value["candidate"]
    assert isinstance(candidate, dict)
    preflight = candidate["preflight"]
    assert isinstance(preflight, dict)
    preflight["kubernetes"] = {
        "namespace": "acceptance",
        "namespace_bound": True,
    }
    sustained = candidate["sustained"]
    assert isinstance(sustained, dict)
    sustained["gateway"] = {
        "host_class": "kubernetes_service",
        "service_name": "trpc-gateway",
        "namespace": "acceptance",
        "scheme": "http",
        "port": 8080,
    }
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


def test_performance_promotion_rejects_unbound_kubernetes_gateway_service(
    tmp_path: Path,
) -> None:
    report_value = _valid_performance_report()
    candidate = report_value["candidate"]
    assert isinstance(candidate, dict)
    sustained = candidate["sustained"]
    assert isinstance(sustained, dict)
    sustained["gateway"] = {
        "host_class": "kubernetes_service",
        "service_name": "trpc-gateway",
        "namespace": "acceptance",
        "scheme": "http",
        "port": 8080,
    }
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "candidate.preflight.kubernetes" in reasons[0]


def test_performance_pass_accepts_kubernetes_metrics_with_unlabeled_workers(
    tmp_path: Path,
) -> None:
    report_value = _valid_kubernetes_performance_report()
    workers = report_value["candidate"]["preflight"]["worker_processes"]  # type: ignore[index]
    assert all(worker["source_fingerprint"] is None for worker in workers)
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    assert _status(report, production_field=True) == ("pass", [])


def test_performance_non_kubernetes_worker_source_fingerprint_is_required(
    tmp_path: Path,
) -> None:
    report_value = _valid_performance_report()
    candidate = report_value["candidate"]
    assert isinstance(candidate, dict)
    preflight = candidate["preflight"]
    assert isinstance(preflight, dict)
    workers = preflight["worker_processes"]
    assert isinstance(workers, list)
    for worker in workers:
        assert isinstance(worker, dict)
        worker["source_fingerprint"] = None
    parameters = candidate["parameters"]
    assert isinstance(parameters, dict)
    evidence = report_value["evidence"]
    assert isinstance(evidence, dict)
    evidence["runtime_fingerprint"] = _runtime_fingerprint(
        mode="real_postgresql_redis_multiprocess",
        workers=workers,
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
        parameters=parameters,
    )
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "worker process image/source identity" in reasons[0]


@pytest.mark.parametrize(
    "mutation",
    ("missing_pod_uid", "forged_container_identity", "missing_memory_bytes", "forged_pod_limit"),
)
def test_performance_kubernetes_metrics_memory_evidence_fails_closed(
    tmp_path: Path, mutation: str
) -> None:
    report_value = _valid_kubernetes_performance_report()
    candidate = report_value["candidate"]
    assert isinstance(candidate, dict)
    preflight = candidate["preflight"]
    assert isinstance(preflight, dict)
    participating = preflight["participating_processes"]
    assert isinstance(participating, dict)
    memory_observation = candidate["memory_observation"]
    assert isinstance(memory_observation, dict)
    observations = memory_observation["role_observations"]
    assert isinstance(observations, dict)
    worker_observations = observations["worker"]
    assert isinstance(worker_observations, dict)
    worker_observation = worker_observations["observations"][0]
    assert isinstance(worker_observation, dict)
    if mutation == "missing_pod_uid":
        worker_participating = participating["worker"]
        assert isinstance(worker_participating, list)
        worker_participating[0].pop("pod_uid")
    elif mutation == "forged_container_identity":
        worker_observation["container_id"] = "containerd://forged"
    elif mutation == "missing_memory_bytes":
        worker_observation.pop("memory_bytes")
    else:
        worker_observation["memory_limit_bytes"] += 1

    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons
    assert "memory" in reasons[0].lower() or "pod" in reasons[0].lower()


@pytest.mark.parametrize(
    "field",
    ("value", "worker_identity_summary_sha256", "parameters_sha256"),
)
def test_performance_pass_rejects_replayed_runtime_fingerprint(tmp_path, field: str) -> None:
    report_value = _valid_performance_report()
    report_value["evidence"]["runtime_fingerprint"][field] = "0" * 64  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "runtime fingerprint" in reasons[0]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("callbacks", release_gate.PERFORMANCE_MAX_CALLBACKS + 1),
        ("callback_rate_per_second", release_gate.PERFORMANCE_MAX_CALLBACK_RATE + 1.0),
        ("burst_turns", release_gate.PERFORMANCE_MAX_BURST_TURNS + 1),
        ("min_workers", release_gate.PERFORMANCE_MAX_WORKERS + 1),
        ("db_pool_size", release_gate.PERFORMANCE_MAX_DB_POOL_SIZE + 1),
        ("max_inflight", release_gate.PERFORMANCE_MAX_INFLIGHT + 1),
        ("timeout_seconds", release_gate.PERFORMANCE_MAX_TIMEOUT_SECONDS + 1.0),
    ),
)
def test_performance_parameters_are_bounded_before_promotion(
    tmp_path, field: str, value: object
) -> None:
    report_value = _valid_performance_report()
    report_value["candidate"]["parameters"][field] = value  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "fail"
    assert reasons
    assert field in reasons[0]


def test_performance_connection_estimate_is_recomputed_from_topology(tmp_path) -> None:
    report_value = _valid_performance_report()
    report_value["candidate"]["preflight"]["resources"][  # type: ignore[index]
        "estimated_runtime_connections"
    ] += 1
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "fail"
    assert "connection estimate" in reasons[0]


@pytest.mark.parametrize("mutation", ("missing", "coverage", "peak"))
def test_performance_requires_real_peak_memory_coverage(tmp_path, mutation: str) -> None:
    report_value = _valid_performance_report()
    memory = report_value["candidate"]["memory_observation"]  # type: ignore[index]
    if mutation == "missing":
        report_value["candidate"].pop("memory_observation")  # type: ignore[index]
    elif mutation == "coverage":
        memory["coverage_complete"] = False  # type: ignore[index]
    else:
        memory["peak_bytes"] = memory["safety_threshold_bytes"] + 1  # type: ignore[index]
        memory["within_safety_threshold"] = False  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status in {"not_run", "fail"}
    assert reasons
    assert "memory" in reasons[0].lower()


def test_performance_memory_observation_rejects_extra_role_key(tmp_path) -> None:
    report_value = _valid_performance_report()
    report_value["candidate"]["memory_observation"]["role_observations"]["gateway"] = {}  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("memory" in reason.lower() for reason in reasons)


def test_performance_memory_observation_must_match_preflight_identity(tmp_path) -> None:
    report_value = _valid_performance_report()
    observation = report_value["candidate"]["memory_observation"]  # type: ignore[index]
    observation["role_observations"]["worker"]["observations"][0]["pid"] += 100  # type: ignore[index]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert any("memory" in reason.lower() for reason in reasons)


def test_performance_pass_rejects_semantically_empty_report(tmp_path) -> None:
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(
        json.dumps(
            {
                "production_gate": "pass",
                "evidence": _current_performance_evidence(),
            }
        ),
        encoding="utf-8",
    )

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert reasons == ["real performance evidence is missing or invalid schema_version=1"]


def test_performance_pass_rejects_direct_only_sustained_phase(tmp_path) -> None:
    report = _valid_performance_report()
    report["candidate"]["sustained"]["ingress_mode"] = "direct_runtime"  # type: ignore[index]
    path = tmp_path / REPORTS["performance"][0]
    path.write_text(json.dumps(report), encoding="utf-8")

    status, reasons = _status(path, production_field=True)

    assert status == "not_run"
    assert "synthetic_encrypted_feishu_http" in reasons[0]


def test_performance_pass_requires_successful_bounded_warmup(tmp_path) -> None:
    for mutation in ("missing", "failed", "wrong_steps"):
        report_value = _valid_performance_report()
        candidate = report_value["candidate"]
        if mutation == "missing":
            candidate.pop("warmup")
        elif mutation == "failed":
            candidate["warmup"]["passed"] = False
        else:
            candidate["warmup"]["stages"][2]["requested"] = 80
        case_directory = tmp_path / mutation
        case_directory.mkdir(exist_ok=True)
        report = case_directory / REPORTS["performance"][0]
        report.write_text(json.dumps(report_value), encoding="utf-8")

        status, _reasons = _status(report, production_field=True)

        assert status == "not_run"


def test_performance_pass_rejects_run_id_mismatch(tmp_path) -> None:
    report = _valid_performance_report()
    report["candidate"]["run_id"] = "different-run"  # type: ignore[index]
    path = tmp_path / REPORTS["performance"][0]
    path.write_text(json.dumps(report), encoding="utf-8")

    status, reasons = _status(path, production_field=True)

    assert status == "not_run"
    assert "candidate.run_id matching evidence.run_id" in reasons[0]


@pytest.mark.parametrize(
    ("path", "value", "expected_status"),
    [
        ("candidate.sustained.requested_callbacks", 199, "fail"),
        ("candidate.sustained.offered_callback_rate_per_second", 99.9, "fail"),
        ("candidate.sustained.ack_p95_ms", 200.0, "fail"),
        ("candidate.burst.requested_concurrent_turns", 199, "fail"),
        ("candidate.preflight.worker_image_attestation.worker_count", 3, "fail"),
        ("candidate.sustained.requested_callbacks", "200", "not_run"),
        ("candidate.sustained.offered_callback_rate_per_second", float("nan"), "fail"),
    ],
)
def test_performance_pass_rejects_locked_target_boundaries(
    tmp_path, path: str, value: object, expected_status: str
) -> None:
    report = _valid_performance_report()
    target: object = report
    for component in path.split(".")[:-1]:
        target = target[component]  # type: ignore[index]
    target[path.split(".")[-1]] = value  # type: ignore[index]
    file_path = tmp_path / REPORTS["performance"][0]
    file_path.write_text(json.dumps(report), encoding="utf-8")

    status, _ = _status(file_path, production_field=True)

    assert status == expected_status


def test_performance_pass_rejects_http_failure_duplicate_ids_and_pending_queue(tmp_path) -> None:
    cases = (
        ("candidate.sustained.http_failure_counts", {"timeout": 1}),
        (
            "candidate.sustained.accepted_inbound_ids",
            ["inbound-0"] * 200,
        ),
        ("candidate.redis.after_burst.pending", 1),
    )
    for index, (path, value) in enumerate(cases):
        report = _valid_performance_report()
        target: object = report
        components = path.split(".")
        for component in components[:-1]:
            target = target[component]  # type: ignore[index]
        target[components[-1]] = value  # type: ignore[index]
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        file_path = case_dir / REPORTS["performance"][0]
        file_path.write_text(json.dumps(report), encoding="utf-8")

        status, _ = _status(file_path, production_field=True)

        assert status == "fail"


def test_production_pass_rejects_evidence_replayed_across_reports(tmp_path) -> None:
    performance = tmp_path / REPORTS["performance"][0]
    deployment = tmp_path / REPORTS["deployment"][0]
    valid_performance = _valid_performance_report()
    evidence = valid_performance["evidence"]
    performance.write_text(json.dumps(valid_performance), encoding="utf-8")
    deployment.write_text(
        json.dumps({"production_gate": "pass", "evidence": evidence}),
        encoding="utf-8",
    )

    assert _status(performance, production_field=True) == ("pass", [])
    assert _status(deployment, production_field=True) == (
        "not_run",
        ["production evidence producer is not allowed for kubernetes-runtime.json"],
    )


def test_production_pass_rejects_unknown_evidence_producer(tmp_path) -> None:
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(
        json.dumps(
            {
                "production_gate": "pass",
                "evidence": _current_evidence("tests.test_release_gate"),
            }
        ),
        encoding="utf-8",
    )

    assert _status(report, production_field=True) == (
        "not_run",
        ["production evidence producer is not allowed for real-performance.json"],
    )


@pytest.mark.parametrize(
    ("path", "value", "expected_status"),
    [
        (("candidate", "kind"), "simulation", "not_run"),
        (("candidate", "selectors"), [], "not_run"),
        (("candidate", "exit_code"), 1, "fail"),
        (("candidate", "duration_seconds"), float("nan"), "fail"),
        (("candidate", "test_counts", "skipped"), 1, "not_run"),
        (("candidate", "lineage", "status"), "not_run", "not_run"),
        (("candidate", "lineage", "image_digest"), "sha256:" + "0" * 64, "not_run"),
        (("case_deltas", "failed_processes"), 1, "fail"),
    ],
)
def test_backend_production_pass_requires_real_contract_semantics(
    tmp_path, path: tuple[str, ...], value: object, expected_status: str
) -> None:
    payload = _valid_backend_report()
    target: dict[str, Any] = payload
    for key in path[:-1]:
        target = target[key]  # type: ignore[assignment]
    target[path[-1]] = value
    report = tmp_path / REPORTS["backend"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, _reasons = _status(report, production_field=True)

    assert status == expected_status


def test_fault_production_pass_requires_complete_observed_scenarios(tmp_path) -> None:
    payload = _valid_fault_report(tmp_path / REPORTS["fault_injection"][0])
    scenarios = payload["candidate"]["scenarios"]  # type: ignore[index]
    scenarios.pop("ambiguous")  # type: ignore[union-attr]
    report = tmp_path / REPORTS["fault_injection"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "complete inventory" in reasons[0]


@pytest.mark.parametrize("scenario_name", ("redis_interrupt", "republish", "dlq"))
def test_fault_production_reads_real_faults_child_phase(tmp_path, scenario_name: str) -> None:
    report_path = tmp_path / REPORTS["fault_injection"][0]
    payload = _valid_fault_report(report_path)
    scenario = payload["candidate"]["scenarios"][scenario_name]  # type: ignore[index]
    child_path = Path(scenario["child_report"])  # type: ignore[index]
    child = json.loads(child_path.read_text(encoding="utf-8"))

    assert scenario["child_phase"] == "fault"  # type: ignore[index]
    assert child["case_deltas"]["requested_phase"] == "fault"
    assert "faults" in child["candidate"]
    assert "fault" not in child["candidate"]
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    assert _status(report_path, production_field=True) == ("pass", [])


def test_fault_production_prefers_component_markers_over_aggregate_and_rejects_tamper(
    tmp_path,
) -> None:
    report_path = tmp_path / REPORTS["fault_injection"][0]
    payload = _valid_fault_report(report_path)
    scenario = payload["candidate"]["scenarios"]["redis_interrupt"]  # type: ignore[index]
    assert isinstance(scenario, dict)
    child_path = Path(scenario["child_report"])
    child = json.loads(child_path.read_text(encoding="utf-8"))
    phase = child["candidate"]["faults"]
    assert isinstance(phase, dict)
    component = phase["redis"]
    assert isinstance(component, dict)
    parent_markers = scenario["stage_markers"]
    assert isinstance(parent_markers, list)
    selected_markers = [dict(marker, component="redis") for marker in parent_markers]
    scenario["stage_markers"] = selected_markers
    component["stage_markers"] = selected_markers
    phase["stage_markers"] = [dict(marker, component="postgres") for marker in selected_markers]
    atomic_write_json(child_path, child)
    scenario["child_report_sha256"] = canonical_sha256(child)
    scenario["child_report_mtime_ns"] = child_path.stat().st_mtime_ns
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    assert _status(report_path, production_field=True) == ("pass", [])

    selected_markers[0]["component"] = "tampered"
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    status, reasons = _status(report_path, production_field=True)

    assert status == "not_run"
    assert "child mismatch" in reasons[0]


def test_fault_production_pass_requires_ambiguous_provider_ledger(tmp_path) -> None:
    payload = _valid_fault_report(tmp_path / REPORTS["fault_injection"][0])
    scenario = payload["candidate"]["scenarios"]["ambiguous"]  # type: ignore[index]
    evidence = scenario["evidence"]
    evidence["provider_ledger"]["side_effect_count"] = 2
    child_path = Path(scenario["child_report"])
    child = json.loads(child_path.read_text(encoding="utf-8"))
    child["candidate"]["ambiguous"]["provider_ledger"]["side_effect_count"] = 2
    atomic_write_json(child_path, child)
    scenario["child_report_sha256"] = canonical_sha256(child)
    scenario["child_report_mtime_ns"] = child_path.stat().st_mtime_ns
    report = tmp_path / REPORTS["fault_injection"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "provider ledger idempotency evidence" in reasons[0]


def test_fault_production_pass_rejects_duplicate_or_nonpassing_marker(tmp_path) -> None:
    for mutation in ("duplicate", "not_run"):
        payload = _valid_fault_report(tmp_path / mutation / REPORTS["fault_injection"][0])
        scenario = payload["candidate"]["scenarios"]["redis_interrupt"]  # type: ignore[index]
        markers = scenario["stage_markers"]
        if mutation == "duplicate":
            markers.append(dict(markers[0]))
        else:
            markers[0]["status"] = "not_run"
        case_directory = tmp_path / mutation
        case_directory.mkdir(exist_ok=True)
        report = case_directory / REPORTS["fault_injection"][0]
        report.write_text(json.dumps(payload), encoding="utf-8")

        status, _reasons = _status(report, production_field=True)

        assert status == "not_run"


@pytest.mark.parametrize(
    "field",
    (
        "child_report_sha256",
        "child_run_id",
        "child_nonce_sha256",
        "child_started_at",
        "child_ended_at",
        "observed_exit_code",
        "child_report_path_scope",
        "child_report_path_confined",
    ),
)
def test_fault_production_pass_requires_parent_child_lineage(field: str, tmp_path) -> None:
    payload = _valid_fault_report(tmp_path / REPORTS["fault_injection"][0])
    payload["candidate"]["scenarios"]["worker_enqueue"].pop(field)  # type: ignore[index]
    report = tmp_path / REPORTS["fault_injection"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, _reasons = _status(report, production_field=True)

    assert status == "not_run"


def test_fault_release_reloads_trusted_child_and_rejects_replacement(tmp_path) -> None:
    report_path = tmp_path / REPORTS["fault_injection"][0]
    payload = _valid_fault_report(report_path)
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    assert _status(report_path, production_field=True) == ("pass", [])

    child_path = Path(
        payload["candidate"]["scenarios"]["redis_interrupt"]["child_report"]  # type: ignore[index]
    )
    child = json.loads(child_path.read_text(encoding="utf-8"))
    child["gate"] = "fail"
    child_path.write_text(json.dumps(child), encoding="utf-8")

    status, reasons = _status(report_path, production_field=True)

    assert status == "not_run"
    assert "child_report mtime mismatch" in reasons[0]


def test_fault_release_rejects_non_strict_child_json(tmp_path) -> None:
    report_path = tmp_path / REPORTS["fault_injection"][0]
    payload = _valid_fault_report(report_path)
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    child_path = Path(
        payload["candidate"]["scenarios"]["redis_interrupt"]["child_report"]  # type: ignore[index]
    )
    recorded_mtime = payload["candidate"]["scenarios"]["redis_interrupt"][  # type: ignore[index]
        "child_report_mtime_ns"
    ]
    child_path.write_text('{"gate": 1, "gate": 2}', encoding="utf-8")
    assert isinstance(recorded_mtime, int)
    os.utime(child_path, ns=(recorded_mtime, recorded_mtime))

    status, reasons = _status(report_path, production_field=True)

    assert status == "not_run"
    assert "strict JSON" in reasons[0]


@pytest.mark.parametrize("mutation", ("missing", "escape"))
def test_fault_release_rejects_missing_or_untrusted_child_path(tmp_path, mutation: str) -> None:
    report_path = tmp_path / REPORTS["fault_injection"][0]
    payload = _valid_fault_report(report_path)
    scenario = payload["candidate"]["scenarios"]["redis_interrupt"]  # type: ignore[index]
    child_path = Path(scenario["child_report"])
    if mutation == "missing":
        child_path.unlink()
    else:
        outside = tmp_path / "outside-child.json"
        outside.write_text(child_path.read_text(encoding="utf-8"), encoding="utf-8")
        scenario["child_report"] = str(outside)
        scenario["child_report_path_scope"] = str(tmp_path)
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report_path, production_field=True)

    assert status == "not_run"
    assert "child_report" in reasons[0]


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("missing_case", "exact inventory"),
        ("nonce_mismatch", "run_nonce"),
        ("credential_fingerprints", "credential_attestation"),
        ("automatic_replay", "zero automatic replay"),
        ("drop_response", "drop-response"),
        ("rate_code", "provider rate-limit code"),
        ("retry_attempts", "retry_attempts bounds"),
        ("retry_elapsed", "did not honor Retry-After"),
        ("short_outage", "outage_seconds bounds"),
        ("reconnect_endpoint_set", "EndpointSlice observation"),
    ],
)
def test_online_im_pass_requires_complete_provider_evidence(
    tmp_path, mutation: str, expected_reason: str, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    channels = payload["candidate"]["channels"]  # type: ignore[index]
    feishu = channels["feishu"]  # type: ignore[index]
    if mutation == "missing_case":
        feishu["provider_evidence"]["observations"].pop("media")  # type: ignore[index]
    elif mutation == "nonce_mismatch":
        feishu["provider_evidence"]["observations"]["round_trip"][  # type: ignore[index]
            "run_nonce"
        ] = "different-online-im-nonce"
    elif mutation == "credential_fingerprints":
        feishu["provider_evidence"]["credential_attestation"]["fingerprints"] = {  # type: ignore[index]
            "secret": "4" * 64
        }
    elif mutation == "automatic_replay":
        feishu["provider_evidence"]["observations"]["ambiguous"][  # type: ignore[index]
            "auto_replay_count"
        ] = 1
    elif mutation == "drop_response":
        feishu["provider_evidence"]["observations"]["ambiguous"][  # type: ignore[index]
            "drop_response_observed"
        ] = False
    elif mutation == "rate_code":
        feishu["provider_evidence"]["observations"]["rate_limit_retry_after"][  # type: ignore[index]
            "provider_error_code"
        ] = "rate_limited"
    elif mutation == "retry_attempts":
        feishu["provider_evidence"]["observations"]["rate_limit_retry_after"][  # type: ignore[index]
            "retry_attempts"
        ] = 1
    elif mutation == "retry_elapsed":
        feishu["provider_evidence"]["observations"]["rate_limit_retry_after"][  # type: ignore[index]
            "retry_elapsed_seconds"
        ] = 1.0
    elif mutation == "short_outage":
        feishu["provider_evidence"]["observations"]["prolonged_outage"][  # type: ignore[index]
            "outage_seconds"
        ] = 59.0
    else:
        feishu["provider_evidence"]["observations"]["reconnect"][  # type: ignore[index]
            "endpoint_set_observed"
        ] = False
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert expected_reason in reasons[0]


def test_online_im_release_accepts_im_gate_sanitized_feishu_reconnect(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    evidence = payload["evidence"]
    feishu = payload["candidate"]["channels"]["feishu"]  # type: ignore[index]
    run_nonce = feishu["provider_evidence"]["run_nonce"]  # type: ignore[index]
    feishu["provider_evidence"] = _sanitized_feishu_probe_evidence(
        run_nonce=run_nonce,
        observed_at=evidence["generated_at"],  # type: ignore[index]
    )
    reconnect = feishu["provider_evidence"]["observations"]["reconnect"]  # type: ignore[index]
    assert "failed_endpoint_id_hash" in reconnect
    assert "replacement_endpoint_id_hash" in reconnect
    assert "endpoint_set_observed" in reconnect
    assert "lock_epoch" not in reconnect
    assert "old_lock_owner_released" not in reconnect
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "pass"
    assert reasons == []


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    (
        ("replacement_endpoint_id_hash", "7" * 64, "distinct endpoints"),
        ("acknowledged_request_id_hash", "1" * 64, "ACK binding"),
        ("ready_endpoint_count", 0, "ready endpoints"),
        ("unready_endpoint_count", 1, "unstable endpoints"),
    ),
)
def test_online_im_release_rechecks_feishu_endpoint_failover_invariants(
    tmp_path, field: str, value: object, expected_reason: str, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    reconnect = payload["candidate"]["channels"]["feishu"]["provider_evidence"][  # type: ignore[index]
        "observations"
    ]["reconnect"]
    reconnect[field] = value
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert expected_reason in reasons[0]


def test_online_im_release_requires_wecom_reconnect_lock_lifecycle(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    reconnect = payload["candidate"]["channels"]["wecom"]["provider_evidence"][  # type: ignore[index]
        "observations"
    ]["reconnect"]
    reconnect.pop("lock_epoch")
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert "reconnect fields" in reasons[0]


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("runtime_source", "runtime_attestation"),
        ("runtime_extra", "runtime_attestation"),
        ("driver_hash", "artifact hashes"),
        ("provider_artifact", "provider artifact_attestation binding"),
        ("runner_mismatch", "shared runner artifact"),
    ),
)
def test_online_im_release_rechecks_runtime_and_artifact_attestations(
    tmp_path, mutation: str, expected_reason: str, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    channels = payload["candidate"]["channels"]  # type: ignore[index]
    feishu = channels["feishu"]  # type: ignore[index]
    wecom = channels["wecom"]  # type: ignore[index]
    if mutation == "runtime_source":
        feishu["runtime_attestation"]["source_fingerprint"] = "9" * 64  # type: ignore[index]
    elif mutation == "runtime_extra":
        feishu["runtime_attestation"]["unexpected"] = True  # type: ignore[index]
    elif mutation == "driver_hash":
        feishu["artifact_attestation"]["driver_sha256"] = "0" * 64  # type: ignore[index]
        feishu["provider_evidence"]["artifact_attestation"][  # type: ignore[index]
            "driver_sha256"
        ] = "0" * 64
    elif mutation == "provider_artifact":
        feishu["provider_evidence"]["artifact_attestation"][  # type: ignore[index]
            "driver_sha256"
        ] = "e" * 64
    else:
        wecom["artifact_attestation"]["runner_sha256"] = "e" * 64  # type: ignore[index]
        wecom["provider_evidence"]["artifact_attestation"][  # type: ignore[index]
            "runner_sha256"
        ] = "e" * 64
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert expected_reason in reasons[0]


@pytest.mark.parametrize(
    ("case", "field", "value", "expected_reason"),
    (
        ("reconnect", "acknowledged_request_id_hash", "5" * 64, "ACK binding"),
        ("reconnect", "provider_code", "500", "provider ACK code"),
        ("credential_rotation", "acknowledged_request_id_hash", "5" * 64, "ACK binding"),
        ("credential_rotation", "provider_code", "500", "provider ACK code"),
    ),
)
def test_online_im_release_rechecks_wecom_send_ack_binding(
    tmp_path, case: str, field: str, value: object, expected_reason: str, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    observation = payload["candidate"]["channels"]["wecom"]["provider_evidence"][  # type: ignore[index]
        "observations"
    ][case]
    observation[field] = value
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert expected_reason in reasons[0]


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    [
        ("outage_mode", "provider_delivery_gap", "outage_mode=service_failover"),
        ("takeover_instance_id_hash", "1" * 64, "distinct failover instances"),
        ("old_lock_owner_released", None, "lock handoff evidence"),
        ("new_lock_owner_acquired", False, "lock handoff evidence"),
        ("connection_epoch", 1, "connection_epoch"),
        ("reply_for_event_id_hash", "5" * 64, "reply event binding"),
        ("acknowledged_request_id_hash", "6" * 64, "send acknowledgement binding"),
        ("reply_count", 2, "reply_count=1"),
        ("pending_count", 1, "pending_count=0"),
    ],
)
def test_online_im_release_rechecks_wecom_service_failover_invariants(
    tmp_path, field: str, value: object, expected_reason: str, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    outage = payload["candidate"]["channels"]["wecom"]["provider_evidence"][  # type: ignore[index]
        "observations"
    ]["prolonged_outage"]
    outage[field] = value
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "not_run"
    assert expected_reason in reasons[0]


def test_online_im_release_accepts_wecom_hard_failover_without_fake_release(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    observations = payload["candidate"]["channels"]["wecom"]["provider_evidence"][  # type: ignore[index]
        "observations"
    ]
    observations["reconnect"]["old_lock_owner_released"] = False
    observations["prolonged_outage"]["old_lock_owner_released"] = False
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "pass"
    assert reasons == []


@pytest.mark.parametrize(
    "mutation", ("identity", "signature", "allowlist", "nonfinite", "oversized")
)
def test_online_im_production_pass_rejects_unbound_or_out_of_range_probe_values(
    tmp_path, mutation: str, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    payload = _valid_online_im_report(trust)
    if mutation == "identity":
        payload["candidate"]["probe"].pop("identity_attestation")  # type: ignore[index]
    elif mutation == "signature":
        payload["candidate"]["probe"]["identity_attestation"][  # type: ignore[index]
            "signature_verified"
        ] = False
    elif mutation == "allowlist":
        payload["candidate"]["probe"]["endpoint_allowlisted"] = False  # type: ignore[index]
    elif mutation == "nonfinite":
        payload["candidate"]["channels"]["feishu"]["provider_evidence"]["observations"][  # type: ignore[index]
            "rate_limit_retry_after"
        ]["retry_after_seconds"] = float("nan")
    else:
        payload["candidate"]["channels"]["feishu"]["provider_evidence"]["observations"][  # type: ignore[index]
            "media"
        ]["bytes"] = 64 * 1024 * 1024 + 1
    report = tmp_path / REPORTS["online_im"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, _reasons = _status(report, production_field=True)

    assert status in {"not_run", "fail"}


@pytest.mark.parametrize(
    "raw",
    (
        '{"schema_version":1,"schema_version":1,"gate":"pass"}',
        '{"schema_version":1,"gate":"pass","value":NaN}',
    ),
)
def test_release_status_rejects_non_strict_json(raw: str, tmp_path) -> None:
    report = tmp_path / "strict.json"
    report.write_text(raw, encoding="utf-8")

    status, reasons = _status(report, production_field=False)

    assert status == "fail"
    assert reasons and "invalid report strict.json" in reasons[0]


def test_release_status_rejects_boolean_schema_version(tmp_path) -> None:
    report = tmp_path / "boolean-schema.json"
    report.write_text('{"schema_version":true,"gate":"pass"}', encoding="utf-8")

    status, reasons = _status(report, production_field=False)

    assert status == "fail"
    assert reasons == ["invalid schema_version in boolean-schema.json"]


def test_online_im_release_rejects_missing_or_rotated_current_trust(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    report = tmp_path / REPORTS["online_im"][0]
    payload = _valid_online_im_report(trust)
    report.write_text(json.dumps(payload), encoding="utf-8")
    assert _status(report, production_field=True) == ("pass", [])

    trust_path = release_gate.IM_PROBE_TRUST_PATH
    trust_path.unlink()
    status, reasons = _status(report, production_field=True)
    assert status == "not_run"
    assert "missing" in reasons[0]

    rotated = {
        "schema_version": 1,
        "probe_url": "https://probe.example.test",
        "key_id": "rotated-fixture-key",
        "ed25519_public_key": base64.b64encode(bytes(range(33, 65))).decode("ascii"),
    }
    atomic_write_json(trust_path, rotated)
    status, reasons = _status(report, production_field=True)
    assert status == "not_run"
    assert "trust" in reasons[0]


def test_production_gate_imports_the_reserved_module_not_only_scripts_package(
    tmp_path, monkeypatch
) -> None:
    report = tmp_path / REPORTS["backend"][0]
    report.write_text(json.dumps(_valid_backend_report()), encoding="utf-8")
    imported: list[str] = []
    original = release_gate.importlib.import_module

    def record(name: str):
        imported.append(name)
        return original(name)

    monkeypatch.setattr(release_gate.importlib, "import_module", record)

    assert _status(report, production_field=True) == ("pass", [])
    assert imported == [PRODUCTION_EVIDENCE_PRODUCERS[report.name]]


def test_each_production_report_accepts_only_its_reserved_producer(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    for filename, expected_producer in PRODUCTION_EVIDENCE_PRODUCERS.items():
        report = tmp_path / filename
        report.write_text(
            json.dumps(
                _valid_performance_report()
                if filename == REPORTS["performance"][0]
                else _valid_backend_report()
                if filename == REPORTS["backend"][0]
                else _valid_real_runtime_report()
                if filename == REPORTS["real_runtime"][0]
                else _valid_migration_report()
                if filename == REPORTS["migration"][0]
                else _valid_fault_report(report)
                if filename == REPORTS["fault_injection"][0]
                else _valid_online_im_report(trust)
                if filename == REPORTS["online_im"][0]
                else _valid_kubernetes_report()
                if filename == REPORTS["deployment"][0]
                else _valid_disaster_recovery_report(tmp_path)
                if filename == REPORTS["disaster_recovery"][0]
                else {
                    "production_gate": "pass",
                    "evidence": _current_evidence(expected_producer),
                }
            ),
            encoding="utf-8",
        )
        assert _status(report, production_field=True) == ("pass", []), filename


def test_legacy_fault_offline_report_cannot_raise_production_status(tmp_path, monkeypatch) -> None:
    (tmp_path / "fault-injection-offline.json").write_text(
        json.dumps(
            {
                "production_gate": "pass",
                "evidence": _current_evidence("scripts.fault_injection_gate"),
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["release_gate.py", "--directory", str(tmp_path), "--output", str(output)],
    )

    assert main() == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["fault_injection"] == "not_run"
    assert any(
        reason.startswith("fault_injection: missing report: fault-injection.json")
        for reason in result["rejection_reasons"]
    )


def test_candidate_fingerprint_excludes_symlinks(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "real.txt").write_text("stable", encoding="utf-8")
    link = source / "linked.txt"
    try:
        link.symlink_to(source / "real.txt")
    except (NotImplementedError, OSError):
        return

    monkeypatch.setattr("scripts.release_gate.ROOT", tmp_path)
    monkeypatch.setattr("scripts.release_gate.SOURCE_FINGERPRINT_ROOTS", ("source",))

    fingerprint = _current_candidate_source_fingerprint()

    assert fingerprint["status"] == "available"
    assert fingerprint["file_count"] == 1
    assert fingerprint["total_bytes"] == len(b"stable")


def test_candidate_fingerprint_reports_file_count_limit(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for index in range(3):
        (source / f"file-{index}.txt").write_text(str(index), encoding="utf-8")

    monkeypatch.setattr("scripts.release_gate.ROOT", tmp_path)
    monkeypatch.setattr("scripts.release_gate.SOURCE_FINGERPRINT_ROOTS", ("source",))
    monkeypatch.setattr("scripts.release_gate.FINGERPRINT_MAX_FILES", 2)

    fingerprint = _current_candidate_source_fingerprint()

    assert fingerprint["status"] == "unavailable"
    assert fingerprint["reason"] == "source_file_count_limit_exceeded"
    assert fingerprint["file_count_limit"] == 2
    assert FINGERPRINT_MAX_FILES == 10_000


def test_candidate_fingerprint_reports_byte_limit(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "large.txt").write_text("0123456789", encoding="utf-8")

    monkeypatch.setattr("scripts.release_gate.ROOT", tmp_path)
    monkeypatch.setattr("scripts.release_gate.SOURCE_FINGERPRINT_ROOTS", ("source",))
    monkeypatch.setattr("scripts.release_gate.FINGERPRINT_MAX_BYTES", 5)

    fingerprint = _current_candidate_source_fingerprint()

    assert fingerprint["status"] == "unavailable"
    assert fingerprint["reason"] == "source_byte_limit_exceeded"
    assert fingerprint["byte_limit"] == 5
    assert FINGERPRINT_MAX_BYTES == 128 * 1024 * 1024


def test_release_gate_rejects_non_object_json_report(tmp_path) -> None:
    report = tmp_path / "report.json"
    report.write_text("[]", encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "fail"
    assert reasons == ["invalid report report.json: root must be a JSON object"]


def test_production_report_cannot_hide_failed_gate_behind_production_pass(tmp_path) -> None:
    payload = _valid_performance_report()
    payload["gate"] = "fail"
    payload["rejection_reasons"] = ["development contract failed"]
    report = tmp_path / REPORTS["performance"][0]
    report.write_text(json.dumps(payload), encoding="utf-8")

    status, reasons = _status(report, production_field=True)

    assert status == "fail"
    assert reasons


def _write_complete_report_set(
    directory, *, failed: str | None = None, trust: dict[str, str] | None = None
) -> None:
    for name, (filename, production_field) in REPORTS.items():
        if name == failed:
            payload: dict[str, Any] = {"gate": "fail", "rejection_reasons": ["test failure"]}
        elif production_field:
            payload = (
                _valid_performance_report()
                if filename == REPORTS["performance"][0]
                else _valid_backend_report()
                if filename == REPORTS["backend"][0]
                else _valid_real_runtime_report()
                if filename == REPORTS["real_runtime"][0]
                else _valid_migration_report()
                if filename == REPORTS["migration"][0]
                else _valid_fault_report(directory / filename)
                if filename == REPORTS["fault_injection"][0]
                else _valid_online_im_report(trust)
                if filename == REPORTS["online_im"][0]
                else _valid_kubernetes_report()
                if filename == REPORTS["deployment"][0]
                else _valid_disaster_recovery_report(directory)
                if filename == REPORTS["disaster_recovery"][0]
                else {
                    "production_gate": "pass",
                    "evidence": _current_evidence(PRODUCTION_EVIDENCE_PRODUCERS[filename]),
                }
            )
        else:
            payload = {"gate": "pass"}
        (directory / filename).write_text(json.dumps(payload), encoding="utf-8")
    production_reports = {
        name: filename for name, (filename, production) in REPORTS.items() if production
    }
    manifest = build_manifest(
        directory,
        reports=production_reports,
        release_id="test-release-bundle",
        release_nonce="r" * 32,
        image_digest="sha256:" + "a" * 64,
    )
    atomic_write_json(directory / "release-manifest.json", manifest)


def _functional_dr_manifest_reports() -> dict[str, str]:
    reports = {
        name: filename
        for name, (filename, production) in REPORTS.items()
        if production and name != "disaster_recovery"
    }
    reports["functional_disaster_recovery"] = FUNCTIONAL_DR_REPORT[0]
    return reports


def _functional_dr_manifest_contract() -> dict[str, tuple[str, str]]:
    return {
        name: (
            filename,
            FUNCTIONAL_DR_REPORT[1]
            if filename == FUNCTIONAL_DR_REPORT[0]
            else PRODUCTION_EVIDENCE_PRODUCERS[filename],
        )
        for name, filename in _functional_dr_manifest_reports().items()
    }


def _write_functional_dr_waiver_report_set(
    directory: Path,
    *,
    trust: dict[str, str],
    destructive_status: str = "not_run",
) -> None:
    _write_complete_report_set(directory, trust=trust)
    functional = _valid_functional_disaster_recovery_report(directory)
    atomic_write_json(directory / FUNCTIONAL_DR_REPORT[0], functional)
    atomic_write_json(
        directory / REPORTS["disaster_recovery"][0],
        {
            "schema_version": 1,
            "production_gate": destructive_status,
            "production_rejection_reasons": ["destructive production DR was not requested"],
        },
    )
    manifest = build_manifest(
        directory,
        reports=_functional_dr_manifest_reports(),
        release_id="test-release-bundle",
        release_nonce="r" * 32,
        image_digest="sha256:" + "a" * 64,
        allow_functional_dr=True,
        authorized_not_run_gates=("disaster_recovery",),
    )
    atomic_write_json(directory / "release-manifest.json", manifest)


def test_explicit_functional_dr_waiver_allows_only_destructive_not_run(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--allow-functional-dr",
            "--require-production",
        ],
    )

    assert main() == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["functional_disaster_recovery"] == "pass"
    assert result["candidate"]["disaster_recovery"] == "not_run"
    assert result["candidate"]["online_im"] == "pass"
    assert result["authorized_not_run_gates"] == ["disaster_recovery"]
    assert result["case_deltas"]["not_run_gates"] == 0
    assert result["runtime_production_gate"] == "pass"
    assert result["gate"] == "pass"


def test_explicit_functional_dr_waiver_never_hides_destructive_failure(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(
        tmp_path,
        trust=trust,
        destructive_status="fail",
    )
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--allow-functional-dr",
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["disaster_recovery"] == "fail"
    assert result["authorized_not_run_gates"] == []
    assert result["runtime_production_gate"] == "fail"
    assert result["gate"] == "fail"


def test_functional_dr_does_not_waive_destructive_not_run_without_explicit_flag(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert "functional_disaster_recovery" not in result["candidate"]
    assert result["candidate"]["disaster_recovery"] == "not_run"
    assert result["authorized_not_run_gates"] == []
    assert result["gate"] != "pass"


def test_explicit_functional_dr_waiver_treats_destructive_gate_failure_as_failure(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    atomic_write_json(
        tmp_path / REPORTS["disaster_recovery"][0],
        {
            "schema_version": 1,
            "gate": "fail",
            "production_gate": "not_run",
            "production_rejection_reasons": ["destructive production DR failed"],
        },
    )
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--allow-functional-dr",
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["disaster_recovery"] == "fail"
    assert result["authorized_not_run_gates"] == []
    assert result["runtime_production_gate"] == "fail"


def test_explicit_functional_dr_waiver_does_not_reclassify_invalid_destructive_pass(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    atomic_write_json(
        tmp_path / REPORTS["disaster_recovery"][0],
        {"schema_version": 1, "gate": "pass", "production_gate": "pass"},
    )
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--allow-functional-dr",
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["disaster_recovery"] == "not_run"
    assert result["authorized_not_run_gates"] == []
    assert result["gate"] != "pass"


@pytest.mark.parametrize(
    "mutation", ("missing", "component", "cleanup", "producer", "lineage", "runtime")
)
def test_explicit_functional_dr_waiver_requires_valid_current_functional_evidence(
    tmp_path, monkeypatch, mutation: str
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    path = tmp_path / FUNCTIONAL_DR_REPORT[0]
    if mutation == "missing":
        path.unlink()
    else:
        report = json.loads(path.read_text(encoding="utf-8"))
        if mutation == "component":
            report["candidate"]["components"]["postgres_pitr"]["status"] = "fail"
        elif mutation == "cleanup":
            report["candidate"]["orchestration"]["cleanup_completed"] = False
        elif mutation == "producer":
            report["evidence"]["producer"] = "scripts.disaster_recovery_gate"
        elif mutation == "runtime":
            report["candidate"]["runtime_attestation"]["jobs"][0]["job_uid_sha256"] = "0" * 64
        else:
            report["candidate"]["lineage"]["image_digest"] = "sha256:" + "b" * 64
        atomic_write_json(path, report)
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--allow-functional-dr",
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["functional_disaster_recovery"] != "pass"
    assert result["authorized_not_run_gates"] == []
    assert result["gate"] != "pass"


def test_explicit_functional_dr_waiver_does_not_exempt_online_im(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    atomic_write_json(
        tmp_path / REPORTS["online_im"][0],
        {
            "schema_version": 1,
            "production_gate": "not_run",
            "production_rejection_reasons": ["online IM was not run"],
        },
    )
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--allow-functional-dr",
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["online_im"] == "not_run"
    assert result["authorized_not_run_gates"] == ["disaster_recovery"]
    assert result["runtime_production_gate"] != "pass"
    assert result["gate"] != "pass"


@pytest.mark.parametrize("policy_mutation", ("missing", "authorized_gates"))
def test_functional_dr_manifest_policy_is_required_and_tamper_evident(
    tmp_path, monkeypatch, policy_mutation: str
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    contract = _functional_dr_manifest_contract()

    assert validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
        allow_functional_dr=True,
        authorized_not_run_gates=("disaster_recovery",),
    ) == ("pass", [])

    manifest_path = tmp_path / "release-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "functional_disaster_recovery" in manifest["reports"]
    assert "disaster_recovery" not in manifest["reports"]
    assert manifest["policy"]["authorized_not_run_gates"] == ["disaster_recovery"]
    if policy_mutation == "missing":
        manifest.pop("policy")
    else:
        manifest["policy"]["authorized_not_run_gates"] = []
    atomic_write_json(manifest_path, manifest)
    status, reasons = validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
        allow_functional_dr=True,
        authorized_not_run_gates=("disaster_recovery",),
    )

    assert status == "fail"
    assert any("functional DR policy" in reason for reason in reasons)


@pytest.mark.parametrize("mutation", ("missing", "component", "cleanup", "producer", "lineage"))
def test_functional_dr_manifest_generation_rejects_invalid_functional_evidence(
    tmp_path, monkeypatch, mutation: str
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    path = tmp_path / FUNCTIONAL_DR_REPORT[0]
    if mutation == "missing":
        path.unlink()
    else:
        report = json.loads(path.read_text(encoding="utf-8"))
        if mutation == "component":
            report["candidate"]["components"]["key_restore"]["status"] = "fail"
        elif mutation == "cleanup":
            report["candidate"]["orchestration"]["cleanup_completed"] = False
        elif mutation == "producer":
            report["evidence"]["producer"] = "scripts.disaster_recovery_gate"
        else:
            report["candidate"]["lineage"]["image_digest"] = "sha256:" + "b" * 64
        atomic_write_json(path, report)

    with pytest.raises(ValueError):
        build_manifest(
            tmp_path,
            reports=_functional_dr_manifest_reports(),
            release_id="test-release-bundle",
            release_nonce="r" * 32,
            image_digest="sha256:" + "a" * 64,
            allow_functional_dr=True,
            authorized_not_run_gates=("disaster_recovery",),
        )


def test_functional_dr_manifest_validation_rechecks_semantics_even_if_hash_is_rewritten(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_functional_dr_waiver_report_set(tmp_path, trust=trust)
    report_path = tmp_path / FUNCTIONAL_DR_REPORT[0]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidate"]["components"]["artifact_restore"]["status"] = "fail"
    atomic_write_json(report_path, report)
    manifest_path = tmp_path / "release-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["reports"]["functional_disaster_recovery"]["sha256"] = canonical_sha256(report)
    atomic_write_json(manifest_path, manifest)

    status, reasons = validate_manifest(
        tmp_path,
        reports=_functional_dr_manifest_contract(),
        current_source=_current_candidate_source_fingerprint(),
        allow_functional_dr=True,
        authorized_not_run_gates=("disaster_recovery",),
    )

    assert status == "fail"
    assert any(
        "functional disaster recovery component artifact_restore" in reason for reason in reasons
    )


def test_release_manifest_generation_rejects_nonpassing_standard_production_report(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    report_path = tmp_path / REPORTS["online_im"][0]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["gate"] = "fail"
    report["rejection_reasons"] = ["online IM failed"]
    atomic_write_json(report_path, report)
    production_reports = {
        name: filename for name, (filename, production) in REPORTS.items() if production
    }

    with pytest.raises(ValueError, match=r"im-online\.json is not production-valid"):
        build_manifest(
            tmp_path,
            reports=production_reports,
            release_id="test-release-bundle",
            release_nonce="r" * 32,
            image_digest="sha256:" + "a" * 64,
        )


def test_release_manifest_validation_rechecks_standard_production_status(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    report_path = tmp_path / REPORTS["online_im"][0]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["production_gate"] = "not_run"
    report["production_rejection_reasons"] = ["online IM was not run"]
    atomic_write_json(report_path, report)
    manifest_path = tmp_path / "release-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["reports"]["online_im"]["sha256"] = canonical_sha256(report)
    atomic_write_json(manifest_path, manifest)
    contract = {
        name: (filename, PRODUCTION_EVIDENCE_PRODUCERS[filename])
        for name, (filename, production) in REPORTS.items()
        if production
    }

    status, reasons = validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
    )

    assert status == "fail"
    assert any("production_gate must be pass" in reason for reason in reasons)


def test_release_manifest_binding_must_match_candidate_lock(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    lock_path = tmp_path / "candidate-lock.json"
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    lock["release_binding"] = {
        "release_id": "different-release",
        "nonce_sha256": "9" * 64,
    }
    atomic_write_json(lock_path, lock)
    contract = {
        name: (filename, PRODUCTION_EVIDENCE_PRODUCERS[filename])
        for name, (filename, production) in REPORTS.items()
        if production
    }
    production_reports = {
        name: filename for name, (filename, production) in REPORTS.items() if production
    }

    with pytest.raises(ValueError, match="candidate lock does not match registry binding"):
        build_manifest(
            tmp_path,
            reports=production_reports,
            release_id="test-release-bundle",
            release_nonce="r" * 32,
            image_digest="sha256:" + "a" * 64,
        )

    status, reasons = validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
    )

    assert status == "fail"
    assert any("candidate lock is invalid" in reason for reason in reasons)


def test_release_manifest_rejects_registry_binding_changed_after_lock(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    binding_path = tmp_path / "registry-image-binding.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding["repository"] = "registry.example/tampered/trpc-agent-service"
    atomic_write_json(binding_path, binding)
    contract = {
        name: (filename, PRODUCTION_EVIDENCE_PRODUCERS[filename])
        for name, (filename, production) in REPORTS.items()
        if production
    }
    production_reports = {
        name: filename for name, (filename, production) in REPORTS.items() if production
    }

    with pytest.raises(ValueError, match="candidate lock does not match registry binding"):
        build_manifest(
            tmp_path,
            reports=production_reports,
            release_id="test-release-bundle",
            release_nonce="r" * 32,
            image_digest="sha256:" + "a" * 64,
        )

    status, reasons = validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
    )

    assert status == "fail"
    assert any("candidate lock is invalid" in reason for reason in reasons)


def test_missing_development_evidence_blocks_overall_gate(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    (tmp_path / REPORTS["migration_full_acceptance"][0]).unlink()
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["migration_full_acceptance"] == "not_run"
    assert result["development_gate"] == "not_run"
    assert result["runtime_production_gate"] == "pass"
    assert result["gate"] == "not_run"


def test_failed_development_evidence_does_not_change_production_gate(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, failed="im_resilience_contract", trust=trust)
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["release_gate.py", "--directory", str(tmp_path), "--output", str(output)],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["im_resilience_contract"] == "fail"
    assert result["development_gate"] == "fail"
    assert result["runtime_production_gate"] == "pass"
    assert result["gate"] == "fail"


def test_all_required_evidence_passes_overall_gate(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--require-production",
        ],
    )

    assert main() == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["development_gate"] == "pass"
    assert result["runtime_production_gate"] == "pass"
    assert result["gate"] == "pass"


def test_release_manifest_is_required_for_production_promotion(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    (tmp_path / "release-manifest.json").unlink()
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["release_bundle"] == "not_run"
    assert result["runtime_production_gate"] == "not_run"


def test_sdk_postgres_worker_attachment_is_auxiliary_only(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    (tmp_path / "postgres-worker-gate.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "check": "postgres_worker_role_gate",
                "status": "passed",
                "gate_decision": "pass",
            }
        ),
        encoding="utf-8",
    )
    production_reports = {
        name: filename for name, (filename, production) in REPORTS.items() if production
    }
    manifest = build_manifest(
        tmp_path,
        reports=production_reports,
        release_id="test-release-bundle",
        release_nonce="r" * 32,
        image_digest="sha256:" + "a" * 64,
    )

    attachment = manifest["auxiliary_reports"]["sdk_postgres_worker"]
    assert attachment["lineage_status"] == "unbound_not_substitute"
    assert attachment["substitutes_service_runtime"] is False
    atomic_write_json(tmp_path / "release-manifest.json", manifest)
    contract = {
        name: (filename, PRODUCTION_EVIDENCE_PRODUCERS[filename])
        for name, (filename, production) in REPORTS.items()
        if production and filename in PRODUCTION_EVIDENCE_PRODUCERS
    }
    assert validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
    ) == ("pass", [])


def test_release_manifest_rejects_runtime_without_database_role_evidence(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    runtime_path = tmp_path / REPORTS["real_runtime"][0]
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    runtime["candidate"].pop("database_role_evidence")
    runtime_path.write_text(json.dumps(runtime), encoding="utf-8")

    production_reports = {
        name: filename for name, (filename, production) in REPORTS.items() if production
    }
    with pytest.raises(ValueError, match="database role evidence"):
        build_manifest(
            tmp_path,
            reports=production_reports,
            release_id="test-release-bundle",
            release_nonce="r" * 32,
            image_digest="sha256:" + "a" * 64,
        )


def test_release_gate_rejects_runtime_without_database_role_evidence(tmp_path) -> None:
    report_value = _valid_real_runtime_report()
    report_value["candidate"].pop("database_role_evidence")
    report = tmp_path / REPORTS["real_runtime"][0]
    report.write_text(json.dumps(report_value), encoding="utf-8")

    assert _status(report, production_field=True) == (
        "not_run",
        ["real runtime evidence is missing or invalid candidate.database_role_evidence is missing"],
    )


def test_release_manifest_detects_report_splicing_after_bundle(tmp_path, monkeypatch) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    report_path = tmp_path / REPORTS["performance"][0]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidate"]["run_id"] = "spliced-after-manifest"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    output = tmp_path / "release-gate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "release_gate.py",
            "--directory",
            str(tmp_path),
            "--output",
            str(output),
            "--require-production",
        ],
    )

    assert main() == 1
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["candidate"]["release_bundle"] == "fail"
    assert any("content hash mismatch" in reason for reason in result["rejection_reasons"])


def test_release_manifest_rejects_report_with_mixed_source_fingerprint(
    tmp_path, monkeypatch
) -> None:
    trust = _install_release_probe_trust(tmp_path, monkeypatch)
    _write_complete_report_set(tmp_path, trust=trust)
    report_path = tmp_path / REPORTS["performance"][0]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["evidence"]["source_fingerprint"]["value"] = "0" * 64
    report_path.write_text(json.dumps(report), encoding="utf-8")

    contract = {
        name: (filename, PRODUCTION_EVIDENCE_PRODUCERS[filename])
        for name, (filename, production) in REPORTS.items()
        if production and filename in PRODUCTION_EVIDENCE_PRODUCERS
    }
    status, reasons = validate_manifest(
        tmp_path,
        reports=contract,
        current_source=_current_candidate_source_fingerprint(),
    )

    assert status == "fail"
    assert any(
        "real-performance.json belongs to a different source candidate" in reason
        for reason in reasons
    )
