import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts import candidate_lock
from scripts import functional_disaster_recovery_gate as gate


def _digest(letter: str) -> str:
    return f"sha256:{letter * 64}"


def _binding_and_lock(tmp_path: Path, monkeypatch):
    source = {"status": "available", "value": "d" * 64}
    release = {"release_id": "release-functional", "nonce_sha256": "e" * 64}
    monkeypatch.setattr(candidate_lock, "source_fingerprint", lambda _root: source)
    monkeypatch.setattr(candidate_lock, "current_release_binding", lambda **_kwargs: release)
    repository = "registry.example/acme/service"
    binding = {
        "kind": "registry_candidate_binding",
        "release_binding": release,
        "source_fingerprint": source,
        "repository": repository,
        "image_digest": _digest("a"),
        "images": {
            "initial": {"digest": _digest("a"), "reference": f"{repository}@{_digest('a')}"},
            "upgrade": {"digest": _digest("b"), "reference": f"{repository}@{_digest('b')}"},
        },
    }
    return binding, candidate_lock.create_candidate_lock(binding, root=tmp_path)


def _observations(binding, lock):
    completed = datetime.now(UTC).replace(microsecond=0)
    started = completed - timedelta(seconds=20)
    created = started - timedelta(seconds=1)
    common = {
        "schema_version": 1,
        "kind": "disaster_recovery_observation",
        "status": "pass",
        "mode": gate.MODE,
        "generated_at": completed.isoformat(),
        "release_binding": binding["release_binding"],
        "source_fingerprint": binding["source_fingerprint"],
        "image_digest": binding["image_digest"],
        "drill_id": "drf-one",
        "tenant_id_hash": "f" * 64,
        "canary_sha256": "1" * 64,
        "restored_canary_sha256": "1" * 64,
        "isolated_restore_target": True,
        "production_system_mutated": False,
        "rpo_seconds": 1,
        "rto_seconds": 20,
        "point_in_time_recovery": False,
        "backup_integrity_verified": False,
        "versioned_restore": False,
        "checksum_verified": False,
        "key_version_restored": False,
        "decrypt_verified": False,
        "execution": {
            "kind": "kubernetes_job",
            "source": "kubectl_api",
            "status": "succeeded",
            "completion_confirmed": True,
            "isolated_namespace": True,
            "production_mutation_checked": True,
            "succeeded": 1,
            "failed": 0,
            "active": 0,
            "context": "ack-functional",
            "namespace": "trpc-dr-functional-1234abcd",
            "cluster_uid_sha256": "2" * 64,
            "namespace_uid_sha256": "3" * 64,
            "pod_uid_sha256": "4" * 64,
            "image_reference": binding["images"]["initial"]["reference"],
            "image_id": f"docker-pullable://service@{binding['image_digest']}",
            "candidate_lock_binding_sha256": lock["binding_sha256"],
            "started_at": started.isoformat(),
            "completed_at": completed.isoformat(),
        },
        "backup": {
            "backend": "postgresql",
            "restore_mode": "logical_snapshot",
            "storage_tier": "ephemeral_same_cluster",
            "disaster_redundant": False,
            "replication_verified": False,
            "pitr_enabled": False,
            "versioning_enabled": False,
            "key_versioned": False,
            "backup_id_sha256": "5" * 64,
            "restore_id_sha256": "5" * 64,
            "created_at": created.isoformat(),
            "restore_started_at": started.isoformat(),
            "restore_completed_at": completed.isoformat(),
        },
        "validation": {
            "source": "restore_job_output",
            "status": "pass",
            "production_data_touched": False,
            "synthetic_data_only": True,
        },
    }
    return {
        "postgres_pitr": {
            **common,
            "component": "postgres_pitr",
            "run_id": "postgres-run",
            "backup_integrity_verified": True,
            "execution": {
                **common["execution"],
                "job_name": "postgres-pitr",
                "job_uid_sha256": "7" * 64,
            },
        },
        "artifact_restore": {
            **common,
            "component": "artifact_restore",
            "run_id": "artifact-run",
            "versioned_restore": True,
            "checksum_verified": True,
            "execution": {
                **common["execution"],
                "job_name": "artifact-restore",
                "job_uid_sha256": "8" * 64,
            },
            "backup": {
                **common["backup"],
                "backend": "minio",
                "restore_mode": "object_version",
                "versioning_enabled": True,
            },
        },
        "key_restore": {
            **common,
            "component": "key_restore",
            "run_id": "key-run",
            "key_version_restored": True,
            "decrypt_verified": True,
            "execution": {
                **common["execution"],
                "job_name": "key-restore",
                "job_uid_sha256": "9" * 64,
            },
            "backup": {
                **common["backup"],
                "backend": "minio",
                "restore_mode": "synthetic_key_version",
                "versioning_enabled": True,
                "key_versioned": True,
            },
        },
    }


def test_functional_gate_passes_real_jobs_but_never_production(tmp_path: Path, monkeypatch) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    directory = tmp_path / "evidence"
    directory.mkdir()
    for component, observation in observations.items():
        (directory / f"{component}.json").write_text(json.dumps(observation), encoding="utf-8")
    lock_path = tmp_path / "lock.json"
    binding_path = tmp_path / "binding.json"
    lock_path.write_text(json.dumps(lock), encoding="utf-8")
    binding_path.write_text(json.dumps(binding), encoding="utf-8")

    report = gate.build_report(
        enabled=True,
        evidence_paths={
            component: directory / f"{component}.json" for component in gate.COMPONENTS
        },
        lock_path=lock_path,
        binding_path=binding_path,
        max_rto_seconds=300,
    )

    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["evidence"]["runtime_fingerprint"]["status"] == "available"
    assert report["evidence"]["runtime_fingerprint"]["worker_count"] == len(gate.COMPONENTS)
    assert report["candidate"]["runtime_attestation"] == {
        "cluster_uid_sha256": "2" * 64,
        "namespace_uid_sha256": "3" * 64,
        "jobs": [
            {
                "component": component,
                "image_digest": binding["image_digest"],
                "job_uid_sha256": {
                    "postgres_pitr": "7",
                    "artifact_restore": "8",
                    "key_restore": "9",
                }[component]
                * 64,
                "pod_uid_sha256": "4" * 64,
            }
            for component in gate.COMPONENTS
        ],
    }
    assert "not WAL point-in-time recovery" in " ".join(report["production_rejection_reasons"])


def test_functional_gate_rejects_redundancy_or_pitr_claims(tmp_path: Path, monkeypatch) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["postgres_pitr"]["point_in_time_recovery"] = True
    observations["artifact_restore"]["backup"]["disaster_redundant"] = True

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )

    assert "postgres_pitr: functional evidence incorrectly claims WAL PITR" in reasons
    assert "artifact_restore: functional evidence incorrectly claims redundancy" in reasons


def test_functional_gate_requires_postgres_identity_and_synthetic_validation(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["postgres_pitr"]["backup"]["restore_id_sha256"] = "6" * 64
    observations["artifact_restore"]["validation"]["synthetic_data_only"] = False

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )

    assert "postgres_pitr: logical restore checksum identity changed" in reasons
    assert "artifact_restore: validation did not prove synthetic-only data" in reasons


def test_functional_gate_rejects_wrong_mode_and_non_exact_runtime_image(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)

    observations["key_restore"]["mode"] = "isolated_restore_drill"
    observations["key_restore"]["execution"]["image_id"] += "-forged"

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )

    assert "key_restore: functional restore mode is missing or invalid" in reasons
    assert "key_restore: observed restore Job image ID is not an exact candidate digest" in reasons


def test_functional_gate_rejects_inconsistent_restore_timestamps(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    item = observations["postgres_pitr"]
    generated = datetime.now(UTC).replace(microsecond=0)
    item["generated_at"] = generated.isoformat()
    item["backup"]["restore_started_at"] = (generated - timedelta(seconds=20)).isoformat()
    item["backup"]["restore_completed_at"] = (generated - timedelta(seconds=22)).isoformat()
    item["backup"]["created_at"] = (generated - timedelta(seconds=10)).isoformat()
    item["execution"]["completed_at"] = (generated - timedelta(seconds=23)).isoformat()

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )

    assert "postgres_pitr: restore started before the backup was created" in reasons
    assert "postgres_pitr: Job completed before restore started" in reasons
    assert "postgres_pitr: restore completed before it started" in reasons


def test_functional_gate_allows_job_completion_after_measured_restore(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    item = observations["key_restore"]
    restore_started = datetime.now(UTC).replace(microsecond=100_000) - timedelta(seconds=10)
    restore_completed = restore_started + timedelta(milliseconds=3)
    item["rto_seconds"] = 0.003
    item["backup"]["created_at"] = (restore_started - timedelta(milliseconds=50)).isoformat()
    item["backup"]["restore_started_at"] = restore_started.isoformat()
    item["backup"]["restore_completed_at"] = restore_completed.isoformat()
    item["execution"]["completed_at"] = (restore_completed + timedelta(seconds=7)).isoformat()
    item["generated_at"] = item["execution"]["completed_at"]

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )

    assert not any(reason.startswith("key_restore:") for reason in reasons)


def test_functional_gate_rejects_forged_measured_rto(tmp_path: Path, monkeypatch) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["artifact_restore"]["rto_seconds"] = 1

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )

    assert "artifact_restore: RTO does not match restore timestamps" in reasons


def test_functional_gate_is_fail_closed_for_unhashable_identity_and_loader_errors(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["artifact_restore"]["drill_id"] = ["not-a-string"]
    observations["artifact_restore"]["tenant_id_hash"] = {"not": "a-hash"}

    reasons = gate.validate_functional_drill(
        observations, lock=lock, binding=binding, max_rto_seconds=300
    )
    assert "artifact_restore: drill_id is missing or invalid" in reasons
    assert "artifact_restore: tenant_id_hash is missing or invalid" in reasons

    lock_path = tmp_path / "lock.json"
    binding_path = tmp_path / "binding.json"
    lock_path.write_text('{"password":"do-not-echo"}', encoding="utf-8")
    binding_path.write_text("{}", encoding="utf-8")
    report = gate.build_report(
        enabled=True,
        evidence_paths={component: tmp_path / f"{component}.json" for component in gate.COMPONENTS},
        lock_path=lock_path,
        binding_path=binding_path,
        max_rto_seconds=300,
    )
    serialized = json.dumps(report)
    assert report["gate"] == "fail"
    assert report["production_gate"] == "not_run"
    assert "do-not-echo" not in serialized


def test_functional_gate_is_inert_without_explicit_opt_in(tmp_path: Path) -> None:
    report = gate.build_report(
        enabled=False,
        evidence_paths={},
        lock_path=tmp_path / "lock.json",
        binding_path=tmp_path / "binding.json",
        max_rto_seconds=300,
    )
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
