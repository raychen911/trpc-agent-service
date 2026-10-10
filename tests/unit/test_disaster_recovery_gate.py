from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts import candidate_lock, disaster_recovery_gate


def _digest(letter: str) -> str:
    return f"sha256:{letter * 64}"


def _binding_and_lock(tmp_path: Path, monkeypatch):
    source = {"status": "available", "value": "d" * 64}
    release = {"release_id": "release-dr", "nonce_sha256": "e" * 64}
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
            "initial": {
                "digest": _digest("a"),
                "reference": f"{repository}@{_digest('a')}",
            },
            "upgrade": {
                "digest": _digest("b"),
                "reference": f"{repository}@{_digest('b')}",
            },
        },
    }
    lock = candidate_lock.create_candidate_lock(binding, root=tmp_path)
    return binding, lock


def _observations(binding, lock):
    # Keep the fixture deterministic while allowing the gate's 24-hour freshness check.
    generated_at = datetime.now(UTC).replace(microsecond=0)
    restore_started = generated_at - timedelta(seconds=120)
    backup_created = generated_at - timedelta(seconds=150)
    common = {
        "schema_version": 1,
        "kind": "disaster_recovery_observation",
        "status": "pass",
        "generated_at": generated_at.isoformat(),
        "release_binding": binding["release_binding"],
        "source_fingerprint": binding["source_fingerprint"],
        "image_digest": binding["image_digest"],
        "drill_id": "drill-one",
        "tenant_id_hash": "f" * 64,
        "canary_sha256": "1" * 64,
        "restored_canary_sha256": "1" * 64,
        "isolated_restore_target": True,
        "production_system_mutated": False,
        "rpo_seconds": 30,
        "rto_seconds": 120,
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
            "context": "ack-acceptance",
            "namespace": "trpc-drill-one",
            "cluster_uid_sha256": "2" * 64,
            "namespace_uid_sha256": "3" * 64,
            "pod_uid_sha256": "4" * 64,
            "image_reference": binding["images"]["initial"]["reference"],
            "image_id": f"docker-pullable://{binding['repository']}@{binding['image_digest']}",
            "candidate_lock_binding_sha256": lock["binding_sha256"],
            "started_at": restore_started.isoformat(),
            "completed_at": generated_at.isoformat(),
        },
        "backup": {
            "backend": "postgresql",
            "storage_tier": "cross_region_redundant",
            "disaster_redundant": True,
            "replication_verified": True,
            "pitr_enabled": True,
            "versioning_enabled": True,
            "key_versioned": True,
            "backup_id_sha256": "5" * 64,
            "restore_id_sha256": "6" * 64,
            "created_at": backup_created.isoformat(),
            "restore_started_at": restore_started.isoformat(),
        },
        "validation": {
            "source": "restore_job_output",
            "status": "pass",
            "production_data_touched": False,
        },
    }
    return {
        "postgres_pitr": {
            **common,
            "component": "postgres_pitr",
            "run_id": "postgres-run",
            "execution": {
                **common["execution"],
                "job_name": "postgres-pitr",
                "job_uid_sha256": "9" * 64,
            },
            "point_in_time_recovery": True,
            "backup_integrity_verified": True,
        },
        "artifact_restore": {
            **common,
            "component": "artifact_restore",
            "run_id": "artifact-run",
            "execution": {
                **common["execution"],
                "job_name": "artifact-restore",
                "job_uid_sha256": "7" * 64,
            },
            "backup": {**common["backup"], "backend": "s3"},
            "versioned_restore": True,
            "checksum_verified": True,
        },
        "key_restore": {
            **common,
            "component": "key_restore",
            "run_id": "key-run",
            "execution": {
                **common["execution"],
                "job_name": "key-restore",
                "job_uid_sha256": "8" * 64,
            },
            "backup": {**common["backup"], "backend": "minio"},
            "key_version_restored": True,
            "decrypt_verified": True,
        },
    }


def test_drill_requires_three_real_restores_bound_to_one_candidate(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    reasons = disaster_recovery_gate.validate_drill(
        _observations(binding, lock),
        lock=lock,
        binding=binding,
        max_rpo_seconds=300,
        max_rto_seconds=3_600,
    )
    assert reasons == []


def test_drill_rejects_checksum_drift_and_missed_objective(tmp_path: Path, monkeypatch) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["artifact_restore"]["restored_canary_sha256"] = "2" * 64
    observations["key_restore"]["rto_seconds"] = 4_000

    reasons = disaster_recovery_gate.validate_drill(
        observations,
        lock=lock,
        binding=binding,
        max_rpo_seconds=300,
        max_rto_seconds=3_600,
    )

    assert "artifact_restore: restored canary checksum does not match" in reasons
    assert "key_restore: RTO exceeds the configured objective" in reasons


def test_drill_rejects_node_local_storage_and_offline_job_claims(
    tmp_path: Path, monkeypatch
) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["artifact_restore"]["backup"]["storage_tier"] = "node_local"
    observations["artifact_restore"]["backup"]["disaster_redundant"] = False
    observations["key_restore"]["execution"]["source"] = "offline_fixture"

    reasons = disaster_recovery_gate.validate_drill(
        observations,
        lock=lock,
        binding=binding,
        max_rpo_seconds=300,
        max_rto_seconds=3_600,
    )

    assert "artifact_restore: backup storage tier is not disaster-redundant" in reasons
    assert "key_restore: restore execution was not observed through kubectl API" in reasons


def test_drill_rejects_missing_job_timestamps_and_derived_rto(tmp_path: Path, monkeypatch) -> None:
    binding, lock = _binding_and_lock(tmp_path, monkeypatch)
    observations = _observations(binding, lock)
    observations["postgres_pitr"]["execution"]["completed_at"] = None
    observations["postgres_pitr"]["rto_seconds"] = 1

    reasons = disaster_recovery_gate.validate_drill(
        observations,
        lock=lock,
        binding=binding,
        max_rpo_seconds=300,
        max_rto_seconds=3_600,
    )

    assert (
        "postgres_pitr: Kubernetes Job start/completion timestamps are missing or stale" in reasons
    )


def test_drill_is_not_run_without_explicit_opt_in(tmp_path: Path) -> None:
    report = disaster_recovery_gate.build_report(
        enabled=False,
        evidence_paths={},
        lock_path=tmp_path / "lock.json",
        binding_path=tmp_path / "binding.json",
        max_rpo_seconds=300,
        max_rto_seconds=3_600,
    )
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
