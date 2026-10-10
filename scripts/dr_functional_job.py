#!/usr/bin/env python3
"""Run one zero-cost disaster-recovery functional check inside Kubernetes.

The checks use only synthetic data in an isolated namespace.  They prove that
the restore code paths work, but deliberately do not claim PostgreSQL WAL PITR,
remote object durability, or KMS recovery.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from scripts.disaster_recovery_gate import COMPONENTS


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha256(value: bytes | str) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class Settings:
    component: str
    drill_id: str
    tenant_id: str
    canary: str
    namespace: str
    postgres_dsn: str
    s3_endpoint: str
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str
    wrapping_key_b64: str

    @classmethod
    def from_environment(cls, component: str) -> Settings:
        values = {
            "drill_id": os.getenv("TRPC_DRILL_ID", ""),
            "tenant_id": os.getenv("TRPC_DR_TENANT_ID", ""),
            "canary": os.getenv("TRPC_DR_CANARY", ""),
            "namespace": os.getenv("TRPC_DR_NAMESPACE", ""),
            "postgres_dsn": os.getenv("TRPC_DR_POSTGRES_DSN", ""),
            "s3_endpoint": os.getenv("TRPC_DR_S3_ENDPOINT", ""),
            "s3_bucket": os.getenv("TRPC_DR_S3_BUCKET", ""),
            "s3_access_key": os.getenv("TRPC_DR_S3_ACCESS_KEY", ""),
            "s3_secret_key": os.getenv("TRPC_DR_S3_SECRET_KEY", ""),
            "wrapping_key_b64": os.getenv("TRPC_DR_TEST_WRAPPING_KEY", ""),
        }
        if any(not value for value in values.values()):
            raise ValueError("functional restore environment is incomplete")
        settings = cls(component=component, **values)
        settings.validate_isolation()
        return settings

    def validate_isolation(self) -> None:
        expected_postgres = f"postgres.{self.namespace}.svc.cluster.local"
        expected_minio = f"minio.{self.namespace}.svc.cluster.local"
        if urlsplit(self.postgres_dsn).hostname != expected_postgres:
            raise ValueError("PostgreSQL target is not the isolated namespace service")
        if urlsplit(self.s3_endpoint).hostname != expected_minio:
            raise ValueError("object-store target is not the isolated namespace service")
        try:
            wrapping_key = base64.b64decode(self.wrapping_key_b64, validate=True)
        except ValueError as error:
            raise ValueError("synthetic wrapping key is invalid") from error
        if len(wrapping_key) != 32:
            raise ValueError("synthetic wrapping key must contain 32 bytes")


@dataclass(frozen=True)
class RestoreOutcome:
    created_at: datetime
    restore_started_at: datetime
    completed_at: datetime
    backup_id_sha256: str
    restore_id_sha256: str
    restored_canary_sha256: str
    checks: Mapping[str, bool]
    backup: Mapping[str, Any]


def _postgres_connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, connect_timeout=10)


def _postgres_restore(settings: Settings) -> RestoreOutcome:
    rows = [(1, settings.canary), (2, f"marker:{settings.drill_id}")]
    created_at = _utc_now()
    backup_payload = _canonical(rows)
    restore_started_at = created_at
    restored_payload = b""
    connection = _postgres_connect(settings.postgres_dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS dr_functional CASCADE")
            cursor.execute("CREATE SCHEMA dr_functional")
            cursor.execute(
                "CREATE TABLE dr_functional.source "
                "(position integer PRIMARY KEY, payload text NOT NULL)"
            )
            cursor.executemany(
                "INSERT INTO dr_functional.source (position, payload) VALUES (%s, %s)",
                rows,
            )
            cursor.execute("SELECT position, payload FROM dr_functional.source ORDER BY position")
            captured = cursor.fetchall()
            backup_payload = _canonical(captured)
            cursor.execute("DROP TABLE dr_functional.source")
            restore_started_at = _utc_now()
            cursor.execute(
                "CREATE TABLE dr_functional.restored "
                "(position integer PRIMARY KEY, payload text NOT NULL)"
            )
            restored_rows = json.loads(backup_payload)
            cursor.executemany(
                "INSERT INTO dr_functional.restored (position, payload) VALUES (%s, %s)",
                restored_rows,
            )
            cursor.execute("SELECT position, payload FROM dr_functional.restored ORDER BY position")
            restored_payload = _canonical(cursor.fetchall())
        connection.commit()
    finally:
        try:
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS dr_functional CASCADE")
            connection.commit()
        finally:
            connection.close()
    completed_at = _utc_now()
    if backup_payload != restored_payload:
        raise RuntimeError("logical PostgreSQL restore checksum mismatch")
    return RestoreOutcome(
        created_at=created_at,
        restore_started_at=restore_started_at,
        completed_at=completed_at,
        backup_id_sha256=_sha256(backup_payload),
        restore_id_sha256=_sha256(restored_payload),
        restored_canary_sha256=_sha256(settings.canary),
        checks={"point_in_time_recovery": False, "backup_integrity_verified": True},
        backup={
            "backend": "postgresql",
            "restore_mode": "logical_snapshot",
            "pitr_enabled": False,
            "versioning_enabled": False,
            "key_versioned": False,
        },
    )


def _s3_client(settings: Settings) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
    )


def _ensure_versioned_bucket(client: BaseClient, bucket: str) -> None:
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
            raise
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    status = client.get_bucket_versioning(Bucket=bucket).get("Status")
    if status != "Enabled":
        raise RuntimeError("functional object-store versioning is not enabled")


def _object_version_restore(settings: Settings) -> RestoreOutcome:
    client = _s3_client(settings)
    bucket = f"{settings.s3_bucket}-artifact"
    _ensure_versioned_bucket(client, bucket)
    key = f"{settings.drill_id}/artifact/canary.bin"
    restored_key = f"{settings.drill_id}/artifact/restored.bin"
    canary = settings.canary.encode("utf-8")
    created_at = _utc_now()
    version = client.put_object(Bucket=bucket, Key=key, Body=canary).get("VersionId")
    if not isinstance(version, str) or not version:
        raise RuntimeError("functional artifact version ID is missing")
    client.put_object(Bucket=bucket, Key=key, Body=b"newer-version")
    restore_started_at = _utc_now()
    restored = client.get_object(Bucket=bucket, Key=key, VersionId=version)["Body"].read()
    client.put_object(Bucket=bucket, Key=restored_key, Body=restored)
    persisted = client.get_object(Bucket=bucket, Key=restored_key)["Body"].read()
    completed_at = _utc_now()
    if persisted != canary:
        raise RuntimeError("functional artifact restore checksum mismatch")
    return RestoreOutcome(
        created_at=created_at,
        restore_started_at=restore_started_at,
        completed_at=completed_at,
        backup_id_sha256=_sha256(version),
        restore_id_sha256=_sha256(restored_key),
        restored_canary_sha256=_sha256(persisted),
        checks={"versioned_restore": True, "checksum_verified": True},
        backup={
            "backend": "minio",
            "restore_mode": "object_version",
            "pitr_enabled": False,
            "versioning_enabled": True,
            "key_versioned": False,
            "source_version_id_sha256": _sha256(version),
        },
    )


def _key_version_restore(settings: Settings) -> RestoreOutcome:
    client = _s3_client(settings)
    bucket = f"{settings.s3_bucket}-key"
    _ensure_versioned_bucket(client, bucket)
    key = f"{settings.drill_id}/keys/test-key.json"
    plaintext = settings.canary.encode("utf-8")
    wrapping_key = base64.b64decode(settings.wrapping_key_b64, validate=True)
    encryption_key = AESGCM.generate_key(bit_length=256)
    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(encryption_key).encrypt(nonce, plaintext, settings.drill_id.encode())
    wrap_nonce = secrets.token_bytes(12)
    wrapped_key = AESGCM(wrapping_key).encrypt(
        wrap_nonce, encryption_key, settings.drill_id.encode()
    )
    version_one = _canonical(
        {
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "wrap_nonce": base64.b64encode(wrap_nonce).decode("ascii"),
            "wrapped_key": base64.b64encode(wrapped_key).decode("ascii"),
        }
    )
    created_at = _utc_now()
    version = client.put_object(Bucket=bucket, Key=key, Body=version_one).get("VersionId")
    if not isinstance(version, str) or not version:
        raise RuntimeError("functional key version ID is missing")
    replacement_key = AESGCM.generate_key(bit_length=256)
    replacement_wrap_nonce = secrets.token_bytes(12)
    replacement = _canonical(
        {
            "ciphertext": base64.b64encode(secrets.token_bytes(48)).decode("ascii"),
            "nonce": base64.b64encode(secrets.token_bytes(12)).decode("ascii"),
            "wrap_nonce": base64.b64encode(replacement_wrap_nonce).decode("ascii"),
            "wrapped_key": base64.b64encode(
                AESGCM(wrapping_key).encrypt(
                    replacement_wrap_nonce,
                    replacement_key,
                    settings.drill_id.encode(),
                )
            ).decode("ascii"),
        }
    )
    client.put_object(Bucket=bucket, Key=key, Body=replacement)
    restore_started_at = _utc_now()
    restored_document = client.get_object(Bucket=bucket, Key=key, VersionId=version)["Body"].read()
    restored = json.loads(restored_document)
    restored_key = AESGCM(wrapping_key).decrypt(
        base64.b64decode(restored["wrap_nonce"]),
        base64.b64decode(restored["wrapped_key"]),
        settings.drill_id.encode(),
    )
    decrypted = AESGCM(restored_key).decrypt(
        base64.b64decode(restored["nonce"]),
        base64.b64decode(restored["ciphertext"]),
        settings.drill_id.encode(),
    )
    completed_at = _utc_now()
    if decrypted != plaintext:
        raise RuntimeError("functional key restore decryption mismatch")
    return RestoreOutcome(
        created_at=created_at,
        restore_started_at=restore_started_at,
        completed_at=completed_at,
        backup_id_sha256=_sha256(version_one),
        restore_id_sha256=_sha256(restored_document),
        restored_canary_sha256=_sha256(decrypted),
        checks={"key_version_restored": True, "decrypt_verified": True},
        backup={
            "backend": "minio",
            "restore_mode": "synthetic_key_version",
            "pitr_enabled": False,
            "versioning_enabled": True,
            "key_versioned": True,
            "source_version_id_sha256": _sha256(version),
        },
    )


def run_component(settings: Settings) -> dict[str, Any]:
    runners = {
        "postgres_pitr": _postgres_restore,
        "artifact_restore": _object_version_restore,
        "key_restore": _key_version_restore,
    }
    outcome = runners[settings.component](settings)
    canary_sha256 = _sha256(settings.canary)
    if outcome.restored_canary_sha256 != canary_sha256:
        raise RuntimeError("functional restore canary mismatch")
    restore_elapsed = max((outcome.completed_at - outcome.restore_started_at).total_seconds(), 0.0)
    backup = {
        **outcome.backup,
        "storage_tier": "ephemeral_same_cluster",
        "disaster_redundant": False,
        "replication_verified": False,
        "backup_id_sha256": outcome.backup_id_sha256,
        "restore_id_sha256": outcome.restore_id_sha256,
        "created_at": _utc(outcome.created_at),
        "restore_started_at": _utc(outcome.restore_started_at),
        "restore_completed_at": _utc(outcome.completed_at),
    }
    return {
        "schema_version": 1,
        "component": settings.component,
        "status": "pass",
        "mode": "same_cluster_zero_cost_functional",
        "drill_id": settings.drill_id,
        "run_id": f"{settings.component}-{_sha256(settings.drill_id + settings.component)[:20]}",
        "tenant_id_hash": _sha256(settings.tenant_id),
        "canary_sha256": canary_sha256,
        "restored_canary_sha256": outcome.restored_canary_sha256,
        "rpo_seconds": max((outcome.restore_started_at - outcome.created_at).total_seconds(), 0.0),
        "rto_seconds": restore_elapsed,
        "point_in_time_recovery": False,
        "backup_integrity_verified": False,
        "versioned_restore": False,
        "checksum_verified": False,
        "key_version_restored": False,
        "decrypt_verified": False,
        **outcome.checks,
        "backup": backup,
        "validation": {
            "source": "restore_job_output",
            "status": "pass",
            "production_data_touched": False,
            "synthetic_data_only": True,
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", choices=COMPONENTS, required=True)
    args = parser.parse_args(argv)
    try:
        result = run_component(Settings.from_environment(args.component))
    except Exception:
        result = {
            "schema_version": 1,
            "component": args.component,
            "status": "fail",
            "failure_code": "functional_restore_failed",
        }
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
