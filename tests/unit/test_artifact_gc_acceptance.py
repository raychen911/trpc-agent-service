import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

import scripts.artifact_gc_acceptance as gate


def _args(tmp_path: Path, *, namespace: str = "") -> argparse.Namespace:
    return argparse.Namespace(
        execute=False,
        namespace=namespace,
        context="ack-acceptance" if namespace else "",
        kubeconfig=tmp_path / "kubeconfig",
        candidate_lock=tmp_path / "candidate-lock.json",
        ttl_seconds=60,
        timeout_seconds=2.0,
        poll_seconds=0.01,
        output=tmp_path / "artifact-gc.json",
        require_production=False,
    )


def _lock() -> dict[str, Any]:
    image = "sha256:" + "c" * 64
    return {
        "image_digest": image,
        "source_fingerprint": {"status": "available", "value": "b" * 64},
        "images": {
            "initial": {
                "digest": image,
                "reference": "registry.example/trpc-agent-service@" + image,
            }
        },
    }


def _pod(*, image: str | None = None, source: str = "b" * 64) -> dict[str, Any]:
    return {
        "metadata": {
            "name": "trpc-artifact-gc-abc",
            "uid": "pod-uid",
            "labels": {
                "app.kubernetes.io/name": "trpc-artifact-gc",
                "io.trpc.agent-service.source-fingerprint": source,
            },
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "artifact-gc",
                    "imageID": "docker-pullable://registry/trpc@" + (image or "sha256:" + "c" * 64),
                }
            ],
        },
    }


def test_default_is_inert_and_reports_not_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(gate.OPT_IN_ENV, raising=False)
    output = tmp_path / "artifact-gc.json"

    def no_pool(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("PostgreSQL must not be contacted")

    monkeypatch.setattr("scripts.artifact_gc_acceptance.asyncpg.create_pool", no_pool)
    monkeypatch.setattr("scripts.artifact_gc_acceptance.boto3.client", no_pool)

    assert gate.main(["--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["executor"] == "none"
    assert {"baseline", "candidate", "case_deltas", "evidence"} <= set(report)


def test_successful_pod_identity_is_promotable_only_with_kubernetes_scope() -> None:
    lock = _lock()
    image, source, reasons = gate._lock_identity(lock)
    assert reasons == []
    observation, pod_reasons = gate.validate_gc_pod(
        _pod(image=image), expected_image=image, expected_source=source
    )
    assert pod_reasons == []
    assert observation["status"] == "pass"
    assert gate._production_status(
        gate="pass",
        namespace="trpc-acceptance",
        identity_available=True,
        identity_rejected=False,
    ) == ("pass", [])
    assert (
        gate._production_status(
            gate="pass",
            namespace=None,
            identity_available=True,
            identity_rejected=False,
        )[0]
        == "not_run"
    )


def test_pod_identity_mismatch_is_rejected() -> None:
    image, source, _ = gate._lock_identity(_lock())
    observation, reasons = gate.validate_gc_pod(
        _pod(image="sha256:" + "d" * 64, source="e" * 64),
        expected_image=image,
        expected_source=source,
    )
    assert observation["status"] == "fail"
    assert any("image digest" in reason for reason in reasons)
    assert any("source fingerprint" in reason for reason in reasons)
    assert (
        gate._production_status(
            gate="pass",
            namespace="trpc-acceptance",
            identity_available=True,
            identity_rejected=True,
        )[0]
        == "fail"
    )


class _Connection:
    def __init__(self, status: str = "staged") -> None:
        self.status = status
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, _sql: str, *_args: object) -> dict[str, str]:
        return {"status": self.status}

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((sql, args))
        return "DELETE 1"


class _Acquire:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _Connection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)


class _Objects:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, str]] = []

    def delete_object(self, **kwargs: str) -> None:
        self.deleted.append((kwargs["Bucket"], kwargs["Key"]))


@pytest.mark.asyncio
async def test_fixture_cleanup_deletes_only_object_and_rows() -> None:
    connection = _Connection()
    pool = _Pool(connection)
    objects = _Objects()

    reasons = await gate._cleanup_fixture(
        pool,
        objects,
        bucket="trpc-artifacts",
        tenant_id="tenant-unique",
        artifact_id="artifact-unique",
        object_key="tenants/safe/staging/object",
    )

    assert reasons == []
    assert objects.deleted == [("trpc-artifacts", "tenants/safe/staging/object")]
    assert [args for _sql, args in connection.executed] == [
        ("tenant-unique", "artifact-unique"),
        ("tenant-unique",),
    ]


@pytest.mark.asyncio
async def test_wait_reports_timeout_without_claiming_deletion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(status="staged")
    pool = _Pool(connection)
    objects = _Objects()
    times = iter((0.0, 2.0))
    monkeypatch.setattr("scripts.artifact_gc_acceptance.time.monotonic", lambda: next(times, 2.0))

    async def _false_async() -> bool:
        return True

    monkeypatch.setattr(gate, "_s3_exists", lambda *_args, **_kwargs: _false_async())

    result, _pod_observation, reasons = await gate._wait_for_deletion(
        pool,
        objects,
        bucket="trpc-artifacts",
        tenant_id="tenant",
        artifact_id="artifact",
        object_key="key",
        timeout_seconds=1,
        poll_seconds=0.01,
        namespace=None,
        kubeconfig=None,
        context=None,
        expected_image=None,
        expected_source=None,
    )
    assert result["status"] == "fail"
    assert any("timed out" in reason for reason in reasons)


def test_kubectl_failure_does_not_expose_command_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("scripts.artifact_gc_acceptance.shutil.which", lambda _name: "kubectl")

    def failed_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["kubectl"], 1, "", "password=do-not-leak")

    monkeypatch.setattr("scripts.artifact_gc_acceptance.subprocess.run", failed_run)
    with pytest.raises(gate.AcceptanceUnavailable) as error:
        gate._kubectl(
            ["get", "pods"],
            kubeconfig=tmp_path / "config",
            context="ack-acceptance",
            timeout_seconds=1,
        )
    assert "password" not in str(error.value)
