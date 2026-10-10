import base64
import json
from dataclasses import replace

import pytest

from scripts import dr_functional_job as job


def _settings(component: str) -> job.Settings:
    namespace = "trpc-dr-functional-1234abcd"
    return job.Settings(
        component=component,
        drill_id="drf-one",
        tenant_id="synthetic-tenant",
        canary="synthetic-canary",
        namespace=namespace,
        postgres_dsn=(
            f"postgresql://postgres:test@postgres.{namespace}.svc.cluster.local:5432/postgres"
        ),
        s3_endpoint=f"http://minio.{namespace}.svc.cluster.local:9000",
        s3_bucket="trpc-dr-functional",
        s3_access_key="synthetic-user",
        s3_secret_key="synthetic-password",
        wrapping_key_b64=base64.b64encode(b"w" * 32).decode("ascii"),
    )


class _Body:
    def __init__(self, value: bytes) -> None:
        self.value = value

    def read(self) -> bytes:
        return self.value


class _S3:
    def __init__(self) -> None:
        self.versioning = False
        self.objects: dict[str, list[tuple[str, bytes]]] = {}

    def create_bucket(self, **_kwargs):
        return {}

    def put_bucket_versioning(self, **_kwargs):
        self.versioning = True
        return {}

    def get_bucket_versioning(self, **_kwargs):
        return {"Status": "Enabled" if self.versioning else "Suspended"}

    def put_object(self, *, Key, Body, **_kwargs):
        payload = Body if isinstance(Body, bytes) else Body.encode()
        version = f"version-{sum(len(items) for items in self.objects.values()) + 1}"
        self.objects.setdefault(Key, []).append((version, payload))
        return {"VersionId": version}

    def get_object(self, *, Key, VersionId=None, **_kwargs):
        versions = self.objects[Key]
        payload = (
            next(value for version, value in versions if version == VersionId)
            if VersionId
            else versions[-1][1]
        )
        return {"Body": _Body(payload)}


class _Cursor:
    def __init__(self) -> None:
        self.fetch_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, *_args, **_kwargs):
        return None

    def executemany(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        self.fetch_count += 1
        return [(1, "synthetic-canary"), (2, "marker:drf-one")]


class _Connection:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self):
        return self.cursor_value

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_settings_reject_non_isolated_targets() -> None:
    settings = _settings("postgres_pitr")
    settings.validate_isolation()
    with pytest.raises(ValueError, match="not the isolated"):
        replace(
            settings, postgres_dsn="postgresql://postgres:test@production:5432/db"
        ).validate_isolation()


def test_postgres_job_performs_logical_backup_loss_and_restore(monkeypatch) -> None:
    connection = _Connection()
    monkeypatch.setattr(job, "_postgres_connect", lambda *_args, **_kwargs: connection)

    result = job.run_component(_settings("postgres_pitr"))

    assert result["status"] == "pass"
    assert result["backup_integrity_verified"] is True
    assert result["point_in_time_recovery"] is False
    assert result["backup"]["restore_mode"] == "logical_snapshot"
    assert result["backup"]["disaster_redundant"] is False
    assert result["backup"]["restore_completed_at"] >= result["backup"]["restore_started_at"]
    assert connection.closed is True
    assert connection.commits == 2
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    ("component", "expected_checks"),
    [
        ("artifact_restore", ("versioned_restore", "checksum_verified")),
        ("key_restore", ("key_version_restored", "decrypt_verified")),
    ],
)
def test_minio_jobs_restore_old_versions_and_verify_canary(
    component: str, expected_checks: tuple[str, str], monkeypatch
) -> None:
    client = _S3()
    monkeypatch.setattr(job, "_s3_client", lambda _settings: client)

    result = job.run_component(_settings(component))

    assert result["status"] == "pass"
    assert all(result[check] is True for check in expected_checks)
    assert result["restored_canary_sha256"] == result["canary_sha256"]
    assert result["backup"]["versioning_enabled"] is True
    assert result["backup"]["restore_completed_at"] >= result["backup"]["restore_started_at"]
    if component == "key_restore":
        key_object = next(value for key, value in client.objects.items() if "/keys/" in key)
        stored_document = json.loads(key_object[0][1])
        assert "key" not in stored_document
        assert "wrapped_key" in stored_document


def test_job_failure_output_is_one_line_and_secret_free(monkeypatch, capsys) -> None:
    monkeypatch.setenv("TRPC_DR_CANARY", "must-not-leak")
    monkeypatch.setattr(
        job.Settings,
        "from_environment",
        lambda _component: (_ for _ in ()).throw(ValueError()),
    )

    assert job.main(["--component", "key_restore"]) == 1
    output = capsys.readouterr().out
    assert len(output.splitlines()) == 1
    assert "must-not-leak" not in output
    assert json.loads(output)["failure_code"] == "functional_restore_failed"
