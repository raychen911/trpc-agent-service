import json
from pathlib import Path

import pytest

from scripts import kubernetes_functional_disaster_recovery as runner


def _digest(letter: str) -> str:
    return f"sha256:{letter * 64}"


def test_manifests_use_ephemeral_support_and_three_immutable_candidate_jobs() -> None:
    namespace, support, jobs = runner.build_manifests(
        namespace="trpc-dr-functional-1234abcd",
        pull_secret="xuanyuan-pull",
        candidate_image=f"mirror.example/acme/service@{_digest('a')}",
        postgres_image=f"mirror.example/pgvector@{_digest('b')}",
        minio_image=f"mirror.example/minio@{_digest('c')}",
        drill_id="drf-one",
        postgres_password="synthetic-postgres-password",
        minio_user="synthetic-user",
        minio_password="synthetic-minio-password",
        tenant_id="synthetic-tenant",
        canary="synthetic-canary",
        wrapping_key_b64="c3ludGhldGljLXdyYXBwaW5nLWtleQ==",
    )

    rendered = json.dumps([namespace, support, jobs])
    assert "PersistentVolumeClaim" not in rendered
    assert "hostPath" not in rendered
    assert rendered.count('"emptyDir": {}') == 2
    job_items = jobs["items"]
    assert {item["metadata"]["name"] for item in job_items} == {
        "postgres-pitr",
        "artifact-restore",
        "key-restore",
    }
    assert all(
        item["spec"]["template"]["spec"]["containers"][0]["image"].endswith(_digest("a"))
        for item in job_items
    )
    assert all(
        item["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
        for item in job_items
    )
    assert all(
        item["spec"]["template"]["spec"]["containers"][0]["command"][:3]
        == ["python", "-m", "scripts.dr_functional_job"]
        for item in job_items
    )
    secret = support["items"][0]
    assert secret["stringData"]["wrapping-key"] == "c3ludGhldGljLXdyYXBwaW5nLWtleQ=="
    assert all(
        any(
            env.get("name") == "TRPC_DR_TEST_WRAPPING_KEY"
            and env.get("valueFrom", {}).get("secretKeyRef", {}).get("key") == "wrapping-key"
            for env in item["spec"]["template"]["spec"]["containers"][0]["env"]
        )
        for item in job_items
    )


def test_cleanup_refuses_any_namespace_outside_functional_prefix() -> None:
    with pytest.raises(ValueError, match="refusing"):
        runner._delete_namespace(
            namespace="trpc-service",
            namespace_uid="uid-one",
            kubeconfig=Path("config"),
            context="ack",
            timeout_seconds=30,
        )


def test_cleanup_refuses_namespace_uid_reuse_without_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deleted = False

    def fake_get(*_args, **_kwargs):
        return {"metadata": {"uid": "new-uid"}}

    def fake_delete(*_args, **_kwargs):
        nonlocal deleted
        deleted = True
        raise AssertionError("namespace with a changed UID must not be deleted")

    monkeypatch.setattr(runner, "_kubectl_json", fake_get)
    monkeypatch.setattr(runner, "_kubectl", fake_delete)

    with pytest.raises(RuntimeError, match="identity changed"):
        runner._delete_namespace(
            namespace="trpc-dr-functional-1234abcd",
            namespace_uid="old-uid",
            kubeconfig=Path("config"),
            context="ack",
            timeout_seconds=30,
        )
    assert deleted is False


def test_job_wait_fails_immediately_on_terminal_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        runner,
        "_kubectl_json",
        lambda *_args, **_kwargs: {
            "status": {
                "failed": 1,
                "conditions": [{"type": "Failed", "status": "True"}],
            }
        },
    )
    monkeypatch.setattr(
        runner.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("must fail without sleeping")),
    )

    with pytest.raises(RuntimeError, match="postgres-pitr failed"):
        runner._wait_for_job(
            job_name="postgres-pitr",
            namespace="trpc-dr-functional-1234abcd",
            kubeconfig=Path("config"),
            context="ack",
            timeout_seconds=30,
        )


def test_pull_secret_copy_selects_only_safe_metadata_and_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = "eyJhdXRocyI6eyJ4dWFueXVhbiI6eyJhdXRoIjoiYmluZCJ9fX0="

    def fake_get(*_args, **_kwargs):
        return {
            "metadata": {
                "name": "xuanyuan-pull",
                "namespace": "trpc-service",
                "uid": "source-uid",
                "resourceVersion": "secret-resource-version",
            },
            "type": "kubernetes.io/dockerconfigjson",
            "data": {".dockerconfigjson": encoded},
        }

    monkeypatch.setattr(runner, "_kubectl_json", fake_get)
    copied = runner._copy_pull_secret(
        name="xuanyuan-pull",
        source_namespace="trpc-service",
        target_namespace="trpc-dr-functional-1234abcd",
        kubeconfig=Path("config"),
        context="ack",
        timeout_seconds=30,
    )

    assert copied == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "xuanyuan-pull",
            "namespace": "trpc-dr-functional-1234abcd",
            "labels": {"app.kubernetes.io/managed-by": "trpc-dr-functional"},
        },
        "type": "kubernetes.io/dockerconfigjson",
        "data": {".dockerconfigjson": encoded},
    }
    assert "source-uid" not in json.dumps(copied)
    assert "secret-resource-version" not in json.dumps(copied)


def test_manifests_reject_mutable_images() -> None:
    with pytest.raises(ValueError, match="immutable sha256"):
        runner.build_manifests(
            namespace="trpc-dr-functional-1234abcd",
            pull_secret="xuanyuan-pull",
            candidate_image="mirror.example/acme/service:latest",
            postgres_image=f"mirror.example/pgvector@{_digest('b')}",
            minio_image=f"mirror.example/minio@{_digest('c')}",
            drill_id="drf-one",
            postgres_password="synthetic-postgres-password",
            minio_user="synthetic-user",
            minio_password="synthetic-minio-password",
            tenant_id="synthetic-tenant",
            canary="synthetic-canary",
            wrapping_key_b64="eA==",
        )


def test_functional_runtime_uses_lock_without_requiring_release_nonce(monkeypatch) -> None:
    class Config:
        support = object()
        release_id = "release-functional"

        @staticmethod
        def resolved_image_references():
            return {"initial": f"mirror.example/acme/service@{_digest('a')}"}

        @staticmethod
        def environment():
            raise AssertionError("functional mode must not require a raw release nonce")

    monkeypatch.setattr(runner, "verify_candidate_lock", lambda *_args, **_kwargs: [])
    image = runner._validate_runtime(
        Config(),
        lock={
            "release_binding": {"release_id": "release-functional"},
            "image_digest": _digest("a"),
        },
        binding={},
    )
    assert image.endswith(_digest("a"))


def test_main_does_not_touch_cluster_without_explicit_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TRPC_DR_FUNCTIONAL_ENABLED", raising=False)
    monkeypatch.setattr(
        runner,
        "_kubectl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("cluster touched")),
    )
    output = tmp_path / "functional.json"

    assert runner.main(["--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"


@pytest.mark.parametrize(
    ("collection_error", "expected_failure_code"),
    [
        (RuntimeError("secret-value-must-not-escape"), "evidence_invalid"),
        (
            runner.DisasterRecoveryCollectionTimeout(),
            "evidence_not_ready_timeout",
        ),
    ],
)
def test_enabled_runner_emits_fixed_failure_code_without_exception_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection_error: RuntimeError,
    expected_failure_code: str,
) -> None:
    class Support:
        postgres_image = f"mirror.example/postgres@{_digest('b')}"
        minio_image = f"mirror.example/minio@{_digest('c')}"

    class Config:
        context = "ack-acceptance"
        image_pull_secret = "xuanyuan-pull"
        kubeconfig = tmp_path / "config"
        release_id = "release-functional"
        support = Support()

    monkeypatch.setenv("TRPC_DR_FUNCTIONAL_ENABLED", "true")
    monkeypatch.setattr(runner, "load_runtime_gate_config", lambda _path: Config())
    monkeypatch.setattr(runner, "_read_json", lambda _path: {})
    monkeypatch.setattr(
        runner,
        "_validate_runtime",
        lambda *_args, **_kwargs: f"mirror.example/acme/service@{_digest('a')}",
    )
    monkeypatch.setattr(runner, "build_manifests", lambda **_kwargs: ({}, {}, {"items": []}))
    monkeypatch.setattr(runner, "_kubectl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runner,
        "_kubectl_json",
        lambda *_args, **_kwargs: {"metadata": {"uid": "namespace-uid"}},
    )
    monkeypatch.setattr(runner, "_copy_pull_secret", lambda **_kwargs: {"kind": "Secret"})
    monkeypatch.setattr(runner, "_wait_for_job", lambda **_kwargs: None)
    monkeypatch.setattr(
        runner,
        "collect_drill",
        lambda **_kwargs: (_ for _ in ()).throw(collection_error),
    )
    monkeypatch.setattr(runner, "_delete_namespace", lambda **_kwargs: True)
    monkeypatch.setattr(
        runner,
        "build_report",
        lambda **_kwargs: {
            "candidate": {"components": {}},
            "case_deltas": {},
            "gate": "fail",
            "production_gate": "not_run",
            "rejection_reasons": ["functional recovery evidence could not be loaded"],
        },
    )
    output = tmp_path / "functional.json"

    assert runner.main(["--output", str(output)]) == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    orchestration = report["candidate"]["orchestration"]
    assert orchestration["failure_stage"] == "evidence_collection"
    assert orchestration["failure_code"] == expected_failure_code
    assert "secret-value-must-not-escape" not in json.dumps(report)
