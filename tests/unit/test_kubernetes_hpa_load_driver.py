from __future__ import annotations

import json

import pytest

import scripts.kubernetes_hpa_load_driver as hpa_driver


def test_kubectl_environment_keeps_os_runtime_values_without_parent_secrets(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(hpa_driver.shutil, "which", lambda _name: "kubectl.exe")
    monkeypatch.setenv("KUBECONFIG", "driver-kubeconfig")
    for name, value in {
        "PATH": r"C:\Windows\System32",
        "HOME": r"C:\Users\runner",
        "USERPROFILE": r"C:\Users\runner",
        "SYSTEMROOT": r"C:\Windows",
        "WINDIR": r"C:\Windows",
        "TEMP": r"C:\Users\runner\AppData\Local\Temp",
        "TMP": r"C:\Users\runner\AppData\Local\Temp",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-inherit")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql://secret")

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return type("Completed", (), {"returncode": 0, "stdout": "{}", "stderr": ""})()

    monkeypatch.setattr(hpa_driver.subprocess, "run", fake_run)
    result = hpa_driver._kubectl(
        ["auth", "whoami", "-o", "json"],
        timeout=5,
    )

    assert result.returncode == 0
    assert captured["command"] == [
        "kubectl.exe",
        "--kubeconfig",
        "driver-kubeconfig",
        "auth",
        "whoami",
        "-o",
        "json",
    ]
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert set(environment) == {
        "PATH",
        "KUBECONFIG",
        "HOME",
        "USERPROFILE",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
    }
    assert environment["KUBECONFIG"] == "driver-kubeconfig"
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "TRPC_SERVICE_DATABASE_DSN" not in environment


def test_kubectl_environment_omits_missing_os_runtime_values(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(hpa_driver.shutil, "which", lambda _name: "kubectl.exe")
    monkeypatch.setenv("KUBECONFIG", "driver-kubeconfig")
    for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP"):
        monkeypatch.delenv(name, raising=False)

    def fake_run(_command, **kwargs):
        captured["env"] = kwargs["env"]
        return type("Completed", (), {"returncode": 0, "stdout": "{}", "stderr": ""})()

    monkeypatch.setattr(hpa_driver.subprocess, "run", fake_run)
    hpa_driver._kubectl(["version", "-o", "json"], timeout=5)

    environment = captured["env"]
    assert isinstance(environment, dict)
    assert not {"SYSTEMROOT", "WINDIR", "TEMP", "TMP"} & set(environment)


def test_load_creates_job_without_patch_permission(monkeypatch) -> None:
    calls: list[list[str]] = []
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "load",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["/bin/sh", "-c", "sleep 1"],
    }
    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        },
        "status": {"succeeded": 1},
    }
    observed_jobs = iter((None, job_payload))
    monkeypatch.setattr(hpa_driver, "_get_job", lambda _config, _timeout: next(observed_jobs))

    def fake_kubectl(arguments, *, timeout, input_text=None):
        del timeout
        calls.append(arguments)
        assert input_text is not None
        manifest = json.loads(input_text)
        assert manifest["kind"] == "Job"
        assert manifest["spec"]["template"]["spec"]["securityContext"] == {
            "runAsNonRoot": True,
            "runAsUser": 999,
            "runAsGroup": 999,
            "seccompProfile": {"type": "RuntimeDefault"},
        }
        return type("Completed", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(hpa_driver, "_kubectl", fake_kubectl)
    result = hpa_driver._load(config, 5)

    assert result["status"] == "pass"
    assert calls == [["create", "-f", "-"]]


def test_job_manifest_uses_configured_image_pull_secret() -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "load",
        "subject": "system:serviceaccount:driver:driver",
        "image": "ghcr.io/acme/backlog@sha256:" + "c" * 64,
        "command": ["python", "-m", "backlog"],
        "image_pull_secret": "ghcr-pull",
    }
    manifest = hpa_driver._job_manifest(config)
    assert manifest["spec"]["template"]["spec"]["imagePullSecrets"] == [{"name": "ghcr-pull"}]
    assert ".dockerconfigjson" not in json.dumps(manifest)


def test_configuration_rejects_invalid_image_pull_secret(monkeypatch) -> None:
    for name, value in {
        "TRPC_K8S_HPA_NAMESPACE": "runtime-gate",
        "TRPC_K8S_HPA_RUN_NONCE": "a" * 32,
        "TRPC_K8S_HPA_CLUSTER_FINGERPRINT": "b" * 64,
        "TRPC_K8S_HPA_PHASE": "load",
        "TRPC_K8S_HPA_DRIVER_SUBJECT": "system:serviceaccount:driver:driver",
        "TRPC_K8S_HPA_DRIVER_JOB_IMAGE": "ghcr.io/acme/backlog@sha256:" + "c" * 64,
        "TRPC_K8S_HPA_DRIVER_JOB_COMMAND": '["python","-m","backlog"]',
        "TRPC_K8S_HPA_DRIVER_IMAGE_PULL_SECRET": "registry/secret",
    }.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="image pull Secret name is invalid"):
        hpa_driver._configuration()


def test_clear_uses_async_delete_and_polls_exact_job_until_absent(monkeypatch) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["/bin/sh", "-c", "sleep 1"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        },
        "status": {"succeeded": 1},
    }
    observed_jobs = iter((job_payload, job_payload, None))
    polls = 0

    def fake_get_job(_config, _timeout):
        nonlocal polls
        polls += 1
        return next(observed_jobs)

    calls: list[list[str]] = []

    def fake_kubectl(arguments, *, timeout, input_text=None):
        del timeout, input_text
        calls.append(arguments)
        return type("Completed", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", fake_get_job)
    monkeypatch.setattr(hpa_driver, "_kubectl", fake_kubectl)
    monkeypatch.setattr(hpa_driver.time, "sleep", lambda _seconds: None)

    result = hpa_driver._clear(config, 5)

    assert result["status"] == "pass"
    assert result["job_uid"] == "job-uid"
    assert result["job_deleted"] is True
    assert polls == 3
    assert calls == [
        [
            "delete",
            "job",
            hpa_driver._job_name(config["nonce"]),
            "--namespace",
            "runtime-gate",
            "--ignore-not-found",
            "--wait=false",
        ]
    ]


@pytest.mark.parametrize("delete_outcome", ["nonzero", "timeout"])
def test_clear_confirms_absence_after_ambiguous_delete(monkeypatch, delete_outcome: str) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["python", "-m", "backlog"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        },
        "status": {"succeeded": 1},
    }
    observed_jobs = iter((job_payload, None))

    def fake_delete(_arguments, *, timeout, input_text=None):
        del timeout, input_text
        if delete_outcome == "timeout":
            raise hpa_driver._TransientKubectlError("kubectl command timed out")
        return type("Completed", (), {"returncode": 1, "stdout": "", "stderr": "unexpected EOF"})()

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", lambda _config, _timeout: next(observed_jobs))
    monkeypatch.setattr(hpa_driver, "_kubectl", fake_delete)

    result = hpa_driver._clear(config, 5)

    assert result["status"] == "pass"
    assert result["job_uid"] == "job-uid"
    assert result["job_deleted"] is True
    assert result["already_absent"] is False


def test_clear_retries_transient_post_delete_read(monkeypatch) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["python", "-m", "backlog"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        },
        "status": {"succeeded": 1},
    }
    observed_jobs = iter((job_payload, hpa_driver._TransientKubectlError("connection reset"), None))

    def fake_get_job(_config, _timeout):
        observed = next(observed_jobs)
        if isinstance(observed, hpa_driver._TransientKubectlError):
            raise observed
        return observed

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", fake_get_job)
    monkeypatch.setattr(
        hpa_driver,
        "_kubectl",
        lambda _arguments, *, timeout, input_text=None: type(
            "Completed", (), {"returncode": 0, "stdout": "", "stderr": ""}
        )(),
    )
    monkeypatch.setattr(hpa_driver.time, "sleep", lambda _seconds: None)

    result = hpa_driver._clear(config, 5)

    assert result["status"] == "pass"
    assert result["job_uid"] == "job-uid"
    assert result["job_deleted"] is True


def test_clear_retries_transient_initial_job_read(monkeypatch) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["python", "-m", "backlog"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        }
    }
    observed_jobs = iter((hpa_driver._TransientKubectlError("unexpected EOF"), job_payload, None))

    def fake_get_job(_config, _timeout):
        observed = next(observed_jobs)
        if isinstance(observed, hpa_driver._TransientKubectlError):
            raise observed
        return observed

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", fake_get_job)
    monkeypatch.setattr(
        hpa_driver,
        "_kubectl",
        lambda _arguments, *, timeout, input_text=None: type(
            "Completed", (), {"returncode": 0, "stdout": "", "stderr": ""}
        )(),
    )
    monkeypatch.setattr(hpa_driver.time, "sleep", lambda _seconds: None)

    result = hpa_driver._clear(config, 5)

    assert result["status"] == "pass"
    assert result["job_uid"] == "job-uid"


def test_clear_retries_ambiguous_delete_while_same_job_remains(monkeypatch) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["python", "-m", "backlog"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        }
    }
    observed_jobs = iter((job_payload, job_payload, None))
    delete_calls = 0

    def fake_delete(_arguments, *, timeout, input_text=None):
        nonlocal delete_calls
        del timeout, input_text
        delete_calls += 1
        return type(
            "Completed",
            (),
            {
                "returncode": 1 if delete_calls == 1 else 0,
                "stdout": "",
                "stderr": "unexpected EOF" if delete_calls == 1 else "",
            },
        )()

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", lambda _config, _timeout: next(observed_jobs))
    monkeypatch.setattr(hpa_driver, "_kubectl", fake_delete)
    monkeypatch.setattr(hpa_driver.time, "sleep", lambda _seconds: None)

    result = hpa_driver._clear(config, 5)

    assert result["status"] == "pass"
    assert result["job_uid"] == "job-uid"
    assert delete_calls == 2


def test_clear_rejects_non_transient_delete_error(monkeypatch) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["python", "-m", "backlog"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        }
    }

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", lambda _config, _timeout: job_payload)
    monkeypatch.setattr(
        hpa_driver,
        "_kubectl",
        lambda _arguments, *, timeout, input_text=None: type(
            "Completed", (), {"returncode": 1, "stdout": "", "stderr": "Forbidden"}
        )(),
    )

    with pytest.raises(RuntimeError, match="could not be deleted"):
        hpa_driver._clear(config, 5)


def test_clear_fails_closed_when_exact_job_never_becomes_absent(monkeypatch) -> None:
    config = {
        "namespace": "runtime-gate",
        "nonce": "a" * 32,
        "fingerprint": "b" * 64,
        "phase": "clear",
        "subject": "system:serviceaccount:driver:driver",
        "image": "registry.example/redis@sha256:" + "c" * 64,
        "command": ["/bin/sh", "-c", "sleep 1"],
    }
    job_payload = {
        "metadata": {
            "name": hpa_driver._job_name(config["nonce"]),
            "namespace": config["namespace"],
            "uid": "job-uid",
            "labels": hpa_driver._labels(config),
        },
        "status": {"succeeded": 1},
    }
    calls = 0

    def fake_monotonic() -> float:
        nonlocal calls
        calls += 1
        return 0.0 if calls <= 3 else 2.0

    monkeypatch.setattr(hpa_driver, "_whoami", lambda *_args: None)
    monkeypatch.setattr(hpa_driver, "_cluster_fingerprint", lambda _timeout: "b" * 64)
    monkeypatch.setattr(hpa_driver, "_get_job", lambda _config, _timeout: job_payload)
    monkeypatch.setattr(
        hpa_driver,
        "_kubectl",
        lambda _arguments, *, timeout, input_text=None: type(
            "Completed", (), {"returncode": 0, "stdout": "", "stderr": ""}
        )(),
    )
    monkeypatch.setattr(hpa_driver.time, "monotonic", fake_monotonic)

    with pytest.raises(RuntimeError, match="deletion was not observed"):
        hpa_driver._clear(config, 1)
