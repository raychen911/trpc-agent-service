from __future__ import annotations

import base64
import json
import os
import socket
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts import im_probe_preflight as preflight

IMAGE_DIGEST = "sha256:" + "a" * 64
IDENTITY_HASH = "b" * 64
RELEASE_ID = "release-test-im"
RELEASE_NONCE = "n" * 32
PROBE_URL = "https://probe.example.test"
FEISHU_PROFILE = b'{"channel":"feishu","schema_version":1}\n'
WECOM_PROFILE = b'{"channel":"wecom","schema_version":1}\n'


@pytest.fixture(autouse=True)
def _stable_source_and_dedicated_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        preflight,
        "source_fingerprint",
        lambda _root: {"status": "available", "value": "c" * 64},
    )
    if os.name != "nt" and hasattr(os, "geteuid"):
        monkeypatch.setattr(preflight.os, "geteuid", lambda: os.getuid() + 1)


def _write_private_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    if os.name != "nt":
        path.chmod(0o600)


def _write_public_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    if os.name != "nt":
        path.chmod(0o644)


def _activate_control_socket(path: Path) -> socket.socket | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        _write_private_file(path, "windows-test-socket-placeholder")
        return None
    path.parent.chmod(0o750)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  # type: ignore[attr-defined]
    listener.bind(str(path))
    path.chmod(0o660)
    return listener


def _candidate_lock() -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "release_candidate_lock",
        "release_binding": {
            "release_id": RELEASE_ID,
            "nonce_sha256": preflight._sha256(RELEASE_NONCE),
        },
        "source_fingerprint": {"status": "available", "value": "c" * 64},
        "repository": "docker.io/example/trpc-agent-service",
        "image_digest": IMAGE_DIGEST,
        "images": {
            "initial": {
                "tag": "release-test",
                "reference": "docker.io/example/trpc-agent-service@" + IMAGE_DIGEST,
                "digest": IMAGE_DIGEST,
            },
            "upgrade": {
                "tag": "release-test-upgrade",
                "reference": "docker.io/example/trpc-agent-service@sha256:" + "d" * 64,
                "digest": "sha256:" + "d" * 64,
            },
        },
    }


def _trust_document(private_key: Ed25519PrivateKey, *, url: str = PROBE_URL) -> dict[str, object]:
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return {
        "schema_version": 1,
        "probe_url": url,
        "key_id": "probe-key-test",
        "ed25519_public_key": base64.b64encode(public_key).decode("ascii"),
    }


def _environment(
    root: Path,
    *,
    private_key: Ed25519PrivateKey,
    image_digest: str = IMAGE_DIGEST,
    runner: Path | None = None,
    driver: Path | None = None,
) -> dict[str, str]:
    root.mkdir(parents=True, exist_ok=True)
    secrets = {
        "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE": root / "feishu-secret",
        "TRPC_IM_PROBE_FEISHU_VERIFICATION_TOKEN_FILE": root / "feishu-token",
        "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE": root / "feishu-key",
        "TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE": root / "wecom-secret",
    }
    for index, path in enumerate(secrets.values()):
        _write_private_file(path, f"secret-{index}")
    signing_key = root / "im-probe-seed"
    _write_private_file(
        signing_key, base64.b64encode(private_key.private_bytes_raw()).decode("ascii")
    )
    feishu_profile = root / "feishu-control-profile.json"
    wecom_profile = root / "wecom-control-profile.json"
    feishu_profile.write_bytes(FEISHU_PROFILE)
    wecom_profile.write_bytes(WECOM_PROFILE)
    if os.name != "nt":
        feishu_profile.chmod(0o600)
        wecom_profile.chmod(0o600)
    control_socket = root.parent / "control" / "im-probe.sock"
    release_context = root / "release-context.json"
    _write_private_file(
        release_context,
        json.dumps(
            {
                "schema_version": 1,
                "release_id": RELEASE_ID,
                "nonce_sha256": preflight._sha256(RELEASE_NONCE),
                "source_fingerprint": "c" * 64,
                "image_digest": image_digest,
            }
        ),
    )
    runner_path = runner or root.parent / "provider-runner"
    feishu_driver_path = driver or root.parent / "feishu-driver"
    wecom_driver_path = driver or root.parent / "wecom-driver"
    artifact_hash = lambda path: (  # noqa: E731 - compact fixture-only helper
        preflight._sha256(path.read_bytes()) if path.exists() else "1" * 64
    )
    values = {
        "TRPC_IM_PROBE_BIND_HOST": "127.0.0.1",
        "TRPC_IM_PROBE_PORT": "8750",
        "TRPC_IM_PROBE_SIGNING_KEY_FILE": str(signing_key),
        "TRPC_IM_PROBE_KEY_ID": "probe-key-test",
        "TRPC_IM_PROBE_RELEASE_CONTEXT_FILE": str(release_context),
        "TRPC_IM_PROBE_IDENTITY_SHA256": IDENTITY_HASH,
        "TRPC_IM_PROBE_FEISHU_APP_ID": "cli_testfeishu",
        "TRPC_IM_PROBE_WECOM_BOT_ID": "wecom-test-bot",
        "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE": str(feishu_profile),
        "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE": str(wecom_profile),
        "TRPC_IM_PROBE_CONTROL_SOCKET": str(control_socket),
        **{name: str(path) for name, path in secrets.items()},
        "TRPC_IM_PROBE_RUNNER": str(runner_path),
        "TRPC_IM_PROBE_RUNNER_SHA256": artifact_hash(runner_path),
        "TRPC_IM_PROBE_BROKER_UID": str(os.getuid() if hasattr(os, "getuid") else 0),
        "TRPC_IM_PROBE_BROKER_GID": str(os.getgid() if hasattr(os, "getgid") else 0),
        "TRPC_IM_PROBE_RUNNER_TIMEOUT_SECONDS": "180",
        "TRPC_IM_PROBE_DRIVER_TIMEOUT_SECONDS": "180",
        "TRPC_IM_ONLINE_TESTS_ENABLED": "true",
        "TRPC_IM_ONLINE_PROBE_URL": PROBE_URL,
        "TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST": PROBE_URL,
        "TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256": IDENTITY_HASH,
        "TRPC_IM_ONLINE_IMAGE_DIGEST": image_digest,
        "TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256": preflight._sha256(FEISHU_PROFILE),
        "TRPC_IM_ONLINE_WECOM_CONTROL_PROFILE_SHA256": preflight._sha256(WECOM_PROFILE),
        "TRPC_RELEASE_ID": RELEASE_ID,
        "TRPC_RELEASE_NONCE": RELEASE_NONCE,
    }
    if driver is not None:
        values["TRPC_IM_PROBE_FEISHU_DRIVER"] = str(feishu_driver_path)
        values["TRPC_IM_PROBE_WECOM_DRIVER"] = str(wecom_driver_path)
    else:
        values["TRPC_IM_PROBE_FEISHU_DRIVER"] = str(feishu_driver_path)
        values["TRPC_IM_PROBE_WECOM_DRIVER"] = str(wecom_driver_path)
    values["TRPC_IM_PROBE_FEISHU_DRIVER_SHA256"] = artifact_hash(feishu_driver_path)
    values["TRPC_IM_PROBE_WECOM_DRIVER_SHA256"] = artifact_hash(wecom_driver_path)
    return values


def _write_env(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )


def _write_inputs(
    tmp_path: Path,
    *,
    values: dict[str, str],
    private_key: Ed25519PrivateKey,
) -> tuple[Path, Path, Path]:
    env_file = tmp_path / "im-probe.env"
    _write_env(env_file, values)
    lock_file = tmp_path / "candidate-lock.json"
    lock_file.write_text(json.dumps(_candidate_lock()), encoding="utf-8")
    trust_file = tmp_path / "im-probe-trust.json"
    trust_file.write_text(json.dumps(_trust_document(private_key)), encoding="utf-8")
    return env_file, lock_file, trust_file


def test_local_mode_never_promotes_missing_host_paths(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.generate()
    values = _environment(tmp_path / "host", private_key=private_key)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    for name in (
        "TRPC_IM_PROBE_SIGNING_KEY_FILE",
        "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE",
        "TRPC_IM_PROBE_FEISHU_VERIFICATION_TOKEN_FILE",
        "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE",
        "TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE",
    ):
        Path(values[name]).unlink()
    runner = Path(values["TRPC_IM_PROBE_RUNNER"])
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )

    report = preflight.build_preflight(
        env_file,
        mode="local",
        candidate_lock=lock_file,
        trust_file=trust_file,
        checkout=checkout,
    )

    serialized = json.dumps(report)
    assert report["readiness"] == "not_run"
    assert report["validation_gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["secrets_recorded"] is False
    assert str(runner) not in serialized
    assert "secret-0" not in serialized
    assert "cli_testfeishu" not in serialized
    assert any(check["status"] == "not_run" for check in report["checks"])


def test_host_mode_passes_only_with_complete_external_runner_and_driver(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    host_root = tmp_path / "host"
    host_root.mkdir()
    private_key = Ed25519PrivateKey.generate()
    runner = tmp_path / "runner" / "provider-runner"
    runner.parent.mkdir()
    _write_private_file(runner, "#!/bin/sh\nexit 1\n")
    if os.name != "nt":
        runner.chmod(0o700)
    driver = tmp_path / "driver" / "provider-driver"
    driver.parent.mkdir()
    _write_private_file(driver, "#!/bin/sh\nexit 1\n")
    if os.name != "nt":
        driver.chmod(0o700)
    values = _environment(host_root, private_key=private_key, runner=runner, driver=driver)
    control_socket = Path(values["TRPC_IM_PROBE_CONTROL_SOCKET"])
    listener = _activate_control_socket(control_socket)
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )

    try:
        report = preflight.build_preflight(
            env_file,
            mode="host",
            candidate_lock=lock_file,
            trust_file=trust_file,
            checkout=checkout,
        )
    finally:
        if listener is not None:
            listener.close()
            control_socket.unlink(missing_ok=True)

    assert report["readiness"] == "pass"
    assert report["production_gate"] == "not_run"
    assert all(check["status"] == "pass" for check in report["checks"])
    assert str(runner) not in json.dumps(report)


def test_control_profile_hash_and_socket_type_are_fail_closed(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.generate()
    values = _environment(tmp_path / "host", private_key=private_key)
    values["TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256"] = "e" * 64
    socket_path = Path(values["TRPC_IM_PROBE_CONTROL_SOCKET"])
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    _write_private_file(socket_path, "not-a-unix-socket")
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )

    report = preflight.build_preflight(
        env_file,
        mode="host",
        candidate_lock=lock_file,
        trust_file=trust_file,
        checkout=tmp_path / "checkout",
    )

    assert report["readiness"] == "fail"
    reasons = " ".join(report["rejection_reasons"])
    assert "control profile hash" in reasons
    if os.name != "nt":
        assert "Unix socket" in reasons


def test_host_mode_rejects_checkout_runner_and_digest_mismatch(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    private_key = Ed25519PrivateKey.generate()
    runner = checkout / "runner"
    _write_private_file(runner, "runner")
    values = _environment(tmp_path / "host", private_key=private_key, runner=runner)
    context_path = Path(values["TRPC_IM_PROBE_RELEASE_CONTEXT_FILE"])
    context = json.loads(context_path.read_text(encoding="utf-8"))
    context["image_digest"] = "sha256:" + "e" * 64
    _write_private_file(context_path, json.dumps(context))
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )

    report = preflight.build_preflight(
        env_file,
        mode="host",
        candidate_lock=lock_file,
        trust_file=trust_file,
        checkout=checkout,
    )

    assert report["readiness"] == "fail"
    reasons = " ".join(report["rejection_reasons"])
    assert "outside application checkout" in reasons
    assert "digest" in reasons
    assert str(runner) not in reasons
    assert "sha256:" + "e" * 64 not in json.dumps(report)


def test_trust_seed_pair_and_online_identity_are_bound(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.generate()
    values = _environment(tmp_path / "host", private_key=private_key)
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )
    trust = json.loads(trust_file.read_text(encoding="utf-8"))
    trust["ed25519_public_key"] = base64.b64encode(
        Ed25519PrivateKey.generate().public_key().public_bytes_raw()
    ).decode("ascii")
    trust_file.write_text(json.dumps(trust), encoding="utf-8")
    values["TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256"] = "f" * 64
    _write_env(env_file, values)

    report = preflight.build_preflight(
        env_file,
        mode="host",
        candidate_lock=lock_file,
        trust_file=trust_file,
        checkout=tmp_path,
    )

    assert report["readiness"] == "fail"
    reasons = " ".join(report["rejection_reasons"])
    assert "Ed25519" in reasons
    assert "identity" in reasons
    assert IDENTITY_HASH not in json.dumps(report)


def test_local_mode_rejects_placeholder_paths_and_duplicate_env_keys(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.generate()
    values = _environment(tmp_path / "host", private_key=private_key)
    values["TRPC_IM_PROBE_RUNNER"] = "/usr/local/libexec/replace-with-runner"
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )
    env_file.write_text(
        env_file.read_text(encoding="utf-8") + "TRPC_IM_PROBE_PORT=8751\n",
        encoding="utf-8",
    )

    report = preflight.build_preflight(
        env_file,
        mode="local",
        candidate_lock=lock_file,
        trust_file=trust_file,
        checkout=tmp_path,
    )

    assert report["readiness"] == "not_run"
    reasons = " ".join(report["rejection_reasons"])
    assert "duplicate" in reasons
    assert "placeholder" in reasons or "path" in reasons
    assert "replace-with-runner" not in json.dumps(report)


def test_cli_writes_content_free_report_and_does_not_call_network(tmp_path: Path) -> None:
    private_key = Ed25519PrivateKey.generate()
    values = _environment(tmp_path / "host", private_key=private_key)
    env_file, lock_file, trust_file = _write_inputs(
        tmp_path, values=values, private_key=private_key
    )
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    output = tmp_path / "preflight.json"

    result = preflight.main(
        [
            "--mode",
            "local",
            "--env-file",
            str(env_file),
            "--candidate-lock",
            str(lock_file),
            "--trust-file",
            str(trust_file),
            "--checkout",
            str(checkout),
            "--output",
            str(output),
        ]
    )

    assert result == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["readiness"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert report["network_calls"] is False
    assert report["production_evidence_written"] is False
