from __future__ import annotations

import base64
import hashlib
import json
import socket
import stat
import threading
from http.client import RemoteDisconnected
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from deploy.im_probe import server as im_probe
from scripts.im_online_gate import REQUIRED_CASES, _verify_probe_signature

IMAGE_DIGEST = "sha256:" + "1" * 64
IDENTITY_SHA256 = "2" * 64
RELEASE_ID = "release-offline-probe"
RELEASE_NONCE_SHA256 = "3" * 64
SOURCE_FINGERPRINT = "4" * 64
NONCE = "probe_nonce_123456"
CONTROL_PROFILE_CONTENTS = {
    "feishu": b'{"channel":"feishu","profile":"offline"}\n',
    "wecom": b'{"channel":"wecom","profile":"offline"}\n',
}


@pytest.fixture
def probe_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
):
    secrets = {
        "feishu": {
            "FEISHU_APP_SECRET": "feishu-app-secret",
            "FEISHU_VERIFICATION_TOKEN": "feishu-verification-token",
            "FEISHU_ENCRYPT_KEY": "feishu-encrypt-key",
        },
        "wecom": {"WECOM_BOT_SECRET": "wecom-bot-secret"},
    }
    credential_paths: dict[str, dict[str, Path]] = {"feishu": {}, "wecom": {}}
    for channel, values in secrets.items():
        for name, value in values.items():
            path = tmp_path / f"{channel.lower()}-{name.lower()}"
            path.write_text(value, encoding="utf-8")
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            credential_paths[channel][name] = path

    private_key = Ed25519PrivateKey.generate()
    signing_key_path = tmp_path / "im-probe-ed25519-seed"
    signing_key_path.write_text(
        base64.b64encode(
            private_key.private_bytes_raw(),
        ).decode("ascii"),
        encoding="ascii",
    )
    signing_key_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    control_profile_paths: dict[str, Path] = {}
    for channel, contents in CONTROL_PROFILE_CONTENTS.items():
        path = tmp_path / f"{channel}-control-profile.json"
        path.write_bytes(contents)
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        control_profile_paths[channel] = path
    control_socket = tmp_path / "control.sock"
    if im_probe.os.name != "nt":
        tmp_path.chmod(0o750)
    if hasattr(socket, "AF_UNIX"):
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(control_socket))
        control_socket.chmod(0o660)
        request.addfinalizer(listener.close)
    else:
        control_socket.write_text("windows-unix-socket-placeholder", encoding="utf-8")
    release_context_path = tmp_path / "release-context.json"
    release_context_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "release_id": RELEASE_ID,
                "nonce_sha256": RELEASE_NONCE_SHA256,
                "source_fingerprint": SOURCE_FINGERPRINT,
                "image_digest": IMAGE_DIGEST,
            }
        ),
        encoding="utf-8",
    )
    release_context_path.chmod(0o600)
    config = im_probe.ProbeConfig(
        bind_host="127.0.0.1",
        port=8750,
        runner=None,
        runner_sha256=None,
        runner_timeout_seconds=1,
        driver_timeout_seconds=1,
        signing_key_path=signing_key_path,
        key_id="offline-probe-key",
        release_context_path=release_context_path,
        release_id=RELEASE_ID,
        release_nonce_sha256=RELEASE_NONCE_SHA256,
        source_fingerprint=SOURCE_FINGERPRINT,
        image_digest=IMAGE_DIGEST,
        identity_sha256=IDENTITY_SHA256,
        account_ids={"feishu": "cli_offlinefeishu", "wecom": "offline_wecom_bot"},
        credential_paths=credential_paths,
        runner_secret_paths={"feishu": {}, "wecom": {}},
        driver_paths={},
        driver_sha256={},
        control_profile_paths=control_profile_paths,
        control_socket=control_socket,
        broker_uid=None,
        broker_gid=None,
    )
    service = im_probe.ProbeService(config)
    monkeypatch.setenv("FEISHU_APP_ID", config.account_ids["feishu"])
    monkeypatch.setenv("WECOM_BOT_ID", config.account_ids["wecom"])
    return config, service, private_key, secrets


def _request(config: im_probe.ProbeConfig, channel: str, nonce: str = NONCE) -> dict[str, object]:
    credential_fingerprints = im_probe._credential_fingerprints(config, channel)
    return {
        "run_id": f"offline-{channel}-run",
        "channel": channel,
        "nonce": nonce,
        "cases": list(REQUIRED_CASES),
        "expected_image_digest": config.image_digest,
        "release_id": config.release_id,
        "release_nonce_sha256": config.release_nonce_sha256,
        "source_fingerprint": config.source_fingerprint,
        "credential_fingerprints": credential_fingerprints,
        "probe_identity_sha256": config.identity_sha256,
        "account_fingerprint": im_probe._fingerprint(
            config.account_ids[channel],
            label=im_probe.CHANNEL_ACCOUNT_VARIABLE[channel],
        ),
        "control_profile_sha256": im_probe._control_profile_sha256(
            config.control_profile_paths[channel],
            label=f"{channel} control profile",
        ),
    }


def test_signed_response_uses_gate_canonical_json(
    probe_fixture,
) -> None:
    config, _service, private_key, _secrets = probe_fixture
    payload = {
        "schema_version": 1,
        "runtime": {"status": "pass", "image_digest": config.image_digest},
        "cases": {case: {"status": "not_run"} for case in REQUIRED_CASES},
    }

    signed = im_probe._signed_response(config, payload)
    attestation = signed["signature_attestation"]
    assert attestation["algorithm"] == "ed25519"
    assert attestation["key_id"] == config.key_id
    public_key = private_key.public_key()
    _verify_probe_signature(
        signed,
        {
            "key_id": config.key_id,
            "public_key": public_key.public_bytes_raw(),
        },
    )
    signed_payload = dict(signed)
    signed_payload.pop("signature_attestation")
    public_key.verify(
        base64.b64decode(attestation["signature"]),
        im_probe._canonical_json(signed_payload),
    )


def test_missing_runner_is_signed_not_run_and_has_no_secret_leak(probe_fixture) -> None:
    config, service, private_key, secrets = probe_fixture
    response = service.handle(_request(config, "feishu"))

    assert response["error_code"] == "provider_runner_unconfigured"
    assert response["runtime"]["status"] == "pass"
    assert response["runtime"]["release_id"] == config.release_id
    assert response["runtime"]["release_nonce_sha256"] == config.release_nonce_sha256
    assert response["runtime"]["source_fingerprint"] == config.source_fingerprint
    assert set(response["runtime"]) == {
        "status",
        "run_nonce",
        "image_digest",
        "release_id",
        "release_nonce_sha256",
        "source_fingerprint",
        "identity_fingerprint",
        "control_profile_sha256",
        "artifact_attestation",
    }
    assert (
        response["runtime"]["control_profile_sha256"]
        == _request(config, "feishu")["control_profile_sha256"]
    )
    assert all(case["status"] == "not_run" for case in response["cases"].values())
    serialized = json.dumps(response, ensure_ascii=True)
    for values in secrets.values():
        for secret in values.values():
            assert secret not in serialized
    attestation = response["signature_attestation"]
    signed_payload = dict(response)
    signed_payload.pop("signature_attestation")
    private_key.public_key().verify(
        base64.b64decode(attestation["signature"]),
        im_probe._canonical_json(signed_payload),
    )


def test_request_is_bound_to_candidate_credentials_identity_and_nonce(probe_fixture) -> None:
    config, service, _private_key, _secrets = probe_fixture
    invalid = _request(config, "wecom")
    invalid["expected_image_digest"] = "sha256:" + "3" * 64
    with pytest.raises(im_probe.ProbeRequestError):
        service.handle(invalid)

    for field in ("release_id", "release_nonce_sha256", "source_fingerprint"):
        invalid_binding = _request(config, "wecom")
        invalid_binding[field] = "wrong" if field == "release_id" else "e" * 64
        with pytest.raises(im_probe.ProbeRequestError, match="does not match"):
            service.handle(invalid_binding)

    invalid_profile = _request(config, "wecom")
    invalid_profile["control_profile_sha256"] = "e" * 64
    with pytest.raises(im_probe.ProbeRequestError, match="control profile"):
        service.handle(invalid_profile)

    valid = _request(config, "wecom")
    service.handle(valid)
    with pytest.raises(im_probe.ProbeRequestError, match="nonce was already consumed"):
        service.handle(valid)


def test_invalid_provider_runner_evidence_stays_not_run(probe_fixture, monkeypatch) -> None:
    config, service, _private_key, _secrets = probe_fixture
    monkeypatch.setattr(
        im_probe,
        "_run_provider_runner",
        lambda _config, _request: {"provider_evidence": {"status": "pass"}},
    )

    response = service.handle(_request(config, "wecom"))

    assert response["error_code"] == "provider_evidence_invalid"
    assert "provider_evidence" not in response
    assert all(case["status"] == "not_run" for case in response["cases"].values())


def test_artifact_attestation_is_validated_inside_provider_evidence(
    probe_fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, service, _private_key, _secrets = probe_fixture
    artifact = {
        "schema_version": 1,
        "runner_sha256": "a" * 64,
        "runner_contract_version": 1,
        "driver_sha256": "b" * 64,
        "driver_contract_version": 1,
    }
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        im_probe,
        "_run_provider_runner",
        lambda _config, _request: {
            "provider_evidence": {"source": "offline"},
            "artifact_attestation": artifact,
        },
    )
    monkeypatch.setattr(
        im_probe,
        "_runner_artifact_attestation",
        lambda _config, _request, _value: artifact,
    )

    def validate(_channel, candidate, **_kwargs):
        observed.update(candidate)
        return candidate["provider_evidence"], []

    monkeypatch.setattr(im_probe, "_validate_provider_evidence", validate)
    response = service.handle(_request(config, "feishu"))

    assert observed["provider_evidence"]["artifact_attestation"] == artifact
    assert response["provider_evidence"]["artifact_attestation"] == artifact


def test_release_context_measures_the_deployed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = tmp_path / "release-context.json"
    context.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "release_id": RELEASE_ID,
                "nonce_sha256": RELEASE_NONCE_SHA256,
                "source_fingerprint": SOURCE_FINGERPRINT,
                "image_digest": IMAGE_DIGEST,
            }
        ),
        encoding="utf-8",
    )
    context.chmod(0o600)
    monkeypatch.setattr(
        im_probe,
        "source_fingerprint",
        lambda _root: {"status": "available", "value": SOURCE_FINGERPRINT},
    )
    assert im_probe._release_context(str(context))[1:] == (
        RELEASE_ID,
        RELEASE_NONCE_SHA256,
        SOURCE_FINGERPRINT,
        IMAGE_DIGEST,
    )

    monkeypatch.setattr(
        im_probe,
        "source_fingerprint",
        lambda _root: {"status": "available", "value": "e" * 64},
    )
    with pytest.raises(im_probe.ProbeConfigurationError, match="deployed source"):
        im_probe._release_context(str(context))


def test_runner_gets_secret_paths_but_not_secret_values(probe_fixture, monkeypatch) -> None:
    config, _service, _private_key, secrets = probe_fixture
    runner = Path(config.signing_key_path.parent / "provider-runner")
    runner.write_text("placeholder", encoding="utf-8")
    runner.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    driver = Path(config.signing_key_path.parent / "feishu-driver")
    driver.write_text("placeholder", encoding="utf-8")
    driver.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    runner_hash = hashlib.sha256(runner.read_bytes()).hexdigest()
    driver_hash = hashlib.sha256(driver.read_bytes()).hexdigest()
    config = im_probe.ProbeConfig(
        **{
            **config.__dict__,
            "runner": runner,
            "runner_sha256": runner_hash,
            "driver_paths": {"feishu": driver},
            "driver_sha256": {"feishu": driver_hash},
            "broker_uid": 0,
            "broker_gid": 0,
        },
    )
    request = _request(config, "feishu")
    observed: dict[str, object] = {}

    class FakeProcess:
        returncode = 0

        def __init__(self, *args, **kwargs) -> None:
            observed["args"] = args
            observed["kwargs"] = kwargs

        def communicate(self, *, input, timeout) -> None:
            observed["input"] = input
            observed["kwargs"]["stdout"].write(b"{}")

    def fake_popen(*args, **kwargs):
        observed["args"] = args
        observed["kwargs"] = kwargs
        return FakeProcess(*args, **kwargs)

    monkeypatch.setattr(im_probe.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        im_probe,
        "_trusted_artifact_sha256",
        lambda path, *, label: runner_hash if path == runner else driver_hash,
    )
    assert im_probe._run_provider_runner(config, request) == {}
    environment = observed["kwargs"]["env"]
    runner_input = observed["input"].decode("utf-8")
    assert all(
        str(path) in environment.values() for path in config.credential_paths["feishu"].values()
    )
    assert "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE" in environment
    assert "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE" in environment
    assert "TRPC_IM_PROBE_WECOM_BOT_ID" not in environment
    assert "TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE" not in environment
    assert environment["TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE"] == str(
        config.control_profile_paths["feishu"]
    )
    assert "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE" not in environment
    assert environment["TRPC_IM_PROBE_CONTROL_SOCKET"] == str(config.control_socket)
    assert "PYTHONPATH" not in environment
    runner_payload = json.loads(runner_input)
    assert runner_payload["control_profile_sha256"] == request["control_profile_sha256"]
    for values in secrets.values():
        for secret in values.values():
            assert secret not in runner_input
            assert secret not in environment.values()


def test_probe_service_does_not_mutate_process_account_environment(
    probe_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _service, _private_key, _secrets = probe_fixture
    monkeypatch.delenv("FEISHU_APP_ID", raising=False)
    monkeypatch.delenv("WECOM_BOT_ID", raising=False)

    im_probe.ProbeService(config)

    assert "FEISHU_APP_ID" not in im_probe.os.environ
    assert "WECOM_BOT_ID" not in im_probe.os.environ


def test_environment_requires_both_profiles_and_runner_control_socket(
    probe_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _service, _private_key, _secrets = probe_fixture
    required = {
        "TRPC_IM_PROBE_SIGNING_KEY_FILE": config.signing_key_path,
        "TRPC_IM_PROBE_FEISHU_APP_SECRET_FILE": config.credential_paths["feishu"][
            "FEISHU_APP_SECRET"
        ],
        "TRPC_IM_PROBE_FEISHU_VERIFICATION_TOKEN_FILE": config.credential_paths["feishu"][
            "FEISHU_VERIFICATION_TOKEN"
        ],
        "TRPC_IM_PROBE_FEISHU_ENCRYPT_KEY_FILE": config.credential_paths["feishu"][
            "FEISHU_ENCRYPT_KEY"
        ],
        "TRPC_IM_PROBE_WECOM_BOT_SECRET_FILE": config.credential_paths["wecom"]["WECOM_BOT_SECRET"],
        "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE": config.control_profile_paths["feishu"],
        "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE": config.control_profile_paths["wecom"],
        "TRPC_IM_PROBE_RELEASE_CONTEXT_FILE": config.release_context_path,
    }
    for variable, path in required.items():
        monkeypatch.setenv(variable, str(path))
    monkeypatch.setenv("TRPC_IM_PROBE_KEY_ID", config.key_id)
    monkeypatch.setenv("TRPC_IM_PROBE_IDENTITY_SHA256", config.identity_sha256)
    monkeypatch.setenv("TRPC_IM_PROBE_FEISHU_APP_ID", config.account_ids["feishu"])
    monkeypatch.setenv("TRPC_IM_PROBE_WECOM_BOT_ID", config.account_ids["wecom"])
    monkeypatch.delenv("TRPC_IM_PROBE_RUNNER", raising=False)
    monkeypatch.setattr(
        im_probe,
        "source_fingerprint",
        lambda _root: {"status": "available", "value": config.source_fingerprint},
    )

    assert im_probe.ProbeConfig.from_environment().control_socket is None

    monkeypatch.delenv("TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE")
    with pytest.raises(im_probe.ProbeConfigurationError, match="WECOM_CONTROL_PROFILE_FILE"):
        im_probe.ProbeConfig.from_environment()
    monkeypatch.setenv(
        "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE",
        str(config.control_profile_paths["wecom"]),
    )

    runner = config.signing_key_path.parent / "external-provider-runner"
    runner.write_text("executable", encoding="utf-8")
    runner.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    monkeypatch.setenv("TRPC_IM_PROBE_RUNNER", str(runner))
    artifact_hash = hashlib.sha256(runner.read_bytes()).hexdigest()
    monkeypatch.setenv("TRPC_IM_PROBE_RUNNER_SHA256", artifact_hash)
    for channel in ("feishu", "wecom"):
        driver = config.signing_key_path.parent / f"external-{channel}-driver"
        driver.write_text("executable", encoding="utf-8")
        driver.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        monkeypatch.setenv(f"TRPC_IM_PROBE_{channel.upper()}_DRIVER", str(driver))
        monkeypatch.setenv(f"TRPC_IM_PROBE_{channel.upper()}_DRIVER_SHA256", artifact_hash)
    socket_metadata = config.control_socket.stat()
    monkeypatch.setenv("TRPC_IM_PROBE_BROKER_UID", str(socket_metadata.st_uid))
    monkeypatch.setenv("TRPC_IM_PROBE_BROKER_GID", str(socket_metadata.st_gid))
    if im_probe.os.name != "nt":
        monkeypatch.setattr(im_probe.os, "geteuid", lambda: socket_metadata.st_uid + 1)
    monkeypatch.setattr(im_probe, "_trusted_artifact_sha256", lambda _path, *, label: artifact_hash)
    monkeypatch.delenv("TRPC_IM_PROBE_CONTROL_SOCKET", raising=False)
    with pytest.raises(im_probe.ProbeConfigurationError, match="CONTROL_SOCKET"):
        im_probe.ProbeConfig.from_environment()

    monkeypatch.setenv("TRPC_IM_PROBE_CONTROL_SOCKET", str(config.control_socket))
    assert im_probe.ProbeConfig.from_environment().control_socket == config.control_socket.resolve()

    monkeypatch.setenv("TRPC_IM_PROBE_RUNNER_SHA256", "3" * 64)
    with pytest.raises(im_probe.ProbeConfigurationError, match="hash does not match"):
        im_probe.ProbeConfig.from_environment()


def test_control_socket_owner_and_mode_are_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    if not hasattr(socket, "AF_UNIX"):
        pytest.skip("AF_UNIX is unavailable")
    path = tmp_path / "broker.sock"
    if im_probe.os.name != "nt":
        tmp_path.chmod(0o750)
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(path))
    path.chmod(0o660)
    request.addfinalizer(listener.close)
    metadata = path.stat()
    monkeypatch.setattr(im_probe.os, "name", "posix")

    assert (
        im_probe._safe_control_socket_path(
            str(path), expected_uid=metadata.st_uid, expected_gid=metadata.st_gid
        )
        == path.resolve()
    )
    with pytest.raises(im_probe.ProbeConfigurationError, match="owner or mode"):
        im_probe._safe_control_socket_path(
            str(path), expected_uid=metadata.st_uid + 1, expected_gid=metadata.st_gid
        )


def test_runner_output_is_bounded_before_json_is_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    class OversizedProcess:
        returncode = 0

        def __init__(self, *args, **kwargs) -> None:
            self.output = kwargs["stdout"]

        def communicate(self, *, input, timeout) -> None:
            self.output.write(b"x" * (im_probe.MAX_RUNNER_OUTPUT_BYTES + 1))

    monkeypatch.setattr(im_probe.subprocess, "Popen", OversizedProcess)

    assert (
        im_probe._run_bounded_json_process(
            ["provider-runner"],
            input_bytes=b"{}",
            timeout=1,
            environment={},
        )
        is None
    )


def test_nonce_cache_is_bounded_and_rejects_recent_replay(probe_fixture) -> None:
    _config, service, _private_key, _secrets = probe_fixture
    for index in range(im_probe.NONCE_CACHE_CAPACITY + 1):
        service._consume_nonce(("feishu", f"nonce-{index}"))

    assert len(service._seen) == im_probe.NONCE_CACHE_CAPACITY
    with pytest.raises(im_probe.ProbeRequestError, match="nonce was already consumed"):
        service._consume_nonce(("feishu", f"nonce-{im_probe.NONCE_CACHE_CAPACITY}"))


def test_control_profile_hash_is_bounded_and_revalidated(probe_fixture) -> None:
    config, service, _private_key, _secrets = probe_fixture
    request = _request(config, "feishu")
    config.control_profile_paths["feishu"].write_bytes(b"changed-profile")

    with pytest.raises(im_probe.ProbeRequestError, match="control profile"):
        service.handle(request)

    config.control_profile_paths["feishu"].write_bytes(
        b"x" * (im_probe.MAX_CONTROL_PROFILE_BYTES + 1)
    )
    with pytest.raises(im_probe.ProbeConfigurationError, match="too large"):
        im_probe._control_profile_sha256(
            config.control_profile_paths["feishu"],
            label="feishu control profile",
        )


def test_ready_checks_both_external_drivers(probe_fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    config, _service, _private_key, _secrets = probe_fixture
    runner = config.signing_key_path.parent / "provider-runner"
    feishu_driver = config.signing_key_path.parent / "feishu-driver"
    wecom_driver = config.signing_key_path.parent / "wecom-driver"
    for path in (runner, feishu_driver, wecom_driver):
        path.write_text("executable", encoding="utf-8")
        path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    artifact_hash = hashlib.sha256(runner.read_bytes()).hexdigest()
    config = im_probe.ProbeConfig(
        **{
            **config.__dict__,
            "runner": runner,
            "runner_sha256": artifact_hash,
            "driver_paths": {"feishu": feishu_driver, "wecom": wecom_driver},
            "driver_sha256": {"feishu": artifact_hash, "wecom": artifact_hash},
            "broker_uid": 0,
            "broker_gid": 0,
        }
    )
    observed: list[tuple[list[str], dict[str, str]]] = []

    def fake_process(command, *, input_bytes, timeout, environment):
        observed.append((list(command), dict(environment)))
        return {"status": "ready"}

    monkeypatch.setattr(im_probe, "_run_bounded_json_process", fake_process)
    monkeypatch.setattr(im_probe, "_trusted_artifact_sha256", lambda _path, *, label: artifact_hash)

    assert im_probe.ProbeService(config).ready()
    assert [command[-1] for command, _environment in observed] == ["feishu", "wecom"]
    assert "TRPC_IM_PROBE_WECOM_DRIVER" not in observed[0][1]
    assert "TRPC_IM_PROBE_FEISHU_DRIVER" not in observed[1][1]
    assert "TRPC_IM_PROBE_WECOM_CONTROL_PROFILE_FILE" not in observed[0][1]
    assert "TRPC_IM_PROBE_FEISHU_CONTROL_PROFILE_FILE" not in observed[1][1]
    assert all(
        environment["TRPC_IM_PROBE_CONTROL_SOCKET"] == str(config.control_socket)
        for _, environment in observed
    )
    assert im_probe._ProbeHTTPServer.daemon_threads is True


def test_http_health_and_invalid_request_are_local_and_fail_closed(probe_fixture) -> None:
    _config, service, _private_key, _secrets = probe_fixture
    http_server = im_probe._ProbeHTTPServer(("127.0.0.1", 0), service)
    thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{http_server.server_port}"
    try:
        with urlopen(base_url + "/health/live", timeout=3) as response:  # noqa: S310
            assert response.status == 200
            assert json.loads(response.read()) == {"status": "pass"}

        with pytest.raises(HTTPError) as ready_error:
            urlopen(base_url + "/health/ready", timeout=3)  # noqa: S310
        assert ready_error.value.code == 503
        assert json.loads(ready_error.value.read()) == {"status": "not_ready"}

        request = Request(  # noqa: S310
            base_url + "/probe",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as probe_error:
            urlopen(request, timeout=3)  # noqa: S310
        assert probe_error.value.code == 400
        assert json.loads(probe_error.value.read()) == {
            "reason": "request_invalid",
            "status": "not_run",
        }
    except RemoteDisconnected as error:
        pytest.fail(f"probe HTTP server disconnected unexpectedly: {error}")
    finally:
        http_server.shutdown()
        http_server.server_close()
        thread.join(timeout=3)
    assert not thread.is_alive()
