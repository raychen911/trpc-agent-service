from __future__ import annotations

import hashlib
import json
import os
import shlex
import stat
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from deploy.im_probe import control_broker as broker

RUN_NONCE = "offline_nonce_123456"
_VALIDATE_POSIX_FILE_OWNER = broker._validate_posix_file_owner
_VALIDATE_TRUSTED_PARENT_CHAIN = broker._validate_trusted_parent_chain


@pytest.fixture(autouse=True)
def _allow_non_root_test_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    if os.name == "posix":
        tmp_path.chmod(0o750)
    if os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() == 0:
        return
    monkeypatch.setattr(broker, "_validate_posix_file_owner", lambda _metadata: None)
    monkeypatch.setattr(broker, "_validate_trusted_parent_chain", lambda _path: None)


def _write_secure(path: Path, contents: str) -> Path:
    path.write_text(contents, encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def _metadata(mode: int, *, uid: int) -> os.stat_result:
    return os.stat_result((mode, 0, 0, 1, uid, 0, 0, 0, 0, 0))


def _config_value(
    tmp_path: Path,
    *,
    handler_code: str = "import json; print(json.dumps({'status': 'pass'}))",
    timeout_seconds: float = 2.0,
) -> tuple[dict[str, Any], dict[str, Path]]:
    paths: dict[str, Path] = {}
    channels: dict[str, Any] = {}
    executable = Path(getattr(sys, "_base_executable", sys.executable))
    if os.name == "posix":
        executable = _write_secure(
            tmp_path / "python-runner",
            "#!/bin/sh\nexec " + shlex.quote(str(executable)) + ' "$@"\n',
        )
        executable.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    executable_sha256 = hashlib.sha256(executable.read_bytes()).hexdigest()
    for channel in broker.CHANNELS:
        profile = _write_secure(
            tmp_path / f"{channel}-control.json",
            json.dumps({"schema_version": 1, "channel": channel}),
        )
        paths[f"{channel}_profile"] = profile
        channels[channel] = {
            "control_profile_file": str(profile),
            "control_profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
            "allowed_actions": {
                "exercise": {
                    "executable": str(executable),
                    "sha256": executable_sha256,
                    "argv": ["-c", handler_code, "fixed-argv"],
                    "timeout_seconds": timeout_seconds,
                }
            },
        }
    value = {
        "schema_version": 1,
        "socket_path": str(tmp_path / "control.sock"),
        "socket_mode": "0660",
        "channels": channels,
    }
    return value, paths


def _load_config(tmp_path: Path, value: dict[str, Any]) -> broker.BrokerConfig:
    path = _write_secure(tmp_path / "broker.json", json.dumps(value))
    return broker._load_config(path, application_root=tmp_path / "application-checkout")


def _request(config: broker.BrokerConfig, channel: str = "feishu") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel": channel,
        "action": "exercise",
        "run_id": f"offline-{channel}-run",
        "run_nonce": RUN_NONCE,
        "control_profile_sha256": config.channels[channel].control_profile_sha256,
        "payload": {"marker": "safe", "argv": ["request-must-not-win"]},
    }


def test_strict_json_rejects_duplicates_non_finite_and_trailing_values() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        broker._strict_json('{"a":1,"a":2}')
    with pytest.raises(ValueError, match="non-finite"):
        broker._strict_json('{"a":NaN}')
    with pytest.raises(ValueError):
        broker._strict_json("{} {}")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update({"extra": True}),
        lambda value: value["channels"].pop("wecom"),
        lambda value: value["channels"]["feishu"].update({"extra": True}),
        lambda value: value["channels"]["feishu"]["allowed_actions"]["exercise"].update(
            {"env": {"KUBECONFIG": "forbidden"}}
        ),
    ],
)
def test_config_schema_is_strict(
    tmp_path: Path,
    mutation: Any,
) -> None:
    value, _ = _config_value(tmp_path)
    mutation(value)

    with pytest.raises(broker.ConfigError, match="schema"):
        _load_config(tmp_path, value)


def test_action_hash_is_required_and_verified_when_loading(tmp_path: Path) -> None:
    value, _ = _config_value(tmp_path)
    action = value["channels"]["feishu"]["allowed_actions"]["exercise"]
    action.pop("sha256")
    with pytest.raises(broker.ConfigError, match="schema"):
        _load_config(tmp_path, value)

    value, _ = _config_value(tmp_path)
    action = value["channels"]["feishu"]["allowed_actions"]["exercise"]
    action["sha256"] = "not-a-sha256"
    with pytest.raises(broker.ConfigError, match="hash is invalid"):
        _load_config(tmp_path, value)

    value, _ = _config_value(tmp_path)
    action = value["channels"]["feishu"]["allowed_actions"]["exercise"]
    action["sha256"] = "f" * 64
    with pytest.raises(broker.ConfigError, match="hash does not match"):
        _load_config(tmp_path, value)


def test_posix_trusted_file_must_be_root_owned() -> None:
    with pytest.raises(broker.ConfigError, match="root-owned"):
        _VALIDATE_POSIX_FILE_OWNER(_metadata(stat.S_IFREG | 0o600, uid=1000))


@pytest.mark.parametrize(
    ("mode", "uid"),
    [
        (stat.S_IFDIR | 0o770, 0),
        (stat.S_IFDIR | 0o700, 1000),
    ],
)
def test_posix_trusted_parent_chain_rejects_writable_directory(
    tmp_path: Path,
    mode: int,
    uid: int,
) -> None:
    target = tmp_path / "trusted" / "config.json"
    unsafe_parent = target.parent

    def metadata(path: Path) -> os.stat_result:
        if path == unsafe_parent:
            return _metadata(mode, uid=uid)
        return _metadata(stat.S_IFDIR | 0o755, uid=0)

    with patch.object(Path, "lstat", autospec=True, side_effect=metadata):
        with pytest.raises(broker.ConfigError, match="parent"):
            _VALIDATE_TRUSTED_PARENT_CHAIN(target)


def test_config_rejects_relative_symlink_checkout_or_insecure_handler(
    tmp_path: Path,
) -> None:
    value, _ = _config_value(tmp_path)
    action = value["channels"]["feishu"]["allowed_actions"]["exercise"]
    action["executable"] = "relative-handler"
    with pytest.raises(broker.ConfigError, match="absolute"):
        _load_config(tmp_path, value)

    application_root = tmp_path / "application-checkout"
    application_root.mkdir()
    handler = _write_secure(application_root / "handler", "handler")
    handler.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    action["executable"] = str(handler)
    with pytest.raises(broker.ConfigError, match="application checkout"):
        _load_config(tmp_path, value)

    assert broker._secure_posix_mode(0o600)
    assert broker._secure_posix_mode(0o640)
    assert not broker._secure_posix_mode(0o620)
    assert not broker._secure_posix_mode(0o602)

    symlink = tmp_path / "symlink-handler"
    with patch.object(
        Path,
        "lstat",
        return_value=os.stat_result((stat.S_IFLNK | 0o777,) + (0,) * 9),
    ):
        with pytest.raises(broker.ConfigError, match="non-symlink"):
            broker._validate_secure_regular_file(symlink, executable=True)


def test_config_rejects_invalid_socket_or_action_limits(tmp_path: Path) -> None:
    value, _ = _config_value(tmp_path)
    value["socket_path"] = "relative.sock"
    with pytest.raises(broker.ConfigError, match="socket path"):
        _load_config(tmp_path, value)

    value, _ = _config_value(tmp_path)
    value["socket_mode"] = "0666"
    with pytest.raises(broker.ConfigError, match="socket mode"):
        _load_config(tmp_path, value)

    value, _ = _config_value(tmp_path, timeout_seconds=181.0)
    with pytest.raises(broker.ConfigError, match="timeout"):
        _load_config(tmp_path, value)


def test_request_schema_payload_and_action_allowlist_are_strict(tmp_path: Path) -> None:
    value, _ = _config_value(tmp_path)
    config = _load_config(tmp_path, value)
    request = _request(config)
    assert broker._parse_request(request).action == "exercise"

    request["executable"] = sys.executable
    with pytest.raises(broker.RequestError, match="schema"):
        broker._parse_request(request)

    request = _request(config)
    request["payload"] = {"deep": [[[[[[[[[[[[[[[[["too-deep"]]]]]]]]]]]]]]]]]}
    with pytest.raises(broker.RequestError, match="payload"):
        broker._parse_request(request)

    request = _request(config)
    request["action"] = "not-allowed"
    response = broker._process_request(config, request)
    assert response == {"status": "not_run", "error_code": "action_not_allowed"}


def test_profile_hash_is_recomputed_for_every_request(tmp_path: Path) -> None:
    value, paths = _config_value(tmp_path)
    config = _load_config(tmp_path, value)
    request = _request(config)
    request["control_profile_sha256"] = "f" * 64
    assert broker._process_request(config, request) == {
        "status": "not_run",
        "error_code": "profile_mismatch",
    }

    paths["feishu_profile"].write_text("changed", encoding="utf-8")
    assert broker._process_request(config, _request(config)) == {
        "status": "not_run",
        "error_code": "profile_mismatch",
    }


def test_action_hash_is_recomputed_before_every_execution(tmp_path: Path) -> None:
    value, _ = _config_value(tmp_path)
    handler = _write_secure(tmp_path / "reviewed-action", "#!/bin/sh\nexit 0\n")
    handler.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    digest = hashlib.sha256(handler.read_bytes()).hexdigest()
    for channel in broker.CHANNELS:
        action = value["channels"][channel]["allowed_actions"]["exercise"]
        action.update({"executable": str(handler), "sha256": digest, "argv": []})
    config = _load_config(tmp_path, value)

    handler.write_text("#!/bin/sh\nexit 78\n", encoding="utf-8")
    handler.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    with patch("deploy.im_probe.control_broker.subprocess.Popen") as popen:
        assert broker._process_request(config, _request(config)) == {
            "status": "not_run",
            "error_code": "handler_hash_mismatch",
        }
    popen.assert_not_called()


def test_handler_receives_canonical_request_fixed_argv_and_empty_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = (
        "import json,os,sys; request=json.load(sys.stdin); "
        "print(json.dumps({'status':'pass','action':os.environ.get('TRPC_IM_CONTROL_ACTION'),"
        "'forbidden':sorted(set(os.environ)&{'PATH','PYTHONPATH','KUBECONFIG'}),"
        "'argv':sys.argv[1:],'payload':request['payload']},sort_keys=True))"
    )
    value, _ = _config_value(tmp_path, handler_code=code)
    config = _load_config(tmp_path, value)
    monkeypatch.setenv("PATH", "must-not-leak")
    monkeypatch.setenv("PYTHONPATH", "must-not-leak")
    monkeypatch.setenv("KUBECONFIG", "must-not-leak")

    response = broker._process_request(config, _request(config))

    assert response["status"] == "pass"
    assert set(response) == {"status", "result"}
    result = response["result"]
    assert result["status"] == "pass"
    assert result["action"] == "exercise"
    assert result["forbidden"] == []
    assert result["argv"] == ["fixed-argv"]
    assert result["payload"]["argv"] == ["request-must-not-win"]


def test_handler_timeout_and_output_limit_are_content_free(tmp_path: Path) -> None:
    value, _ = _config_value(
        tmp_path,
        handler_code="import time; time.sleep(1)",
        timeout_seconds=0.05,
    )
    config = _load_config(tmp_path, value)
    assert broker._process_request(config, _request(config)) == {
        "status": "not_run",
        "error_code": "handler_timeout",
    }

    value, _ = _config_value(
        tmp_path,
        handler_code=f"import sys; sys.stdout.write('x'*{broker.MAX_OUTPUT_BYTES + 1})",
    )
    config = _load_config(tmp_path, value)
    assert broker._process_request(config, _request(config)) == {
        "status": "not_run",
        "error_code": "handler_output_too_large",
    }


def test_line_protocol_is_bounded_and_returns_only_error_enum(tmp_path: Path) -> None:
    value, _ = _config_value(tmp_path)
    config = _load_config(tmp_path, value)
    assert broker._process_line(config, b"not-json\n") == (
        b'{"error_code":"invalid_request","status":"not_run"}\n'
    )
    assert broker._process_line(config, b"x" * (broker.MAX_INPUT_BYTES + 1)) == (
        b'{"error_code":"request_too_large","status":"not_run"}\n'
    )
    assert broker._process_line(config, b"{}\n{}\n") == (
        b'{"error_code":"invalid_request","status":"not_run"}\n'
    )


def test_check_validates_without_executing_handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "handler-ran"
    code = f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')"
    value, _ = _config_value(tmp_path, handler_code=code)
    config_path = _write_secure(tmp_path / "broker.json", json.dumps(value))
    monkeypatch.setenv("TRPC_IM_CONTROL_BROKER_CONFIG_FILE", str(config_path))
    monkeypatch.setattr(broker, "_platform_supports_unix_socket", lambda: True)

    assert broker._check() is True
    assert not marker.exists()


def test_check_rejects_action_replaced_without_hash_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, _ = _config_value(tmp_path)
    handler = _write_secure(tmp_path / "reviewed-action", "#!/bin/sh\nexit 0\n")
    handler.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    digest = hashlib.sha256(handler.read_bytes()).hexdigest()
    for channel in broker.CHANNELS:
        action = value["channels"][channel]["allowed_actions"]["exercise"]
        action.update({"executable": str(handler), "sha256": digest, "argv": []})
    config_path = _write_secure(tmp_path / "broker.json", json.dumps(value))
    monkeypatch.setenv("TRPC_IM_CONTROL_BROKER_CONFIG_FILE", str(config_path))
    monkeypatch.setattr(broker, "_platform_supports_unix_socket", lambda: True)

    assert broker._check() is True
    handler.write_text("#!/bin/sh\nexit 78\n", encoding="utf-8")
    handler.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    assert broker._check() is False


def test_check_output_is_content_free(
    monkeypatch: pytest.MonkeyPatch,
    capsysbinary: pytest.CaptureFixture[bytes],
) -> None:
    monkeypatch.setattr(broker, "_check", lambda: True)

    assert broker.main(["--check"]) == 0
    assert capsysbinary.readouterr().out == b'{"status":"ready"}\n'
