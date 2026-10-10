#!/usr/bin/env python3
"""Minimal fail-closed Unix-socket broker for independent IM evidence drivers.

The broker does not observe providers and cannot manufacture evidence.  It
only maps a strict request to a host-owned, preconfigured executable and
returns that executable's single JSON object.  Executable, argv and
environment are never selected by the requester.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import socket
import socketserver
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, cast

MAX_INPUT_BYTES = 64 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
MAX_CONFIG_BYTES = 64 * 1024
MAX_PROFILE_BYTES = 64 * 1024
MAX_PAYLOAD_DEPTH = 16
MAX_PAYLOAD_NODES = 2048
MAX_PAYLOAD_STRING_BYTES = 16 * 1024
MAX_ARGV_ITEMS = 64
MAX_ARG_BYTES = 16 * 1024
MAX_TIMEOUT_SECONDS = 180.0

CHANNELS = ("feishu", "wecom")
CONFIG_ENV = "TRPC_IM_CONTROL_BROKER_CONFIG_FILE"
APPLICATION_ROOT_ENV = "TRPC_IM_PROBE_APPLICATION_ROOT"
ACTION_ENV = "TRPC_IM_CONTROL_ACTION"

CONFIG_FIELDS = frozenset({"schema_version", "socket_path", "socket_mode", "channels"})
CHANNEL_FIELDS = frozenset({"control_profile_file", "control_profile_sha256", "allowed_actions"})
ACTION_FIELDS = frozenset({"executable", "sha256", "argv", "timeout_seconds"})
REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "channel",
        "action",
        "run_id",
        "run_nonce",
        "control_profile_sha256",
        "payload",
    }
)
SOCKET_MODES = {"0600": 0o600, "0660": 0o660}

SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$")
NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class ConfigError(RuntimeError):
    """Host configuration is unsafe or malformed."""


class RequestError(RuntimeError):
    """A client request is malformed."""


class DispatchError(RuntimeError):
    """A content-free error code safe to expose to a client."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True)
class ActionConfig:
    executable: Path
    sha256: str
    argv: tuple[str, ...]
    timeout_seconds: float


@dataclass(frozen=True)
class ChannelConfig:
    control_profile_file: Path
    control_profile_sha256: str
    allowed_actions: dict[str, ActionConfig]


@dataclass(frozen=True)
class BrokerConfig:
    socket_path: Path
    socket_mode: int
    channels: dict[str, ChannelConfig]


@dataclass(frozen=True)
class BrokerRequest:
    channel: str
    action: str
    run_id: str
    run_nonce: str
    control_profile_sha256: str
    payload: dict[str, Any]

    def canonical_value(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "channel": self.channel,
            "action": self.action,
            "run_id": self.run_id,
            "run_nonce": self.run_nonce,
            "control_profile_sha256": self.control_profile_sha256,
            "payload": self.payload,
        }


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value} is forbidden")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key {key!r}")
        value[key] = item
    return value


def _strict_json(raw: str) -> Any:
    return json.loads(
        raw,
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )


def _secure_posix_mode(mode: int) -> bool:
    return mode & (stat.S_IWGRP | stat.S_IWOTH) == 0


def _effective_ids() -> tuple[int, int]:
    get_uid = getattr(os, "geteuid", None)
    get_gid = getattr(os, "getegid", None)
    if not callable(get_uid) or not callable(get_gid):
        raise ConfigError("POSIX process identity is unavailable")
    return int(get_uid()), int(get_gid())


def _validate_posix_file_owner(metadata: os.stat_result) -> None:
    if metadata.st_uid != 0:
        raise ConfigError("configured file must be root-owned")


def _validate_trusted_parent_chain(path: Path) -> None:
    current = path.parent
    while True:
        try:
            metadata = current.lstat()
        except OSError as error:
            raise ConfigError("configured file parent is unavailable") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ConfigError("configured file parent must be a non-symlink directory")
        if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ConfigError("configured file parent is group or other writable")
        if metadata.st_uid != 0 and metadata.st_mode & stat.S_IWUSR:
            raise ConfigError("configured file parent is writable by a non-root owner")
        parent = current.parent
        if parent == current:
            return
        current = parent


def _validate_secure_regular_file(
    path: Path,
    *,
    executable: bool = False,
) -> os.stat_result:
    if not path.is_absolute():
        raise ConfigError("path must be absolute")
    if os.name == "posix":
        _validate_trusted_parent_chain(path)
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ConfigError("configured file is unavailable") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ConfigError("configured file must be a non-symlink regular file")
    if os.name == "posix":
        _validate_posix_file_owner(metadata)
        if not _secure_posix_mode(metadata.st_mode):
            raise ConfigError("configured file is group or other writable")
    if executable and not os.access(path, os.X_OK):
        raise ConfigError("configured executable is not executable")
    return metadata


def _read_secure_file(path: Path, *, limit: int) -> bytes:
    _validate_secure_regular_file(path)
    flags = os.O_RDONLY
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow:
        flags |= nofollow
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ConfigError("configured file cannot be opened") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ConfigError("configured file changed type")
        if os.name == "posix":
            _validate_posix_file_owner(metadata)
            if not _secure_posix_mode(metadata.st_mode):
                raise ConfigError("configured file is group or other writable")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            contents = stream.read(limit + 1)
    finally:
        os.close(descriptor)
    if len(contents) > limit:
        raise ConfigError("configured file is too large")
    return contents


def _secure_file_sha256(path: Path, *, executable: bool = False) -> str:
    _validate_secure_regular_file(path, executable=executable)
    flags = os.O_RDONLY
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow:
        flags |= nofollow
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ConfigError("configured file cannot be opened") from error
    digest = hashlib.sha256()
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ConfigError("configured file changed type")
        if os.name == "posix":
            _validate_posix_file_owner(metadata)
            if not _secure_posix_mode(metadata.st_mode):
                raise ConfigError("configured file is group or other writable")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(64 * 1024):
                digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _application_root() -> Path | None:
    configured = os.environ.get(APPLICATION_ROOT_ENV, "").strip()
    if configured:
        candidate = Path(configured)
        if not candidate.is_absolute():
            raise ConfigError("application root must be absolute")
        return candidate.resolve(strict=False)
    for candidate in Path(__file__).resolve().parents:
        if (candidate / ".git").exists():
            return candidate
    return None


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def _safe_name(value: object) -> str | None:
    if not isinstance(value, str) or SAFE_NAME_RE.fullmatch(value) is None:
        return None
    return value


def _safe_identifier(value: object) -> str | None:
    if not isinstance(value, str) or SAFE_ID_RE.fullmatch(value) is None:
        return None
    return value


def _parse_action(value: object, *, application_root: Path | None) -> ActionConfig:
    if not isinstance(value, dict) or set(value) != ACTION_FIELDS:
        raise ConfigError("allowed action has an invalid schema")
    executable_value = value.get("executable")
    if not isinstance(executable_value, str):
        raise ConfigError("allowed action executable must be absolute")
    executable = Path(executable_value)
    if not executable.is_absolute():
        raise ConfigError("allowed action executable must be absolute")
    _validate_secure_regular_file(executable, executable=True)
    if application_root is not None and _is_within(executable, application_root):
        raise ConfigError("allowed action executable is inside the application checkout")
    action_hash = value.get("sha256")
    if not isinstance(action_hash, str) or HEX64_RE.fullmatch(action_hash) is None:
        raise ConfigError("allowed action hash is invalid")
    actual_hash = _secure_file_sha256(executable, executable=True)
    if actual_hash != action_hash.lower():
        raise ConfigError("allowed action hash does not match")

    argv_value = value.get("argv")
    if (
        not isinstance(argv_value, list)
        or len(argv_value) > MAX_ARGV_ITEMS
        or any(
            not isinstance(item, str) or "\x00" in item or len(item.encode("utf-8")) > MAX_ARG_BYTES
            for item in argv_value
        )
    ):
        raise ConfigError("allowed action argv is invalid")
    timeout = value.get("timeout_seconds")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(float(timeout))
        or not 0 < float(timeout) <= MAX_TIMEOUT_SECONDS
    ):
        raise ConfigError("allowed action timeout is invalid")
    return ActionConfig(
        executable=executable.resolve(strict=True),
        sha256=action_hash.lower(),
        argv=tuple(argv_value),
        timeout_seconds=float(timeout),
    )


def _parse_channel(value: object, *, application_root: Path | None) -> ChannelConfig:
    if not isinstance(value, dict) or set(value) != CHANNEL_FIELDS:
        raise ConfigError("channel has an invalid schema")
    profile_value = value.get("control_profile_file")
    profile_hash = value.get("control_profile_sha256")
    if not isinstance(profile_value, str):
        raise ConfigError("control profile path must be absolute")
    profile = Path(profile_value)
    if not profile.is_absolute():
        raise ConfigError("control profile path must be absolute")
    if not isinstance(profile_hash, str) or HEX64_RE.fullmatch(profile_hash) is None:
        raise ConfigError("control profile hash is invalid")
    actual_hash = hashlib.sha256(_read_secure_file(profile, limit=MAX_PROFILE_BYTES)).hexdigest()
    if actual_hash != profile_hash.lower():
        raise ConfigError("control profile hash does not match")

    actions_value = value.get("allowed_actions")
    if not isinstance(actions_value, dict) or not actions_value:
        raise ConfigError("allowed actions map is invalid")
    actions: dict[str, ActionConfig] = {}
    for raw_name, raw_action in actions_value.items():
        name = _safe_name(raw_name)
        if name is None:
            raise ConfigError("allowed action name is invalid")
        actions[name] = _parse_action(raw_action, application_root=application_root)
    return ChannelConfig(
        control_profile_file=profile.resolve(strict=True),
        control_profile_sha256=profile_hash.lower(),
        allowed_actions=actions,
    )


def _validate_socket_parent(
    socket_path: Path,
    *,
    starting: bool,
    socket_mode: int = 0o600,
) -> None:
    if not socket_path.is_absolute():
        raise ConfigError("socket path must be absolute")
    parent = socket_path.parent
    try:
        parent_metadata = parent.lstat()
    except OSError as error:
        raise ConfigError("socket parent is unavailable") from error
    if stat.S_ISLNK(parent_metadata.st_mode) or not stat.S_ISDIR(parent_metadata.st_mode):
        raise ConfigError("socket parent must be a non-symlink directory")
    if os.name == "posix" and (
        parent_metadata.st_uid != _effective_ids()[0]
        or parent_metadata.st_gid != _effective_ids()[1]
        or stat.S_IMODE(parent_metadata.st_mode) != 0o750
    ):
        raise ConfigError("socket parent owner or mode is invalid")
    try:
        socket_metadata = socket_path.lstat()
    except FileNotFoundError:
        return
    except OSError as error:
        raise ConfigError("socket path cannot be inspected") from error
    if stat.S_ISLNK(socket_metadata.st_mode) or not stat.S_ISSOCK(socket_metadata.st_mode):
        raise ConfigError("socket path must be a non-symlink socket")
    if os.name == "posix" and (
        socket_metadata.st_uid != _effective_ids()[0]
        or socket_metadata.st_gid != _effective_ids()[1]
        or stat.S_IMODE(socket_metadata.st_mode) != socket_mode
    ):
        raise ConfigError("socket owner or mode is invalid")
    if starting:
        raise ConfigError("socket path already exists")


def _parse_config(value: object, *, application_root: Path | None) -> BrokerConfig:
    if not isinstance(value, dict) or set(value) != CONFIG_FIELDS:
        raise ConfigError("broker config has an invalid schema")
    if type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
        raise ConfigError("broker config schema version is invalid")
    socket_value = value.get("socket_path")
    socket_mode_value = value.get("socket_mode")
    if not isinstance(socket_value, str) or not Path(socket_value).is_absolute():
        raise ConfigError("socket path must be absolute")
    if socket_mode_value not in SOCKET_MODES:
        raise ConfigError("socket mode must be 0600 or 0660")
    socket_path = Path(socket_value)
    _validate_socket_parent(socket_path, starting=False)

    channels_value = value.get("channels")
    if not isinstance(channels_value, dict) or set(channels_value) != set(CHANNELS):
        raise ConfigError("channels have an invalid schema")
    channels = {
        channel: _parse_channel(
            channels_value[channel],
            application_root=application_root,
        )
        for channel in CHANNELS
    }
    return BrokerConfig(
        socket_path=socket_path,
        socket_mode=SOCKET_MODES[cast(str, socket_mode_value)],
        channels=channels,
    )


def _load_config(
    path: Path | None = None,
    *,
    application_root: Path | None = None,
) -> BrokerConfig:
    selected = path
    if selected is None:
        configured = os.environ.get(CONFIG_ENV, "").strip()
        if not configured:
            raise ConfigError("broker config is not configured")
        selected = Path(configured)
    if not selected.is_absolute():
        raise ConfigError("broker config path must be absolute")
    selected_application_root = (
        _application_root() if application_root is None else application_root
    )
    if selected_application_root is not None and _is_within(
        selected,
        selected_application_root,
    ):
        raise ConfigError("broker config is inside the application checkout")
    raw = _read_secure_file(selected, limit=MAX_CONFIG_BYTES)
    try:
        value = _strict_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise ConfigError("broker config is not strict JSON") from error
    return _parse_config(
        value,
        application_root=selected_application_root,
    )


def _validate_payload(value: object, *, depth: int = 0, nodes: list[int] | None = None) -> None:
    if nodes is None:
        nodes = [0]
    nodes[0] += 1
    if depth > MAX_PAYLOAD_DEPTH or nodes[0] > MAX_PAYLOAD_NODES:
        raise RequestError("payload exceeds structural limits")
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > 10**18:
            raise RequestError("payload integer is out of range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RequestError("payload number is non-finite")
        return
    if isinstance(value, str):
        if "\x00" in value or len(value.encode("utf-8")) > MAX_PAYLOAD_STRING_BYTES:
            raise RequestError("payload string is invalid")
        return
    if isinstance(value, list):
        for item in value:
            _validate_payload(item, depth=depth + 1, nodes=nodes)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or not key
                or "\x00" in key
                or len(key.encode("utf-8")) > 256
            ):
                raise RequestError("payload key is invalid")
            _validate_payload(item, depth=depth + 1, nodes=nodes)
        return
    raise RequestError("payload contains an unsafe JSON value")


def _parse_request(value: object) -> BrokerRequest:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise RequestError("request has an invalid schema")
    if type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
        raise RequestError("request schema version is invalid")
    channel = value.get("channel")
    action = _safe_name(value.get("action"))
    run_id = _safe_identifier(value.get("run_id"))
    run_nonce = value.get("run_nonce")
    profile_hash = value.get("control_profile_sha256")
    payload = value.get("payload")
    if channel not in CHANNELS:
        raise RequestError("request channel is invalid")
    if action is None or run_id is None:
        raise RequestError("request identity is invalid")
    if not isinstance(run_nonce, str) or NONCE_RE.fullmatch(run_nonce) is None:
        raise RequestError("request nonce is invalid")
    if not isinstance(profile_hash, str) or HEX64_RE.fullmatch(profile_hash) is None:
        raise RequestError("request control profile hash is invalid")
    if not isinstance(payload, dict):
        raise RequestError("request payload must be an object")
    _validate_payload(payload)
    return BrokerRequest(
        channel=cast(str, channel),
        action=action,
        run_id=run_id,
        run_nonce=run_nonce,
        control_profile_sha256=profile_hash.lower(),
        payload=payload,
    )


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _invoke_action(
    action_name: str,
    action: ActionConfig,
    request: BrokerRequest,
) -> dict[str, Any]:
    canonical_request = _canonical_bytes(request.canonical_value())
    if len(canonical_request) > MAX_INPUT_BYTES + 1:
        raise DispatchError("request_too_large")
    try:
        actual_hash = _secure_file_sha256(action.executable, executable=True)
    except ConfigError as error:
        raise DispatchError("handler_failed") from error
    if actual_hash != action.sha256:
        raise DispatchError("handler_hash_mismatch")
    with tempfile.TemporaryFile(mode="w+b") as output:
        try:
            process = subprocess.Popen(  # noqa: S603 - executable and argv are host allowlisted
                [str(action.executable), *action.argv],
                stdin=subprocess.PIPE,
                stdout=output,
                stderr=subprocess.DEVNULL,
                env={ACTION_ENV: action_name},
                shell=False,
                close_fds=True,
            )
        except OSError as error:
            raise DispatchError("handler_failed") from error
        try:
            process.communicate(input=canonical_request, timeout=action.timeout_seconds)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.communicate()
            raise DispatchError("handler_timeout") from error
        if process.returncode != 0:
            raise DispatchError("handler_failed")
        output.seek(0)
        raw = output.read(MAX_OUTPUT_BYTES + 1)
    if len(raw) > MAX_OUTPUT_BYTES:
        raise DispatchError("handler_output_too_large")
    try:
        result = _strict_json(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise DispatchError("handler_output_invalid") from error
    if not isinstance(result, dict):
        raise DispatchError("handler_output_invalid")
    if len(_canonical_bytes(result)) > MAX_OUTPUT_BYTES + 1:
        raise DispatchError("handler_output_too_large")
    return result


def _dispatch(config: BrokerConfig, request: BrokerRequest) -> dict[str, Any]:
    channel = config.channels[request.channel]
    action = channel.allowed_actions.get(request.action)
    if action is None:
        raise DispatchError("action_not_allowed")
    try:
        profile = _read_secure_file(channel.control_profile_file, limit=MAX_PROFILE_BYTES)
    except ConfigError as error:
        raise DispatchError("profile_unavailable") from error
    actual_hash = hashlib.sha256(profile).hexdigest()
    if (
        actual_hash != channel.control_profile_sha256
        or request.control_profile_sha256 != channel.control_profile_sha256
    ):
        raise DispatchError("profile_mismatch")
    return _invoke_action(request.action, action, request)


def _error(error_code: str) -> dict[str, str]:
    return {"status": "not_run", "error_code": error_code}


def _process_request(config: BrokerConfig, value: object) -> dict[str, Any]:
    try:
        request = _parse_request(value)
        response: dict[str, Any] = {"status": "pass", "result": _dispatch(config, request)}
        if len(_canonical_bytes(response)) > MAX_OUTPUT_BYTES + 1:
            return _error("handler_output_too_large")
        return response
    except RequestError:
        return _error("invalid_request")
    except DispatchError as error:
        return _error(error.error_code)
    except Exception:
        return _error("internal_error")


def _process_line(config: BrokerConfig, raw: bytes) -> bytes:
    if len(raw) > MAX_INPUT_BYTES + 1:
        return _canonical_bytes(_error("request_too_large"))
    if not raw.endswith(b"\n"):
        error_code = "request_too_large" if len(raw) > MAX_INPUT_BYTES else "invalid_request"
        return _canonical_bytes(_error(error_code))
    payload = raw[:-1]
    if payload.endswith(b"\r"):
        payload = payload[:-1]
    if len(payload) > MAX_INPUT_BYTES:
        return _canonical_bytes(_error("request_too_large"))
    if b"\n" in payload or b"\r" in payload:
        return _canonical_bytes(_error("invalid_request"))
    try:
        value = _strict_json(payload.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError, RecursionError):
        return _canonical_bytes(_error("invalid_request"))
    return _canonical_bytes(_process_request(config, value))


def _platform_supports_unix_socket() -> bool:
    return sys.platform.startswith("linux") and hasattr(socket, "AF_UNIX")


_UnixServerBase = getattr(socketserver, "UnixStreamServer", socketserver.TCPServer)


class ThreadingUnixStreamServer(
    socketserver.ThreadingMixIn,
    _UnixServerBase,  # type: ignore[misc, valid-type]
):
    """Thread-per-request AF_UNIX server with no request logging."""

    daemon_threads = True
    allow_reuse_address = False
    broker_config: BrokerConfig

    def handle_error(self, request: object, client_address: object) -> None:
        del request, client_address


class _ControlRequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        connection = cast(socket.socket, self.request)
        received = bytearray()
        while len(received) <= MAX_INPUT_BYTES + 1:
            chunk = connection.recv(min(4096, MAX_INPUT_BYTES + 2 - len(received)))
            if not chunk:
                break
            received.extend(chunk)
            if b"\n" in chunk:
                break
        response = _process_line(
            cast(ThreadingUnixStreamServer, self.server).broker_config,
            bytes(received),
        )
        try:
            connection.sendall(response)
        except OSError:
            return


def _check() -> bool:
    if not _platform_supports_unix_socket():
        return False
    try:
        config = _load_config()
        _validate_socket_parent(
            config.socket_path,
            starting=False,
            socket_mode=config.socket_mode,
        )
    except ConfigError:
        return False
    return True


def _serve() -> int:
    if not _platform_supports_unix_socket():
        return 1
    try:
        config = _load_config()
        _validate_socket_parent(
            config.socket_path,
            starting=True,
            socket_mode=config.socket_mode,
        )
    except ConfigError:
        return 1

    server: ThreadingUnixStreamServer | None = None
    socket_identity: tuple[int, int] | None = None
    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def stop_on_sigterm(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGTERM, stop_on_sigterm)
        server = ThreadingUnixStreamServer(str(config.socket_path), _ControlRequestHandler)
        server.broker_config = config
        os.chmod(config.socket_path, config.socket_mode)
        metadata = config.socket_path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISSOCK(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != config.socket_mode
            or metadata.st_uid != _effective_ids()[0]
            or metadata.st_gid != _effective_ids()[1]
        ):
            return 1
        socket_identity = (metadata.st_dev, metadata.st_ino)
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    except OSError:
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        if server is not None:
            server.server_close()
        if socket_identity is not None:
            try:
                metadata = config.socket_path.lstat()
                if (
                    not stat.S_ISLNK(metadata.st_mode)
                    and stat.S_ISSOCK(metadata.st_mode)
                    and (metadata.st_dev, metadata.st_ino) == socket_identity
                ):
                    config.socket_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
    return 0


def _print_status(status: str) -> None:
    sys.stdout.buffer.write(_canonical_bytes({"status": status}))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        ready = _check()
        _print_status("ready" if ready else "not_ready")
        return 0 if ready else 1
    return _serve()


if __name__ == "__main__":
    raise SystemExit(main())
