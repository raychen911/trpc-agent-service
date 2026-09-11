"""Stage 6B1 Task 1 — safe structured logging boundary tests.

Proves the field whitelist, single-line JSON output, control-char escaping,
length caps, defensive blocking of third-party secret-bearing records, and
idempotent root/SDK logger configuration required by the design spec.
"""

import json
import logging
import math
import typing
from io import StringIO

import pytest
import uvicorn
from trpc_service import _cli
from trpc_service.log import SafeLogFields, SensitiveDataFilter, configure_logging, safe_log
from trpc_service.log.config import SafeLogStreamHandler, THIRD_PARTY_LOGGER_NAMES
from trpc_service.log.safe import MAX_FIELD_VALUE_CHARS, SAFE_LOG_FIELD_NAMES

# Obviously-fake sentinel values.  None of them may ever appear in captured
# stdout/stderr/caplog output when logging goes through safe_log or the filter.
API_KEY_SENTINEL = "sk-FAKESENTINELAPIKEY0123456789"
BEARER_SENTINEL = "eyJFAKESENTINELBEARERTOKENabcdef"
DSN_SENTINEL = "postgresql://svc:FAKESENTINELPASSWORD@db.internal:5432/app"
BODY_SENTINEL = "FAKESENTINELBODYUSERTEXT"

ALL_SENTINELS = (API_KEY_SENTINEL, BEARER_SENTINEL, DSN_SENTINEL, BODY_SENTINEL)

# Keyword-free sentinels (Codex review): no regex heuristic may be trusted to
# recognise these — taken-over loggers must be fail-closed dropped entirely.
PLAIN_BODY_SENTINEL = "PLAIN_USER_BODY_SENTINEL_6B1"
PLAIN_QUERY_SENTINEL = "PLAIN_QUERY_SENTINEL_6B1"
LOG_LEVEL_ENV = "TRPC_LOG_LEVEL"

WHITELIST = {
    "service",
    "event",
    "trace_id",
    "span_id",
    "request_id",
    "tenant_id",
    "channel",
    "operation",
    "status",
    "error_code",
    "exception_type",
    "duration_ms",
    "worker_endpoint",
}

# Third-party log records that MUST be blocked by SensitiveDataFilter.
SENSITIVE_RECORDS = [
    "provider request failed, headers include {'api_key': '" + API_KEY_SENTINEL + "'}",
    "GET https://api.example/v1 Authorization: Bearer " + BEARER_SENTINEL,
    "cannot reach database at " + DSN_SENTINEL,
    "inbound payload " + repr({
        "text": "hello " + BODY_SENTINEL,
        "msg_id": "m-1"
    }),
]

# Names this task must take over for uvicorn (design spec + Codex review):
# the access logger is fail-closed dropped (its lines carry request paths and
# query strings); the remaining uvicorn loggers keep their fixed operational
# text but only via the safe root handler — never their own StreamHandlers.
UVICORN_LOGGER_NAMES = ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi")
TAKEOVER_LOGGER_NAMES = tuple(THIRD_PARTY_LOGGER_NAMES) + UVICORN_LOGGER_NAMES


@pytest.fixture(autouse=True)
def restore_logging_state():
    """Undo every global mutation configure_logging (and dictConfig) makes, per test."""
    manager = logging.Logger.manager
    all_disabled = {
        name: getattr(obj, "disabled", None)
        for name, obj in manager.loggerDict.items() if hasattr(obj, "disabled")
    }
    root = logging.getLogger()
    root_snapshot = (list(root.handlers), root.level, list(root.filters))
    factory_before = logging.getLogRecordFactory()
    third_party = {}
    for name in TAKEOVER_LOGGER_NAMES:
        logger = logging.getLogger(name)
        third_party[name] = (
            list(logger.handlers),
            logger.level,
            list(logger.filters),
            logger.propagate,
            logger.disabled,
            "addHandler" in logger.__dict__,
            "removeHandler" in logger.__dict__,
        )
    yield
    logging.setLogRecordFactory(factory_before)  # never leak the safe factory
    handlers, level, filters = root_snapshot
    root.handlers[:] = handlers
    root.setLevel(level)
    root.filters[:] = filters
    for name, state in third_party.items():
        handlers, level, filters, propagate, disabled, had_add_guard, had_remove_guard = state
        logger = logging.getLogger(name)
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.filters[:] = filters
        logger.propagate = propagate
        logger.disabled = disabled
        if not had_add_guard:
            logger.__dict__.pop("addHandler", None)
        if not had_remove_guard:
            logger.__dict__.pop("removeHandler", None)
    for name, disabled in all_disabled.items():
        obj = manager.loggerDict.get(name)
        if obj is not None and disabled is not None:
            obj.disabled = disabled


def _configure(service_name="test-service", environ=None):
    configure_logging(service_name, {} if environ is None else environ)


def _emitted_json(capsys):
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.splitlines()
    assert lines, "expected at least one log line on stderr"
    return [json.loads(line) for line in lines], captured


def test_safe_log_fields_whitelist_matches_spec():
    assert set(SafeLogFields.__annotations__) == WHITELIST
    assert SAFE_LOG_FIELD_NAMES == frozenset(WHITELIST)


def test_configure_logging_is_idempotent(capsys):
    _configure()
    _configure()
    root = logging.getLogger()
    assert sum(isinstance(h, SafeLogStreamHandler) for h in root.handlers) == 1
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.once")
    parsed, _captured = _emitted_json(capsys)
    assert len(parsed) == 1  # one configure, one handler, one line


def test_configure_logging_rebinds_service_name(capsys):
    _configure("svc-a")
    _configure("svc-b")
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.rebind")
    parsed, _ = _emitted_json(capsys)
    assert [line["service"] for line in parsed] == ["svc-b"]


def test_safe_log_emits_single_line_json_with_whitelisted_fields(capsys):
    _configure()
    logger = logging.getLogger("trpc_service.test")
    safe_log(
        logger,
        logging.WARN,
        "gateway.dispatch",
        trace_id="0" * 32,
        span_id="1" * 16,
        request_id="req-1",
        tenant_id="tenant-a",
        channel="wecom",
        operation="dispatch",
        status="ok",
        duration_ms=12.5,
        worker_endpoint="http://127.0.0.1:8001",
    )
    parsed, captured = _emitted_json(capsys)
    assert len(parsed) == 1
    line = parsed[0]
    assert line["service"] == "test-service"
    assert line["level"] == "WARNING"
    assert line["event"] == "gateway.dispatch"
    assert line["tenant_id"] == "tenant-a"
    assert line["duration_ms"] == 12.5
    assert set(line) <= WHITELIST | {"ts", "level"}
    assert captured.err.count("\n") == 1


def test_safe_log_rejects_unknown_fields(capsys):
    _configure()
    logger = logging.getLogger("trpc_service.test")
    for bad in ("prompt", "response", "tool_arguments", "authorization", "headers"):
        with pytest.raises(ValueError):
            safe_log(logger, logging.INFO, "probe.rejected", **{bad: "x"})
    safe_log(logger, logging.INFO, "probe.accepted")
    parsed, captured = _emitted_json(capsys)
    assert [line["event"] for line in parsed] == ["probe.accepted"]
    for name in ("prompt", "response", "tool_arguments", "headers"):
        assert name not in captured.err


def test_safe_log_rejects_container_and_object_values(capsys):
    _configure()
    logger = logging.getLogger("trpc_service.test")
    for value in ({"a": 1}, ["a"], ("a", ), {1, 2}, object(), b"bytes"):
        with pytest.raises(TypeError):
            safe_log(logger, logging.INFO, "probe.bad_value", status=value)
    safe_log(logger, logging.INFO, "probe.ok", status="ok", tenant_id="tenant-a", trace_id="t")
    parsed, _ = _emitted_json(capsys)
    assert len(parsed) == 1


def test_safe_log_rejects_non_numeric_duration():
    _configure()
    logger = logging.getLogger("trpc_service.test")
    for value in ("12", True, float("nan"), float("inf"), math.inf):
        with pytest.raises(TypeError):
            safe_log(logger, logging.INFO, "probe.bad_duration", duration_ms=value)


def test_safe_log_escapes_control_chars_and_ansi(capsys):
    _configure()
    nasty = "line1\nline2\ttabbed \x1b[31mred\x1b[0m \x00\x7f end"
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.escape", operation=nasty)
    captured = capsys.readouterr()
    assert "\x1b" not in captured.err
    assert "\x7f" not in captured.err
    assert "\x00" not in captured.err
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1  # single line despite newline/ANSI/NUL input
    line = json.loads(lines[0])
    # the decoded value itself is safe: no raw control characters survive
    assert "\n" not in line["operation"]
    assert "\t" not in line["operation"]
    assert "\x00" not in line["operation"]
    assert "\x1b" not in line["operation"]
    assert "line1" in line["operation"]


def test_safe_log_truncates_overlong_values(capsys):
    _configure()
    long_value = "A" * (MAX_FIELD_VALUE_CHARS * 4)
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.truncate", operation=long_value)
    parsed, _ = _emitted_json(capsys)
    assert len(parsed[0]["operation"]) <= MAX_FIELD_VALUE_CHARS
    assert parsed[0]["operation"].endswith("[truncated]")


def test_third_party_secret_records_are_blocked(capsys, caplog):
    caplog.set_level(logging.DEBUG)
    _configure()
    for name in THIRD_PARTY_LOGGER_NAMES:
        for message in SENSITIVE_RECORDS:
            logging.getLogger(name).warning(message)
    logging.getLogger("trpc_service.test").warning("probe.harmless_boundary")
    parsed, captured = _emitted_json(capsys)
    for sentinel in ALL_SENTINELS:
        assert sentinel not in captured.out
        assert sentinel not in captured.err
        assert sentinel not in caplog.text
    assert len(parsed) == 1
    assert parsed[0]["message"] == "probe.harmless_boundary"


def test_root_logger_secret_records_are_blocked(capsys, caplog):
    caplog.set_level(logging.DEBUG)
    _configure()
    for message in SENSITIVE_RECORDS:
        logging.getLogger().warning(message)
    captured = capsys.readouterr()
    for sentinel in ALL_SENTINELS:
        assert sentinel not in captured.err
        assert sentinel not in caplog.text


def test_exception_details_reduced_to_type_only(capsys, caplog):
    """Non-takeover foreign records: exception detail reduced to the type name.

    Taken-over SDK loggers must be dropped entirely (covered by the plain-body
    sentinel tests below), so this uses an ordinary foreign logger name.
    """
    caplog.set_level(logging.DEBUG)
    _configure()
    logger = logging.getLogger("some_other_library")
    try:
        raise RuntimeError(f"provider call failed for api_key={API_KEY_SENTINEL} dsn={DSN_SENTINEL}")
    except RuntimeError:
        logger.exception("call failed")
    logger.error("provider failed: %s", RuntimeError(f"token={BEARER_SENTINEL}"))
    parsed, captured = _emitted_json(capsys)
    for sentinel in ALL_SENTINELS:
        assert sentinel not in captured.out
        assert sentinel not in captured.err
        assert sentinel not in caplog.text
    assert "Traceback" not in captured.err
    assert "Traceback" not in caplog.text
    assert len(parsed) == 1  # the %-interpolated record was blocked outright
    assert parsed[0]["message"] == "call failed"
    assert parsed[0]["exception_type"] == "RuntimeError"


def test_safe_log_secret_bearing_values_are_blocked(capsys):
    _configure()
    logger = logging.getLogger("trpc_service.test")
    safe_log(logger, logging.INFO, "probe.leaky", operation=f"connect {DSN_SENTINEL}")
    safe_log(logger, logging.INFO, "probe.leaky2", error_code=f"auth failed: Bearer {BEARER_SENTINEL}")
    safe_log(logger, logging.INFO, "probe.clean", status="ok")
    parsed, captured = _emitted_json(capsys)
    for sentinel in ALL_SENTINELS:
        assert sentinel not in captured.err
    assert [line["event"] for line in parsed] == ["probe.clean"]


def test_filter_never_stores_or_echoes_original():
    data_filter = SensitiveDataFilter()
    secret = SENSITIVE_RECORDS[0]
    blocked = logging.LogRecord("Lark", logging.WARNING, __file__, 1, secret, None, None)
    assert data_filter.filter(blocked) is False
    state = " ".join(str(value) for value in vars(data_filter).values())
    for sentinel in ALL_SENTINELS:
        assert sentinel not in state
        assert sentinel not in str(vars(blocked))
    assert data_filter.blocked_count == 1
    harmless = logging.LogRecord("Lark", logging.WARNING, __file__, 1, "harmless", None, None)
    assert data_filter.filter(harmless) is True


def test_late_third_party_stdout_handler_is_refused(capsys):
    _configure()
    leaky_stream = StringIO()
    logging.getLogger("Lark").addHandler(logging.StreamHandler(leaky_stream))
    logging.getLogger("Lark").warning("sdk handshake retry 1")
    captured = capsys.readouterr()
    # Lark raw logs are fail-closed dropped: the late handler was never
    # installed AND nothing is forwarded to the safe root handler either.
    assert leaky_stream.getvalue() == ""
    assert captured.out == ""
    assert captured.err == ""


def test_takeover_loggers_are_fail_closed():
    _configure()
    for name in THIRD_PARTY_LOGGER_NAMES + ("uvicorn.access", ):
        logger = logging.getLogger(name)
        assert logger.disabled is True, name
        assert logger.propagate is False, name
        assert all(not isinstance(h, logging.StreamHandler) for h in logger.handlers), name
    for name in ("uvicorn", "uvicorn.error", "uvicorn.asgi"):
        logger = logging.getLogger(name)
        assert logger.propagate is True, name
        assert all(not isinstance(h, logging.StreamHandler) for h in logger.handlers), name


def test_repeated_configure_keeps_drop_guarantees(capsys):
    """Late SDK load / repeated configure / handler-close keep the boundary."""
    _configure()
    _configure()
    _configure("test-service", {LOG_LEVEL_ENV: "DEBUG"})
    logging.getLogger("trpc_agent_sdk").warning(BODY_SENTINEL + " late-load")
    leaky = StringIO()
    logging.getLogger("aibot").handlers.append(logging.StreamHandler(leaky))
    logging.getLogger("aibot").warning(BODY_SENTINEL + " direct-append")
    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler, SafeLogStreamHandler):
            handler.close()
    _configure()
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.after_close")
    captured = capsys.readouterr()
    assert BODY_SENTINEL not in captured.out
    assert BODY_SENTINEL not in captured.err
    assert leaky.getvalue() == ""
    assert "probe.after_close" in captured.err


# ---------------------------------------------------------------------------
# CLI integration: configure_logging runs before gateway/worker/admin startup.
# ---------------------------------------------------------------------------

CLI_COMMANDS = [("gateway", "gateway"), ("web", "gateway"), ("worker", "worker"), ("admin", "admin")]


@pytest.mark.parametrize("command,service", CLI_COMMANDS)
def test_cli_configures_logging_before_server_start(monkeypatch, command, service):
    monkeypatch.delenv("TRPC_LOG_LEVEL", raising=False)
    configured = {}

    def fake_runner(app_path, **kwargs):
        root = logging.getLogger()
        configured["installed"] = any(isinstance(h, SafeLogStreamHandler) for h in root.handlers)
        configured["service"] = next(h.formatter._service for h in root.handlers if isinstance(h, SafeLogStreamHandler))

    result = _cli.main([command, "--host", "127.0.0.1", "--port", "8001"], server_runner=fake_runner)
    assert result == 0
    assert configured == {"installed": True, "service": service}


def test_cli_non_serving_commands_do_not_configure_logging():
    root = logging.getLogger()
    before = sum(isinstance(h, SafeLogStreamHandler) for h in root.handlers)
    stdout, stderr = StringIO(), StringIO()
    result = _cli.main(
        ["model-config-check"],
        environ={
            "TRPC_MODEL_NAME": "deepseek-chat",
            "TRPC_MODEL_API_KEY": "k"
        },
        stdout=stdout,
        stderr=stderr,
    )
    assert result == 0
    after = sum(isinstance(h, SafeLogStreamHandler) for h in root.handlers)
    assert after == before


# ---------------------------------------------------------------------------
# Codex review round 1: fail-closed takeover and uvicorn bypass closure.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "logger_name",
    [
        "trpc_agent_sdk",
        "trpc_agent_sdk.runners",  # sub-loggers must not smuggle records to root
        "aibot",
        "Lark",
        "Lark.EventDispatcher",
        "opentelemetry.exporter.otlp.proto.http.trace_exporter",
        "uvicorn.access",
    ],
)
def test_plain_body_sentinel_dropped_from_takeover_tree(capsys, caplog, logger_name):
    """Keyword-free user-body sentinels: regexes cannot see them — drop all."""
    caplog.set_level(logging.DEBUG)
    _configure()
    logging.getLogger(logger_name).warning(PLAIN_BODY_SENTINEL)
    logging.getLogger(logger_name).info("inbound %s", PLAIN_BODY_SENTINEL + "-args")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert PLAIN_BODY_SENTINEL not in caplog.text


def test_direct_handler_append_still_dropped(capsys):
    """Bypassing the addHandler refusal must still emit nothing anywhere.

    Guards the guarantee that the fail-closed boundary is ``disabled`` +
    ``propagate=False``, not the instance-level refusal stub.
    """
    _configure()
    leaky = StringIO()
    for name in THIRD_PARTY_LOGGER_NAMES + ("uvicorn.access", ):
        logger = logging.getLogger(name)
        logger.handlers.append(logging.StreamHandler(leaky))
        logger.warning(PLAIN_BODY_SENTINEL + " bypass-" + name)
    captured = capsys.readouterr()
    assert leaky.getvalue() == ""
    assert PLAIN_BODY_SENTINEL not in captured.out
    assert PLAIN_BODY_SENTINEL not in captured.err


def test_configure_logging_replaces_all_root_handlers(capsys):
    """A pre-existing raw root handler must not keep emitting in parallel."""
    raw_stream = StringIO()
    root = logging.getLogger()
    root.addHandler(logging.StreamHandler(raw_stream))
    _configure()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0], SafeLogStreamHandler)
    logging.getLogger().warning("harmless operational line pid-42")
    assert raw_stream.getvalue() == ""
    parsed, _ = _emitted_json(capsys)
    assert len(parsed) == 1
    assert parsed[0]["message"] == "harmless operational line pid-42"


async def _dummy_asgi_app(scope, receive, send):
    return None


def test_uvicorn_safe_config_probe_closes_bypass(capsys):
    """Real uvicorn.Config.configure_logging() with the CLI's safe settings.

    Proves that after ``configure_logging`` the server startup cannot install
    any output path that bypasses SafeLogStreamHandler, and that access/query
    text never surfaces while operational startup lines do.
    """
    _configure("gateway")
    root = logging.getLogger()
    before = list(root.handlers)
    config = uvicorn.Config(
        _dummy_asgi_app,
        host="127.0.0.1",
        port=0,
        log_config=None,
        access_log=False,
    )
    config.configure_logging()
    assert root.handlers == before
    assert len(root.handlers) == 1 and isinstance(root.handlers[0], SafeLogStreamHandler)
    logging.getLogger("uvicorn").error("GET / Authorization: Bearer %s", BEARER_SENTINEL)
    logging.getLogger("uvicorn.access").info("GET /search?%s=1 HTTP/1.1", PLAIN_QUERY_SENTINEL)
    logging.getLogger("uvicorn.access").warning("chat body says %s", PLAIN_BODY_SENTINEL)
    logging.getLogger("uvicorn.error").info("Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)")
    captured = capsys.readouterr()
    for sentinel in (PLAIN_QUERY_SENTINEL, PLAIN_BODY_SENTINEL, BEARER_SENTINEL):
        assert sentinel not in captured.out
        assert sentinel not in captured.err
    assert "Uvicorn running" in captured.err  # operational line stays visible, via safe handler only
    json.loads(captured.err.strip().splitlines()[-1])  # and it is the safe single-line JSON


def test_hostile_default_dictconfig_then_reconfigure_closes_bypass(capsys):
    """Even if uvicorn's default dictConfig ran (raw handlers everywhere),
    re-applying the boundary leaves no bypass."""
    from uvicorn.config import LOGGING_CONFIG

    config = uvicorn.Config(_dummy_asgi_app, host="127.0.0.1", port=0, log_config=dict(LOGGING_CONFIG))
    config.configure_logging()  # hostile: replaces root handlers with raw StreamHandlers
    _configure("gateway")
    root = logging.getLogger()
    assert len(root.handlers) == 1 and isinstance(root.handlers[0], SafeLogStreamHandler)
    logging.getLogger("uvicorn.access").info("GET /?%s=1 HTTP/1.1", PLAIN_QUERY_SENTINEL)
    logging.getLogger("uvicorn").error("Authorization: Bearer %s", BEARER_SENTINEL)
    logging.getLogger("trpc_agent_sdk").warning(PLAIN_BODY_SENTINEL)
    logging.getLogger("uvicorn.error").info("Finished server process [4242]")
    captured = capsys.readouterr()
    for sentinel in (PLAIN_QUERY_SENTINEL, PLAIN_BODY_SENTINEL, BEARER_SENTINEL):
        assert sentinel not in captured.out
        assert sentinel not in captured.err
    assert "Finished server process" in captured.err
    json.loads(captured.err.strip().splitlines()[-1])


def test_safe_logfields_runtime_typeddict_consistency():
    """Runtime shape of SafeLogFields must match the whitelist declaration."""
    assert typing.is_typeddict(SafeLogFields)
    # Resolve under ``from __future__ import annotations`` (ForwardRef form).
    annotations = typing.get_type_hints(SafeLogFields)
    assert set(annotations) == set(SAFE_LOG_FIELD_NAMES)
    assert not SafeLogFields.__required_keys__  # every field optional at the call site
    assert annotations["duration_ms"] is float
    assert all(annotations[name] is str for name in annotations if name != "duration_ms")


def test_all_whitelisted_fields_round_trip(capsys):
    _configure()
    fields = {name: ("v-1" if name != "duration_ms" else 1.5) for name in SAFE_LOG_FIELD_NAMES}
    fields.pop("event")  # event is the positional argument, never a kwarg
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.fields", **fields)
    parsed, _ = _emitted_json(capsys)
    assert len(parsed) == 1
    for name, value in fields.items():
        assert parsed[0][name] == value, name


# Exactly the fixed-text lines Stage 6A2's acceptance script greps for:
# they come from PRODUCT loggers and must survive every takeover.
FIXED_OBSERVABILITY_LINES = [
    (
        "trpc_service.channels.wecom.service",
        "WeCom AI Bot authenticated and started (channel=wecom)",
    ),
    (
        "trpc_service.worker.approval_service",
        "approval request created (tenant=tenant_default, tool=get_current_time, state=pending)",
    ),
    (
        "trpc_service.worker.approval_service",
        "approval executed (tenant=tenant_default, tool=get_current_time, decision=approve)",
    ),
    (
        "trpc_service.channels.wecom.service",
        "WeCom reply chain completed (terminal=done, channel=wecom)",
    ),
]


def test_fixed_product_observability_lines_remain_visible(capsys):
    _configure("gateway")
    for name, text in FIXED_OBSERVABILITY_LINES:
        logging.getLogger(name).warning(text)
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = [json.loads(line) for line in captured.err.splitlines()]
    assert len(lines) == len(FIXED_OBSERVABILITY_LINES)
    for line in lines:
        assert line["level"] == "WARNING"
        assert line["service"] == "gateway"


@pytest.mark.parametrize("command,app_path", [
    ("gateway", "trpc_service.gateway.app:create_gateway_app"),
    ("web", "trpc_service.gateway.app:create_gateway_app"),
    ("worker", "trpc_service.worker.app:create_worker_app"),
    ("admin", "trpc_service.admin.app:create_admin_app"),
])
def test_cli_starts_uvicorn_with_safe_logging(monkeypatch, command, app_path):
    """uvicorn.run(log_config=None) secretly re-applies the default dictConfig;
    the CLI must therefore go through Config(log_config=None, access_log=False)."""
    calls = {}

    class FakeConfig:

        def __init__(self, app, **kwargs):
            calls["config"] = (app, kwargs)

    class FakeServer:

        def __init__(self, config):
            calls["server"] = True

        def run(self):
            calls["ran"] = True

    monkeypatch.setattr(uvicorn, "Config", FakeConfig)
    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    result = _cli.main([command, "--host", "127.0.0.1", "--port", "8123"])
    assert result == 0
    assert calls.get("ran") is True
    app, kwargs = calls["config"]
    assert app == app_path
    assert kwargs["host"] == "127.0.0.1"
    assert kwargs["port"] == 8123
    assert kwargs["factory"] is True
    assert kwargs["log_config"] is None
    assert kwargs["access_log"] is False
    assert any(isinstance(h, SafeLogStreamHandler) for h in logging.getLogger().handlers)


# ---------------------------------------------------------------------------
# Codex review round 2: creation-time scrub of dropped-subtree records (P1)
# and strict runtime field types for safe_log (P2).
# ---------------------------------------------------------------------------

DROPPED_CHILDREN = (
    "trpc_agent_sdk.runners",
    "Lark.EventDispatcher",
    "aibot.connection",
    "opentelemetry.exporter.otlp.proto.http.trace_exporter",
    "uvicorn.access.ghost",
)


def test_direct_child_handler_append_is_scrubbed(capsys, caplog):
    """Sub-loggers created *after* configuration bypass disabled/propagate/
    NullHandler/refusal entirely: Logger.disabled only gates the originating
    logger and callHandlers hits the child's own handlers first.  The safe
    LogRecordFactory must therefore scrub dropped-subtree records at creation
    time, replacing them with a fixed safe category before any handler runs.
    """
    caplog.set_level(logging.DEBUG)
    _configure()
    leaky = StringIO()
    raw = logging.StreamHandler(leaky)
    raw.setFormatter(logging.Formatter("%(message)s"))
    messages = {child: PLAIN_BODY_SENTINEL + "--" + child for child in DROPPED_CHILDREN}
    for child in messages:
        logging.getLogger(child).handlers.append(raw)
    try:
        for child, msg in messages.items():
            logging.getLogger(child).warning(msg)
        out = leaky.getvalue()
    finally:
        for child in messages:
            logger = logging.getLogger(child)
            if raw in logger.handlers:
                logger.handlers.remove(raw)
    captured = capsys.readouterr()
    for msg in messages.values():
        assert msg not in out
        assert msg not in captured.out
        assert msg not in captured.err
        assert msg not in caplog.text
    # The raw handler demonstrably ran — it only ever saw the fixed category.
    assert out.strip()
    assert PLAIN_BODY_SENTINEL not in out


def test_record_factory_is_chained_and_idempotent(capsys):
    """Repeated configure never double-wraps, and product records keep their
    fields (the factory chains to, not replaces, the previous one)."""
    _configure()
    factory = logging.getLogRecordFactory()
    _configure("other-service")
    assert logging.getLogRecordFactory() is factory
    logging.getLogger("trpc_service.test").warning("operational pid-1")
    parsed, _ = _emitted_json(capsys)
    assert parsed[-1]["message"] == "operational pid-1"


# ---------------------------------------------------------------------------
# P2 — runtime type enforcement for every whitelisted field.
# ---------------------------------------------------------------------------

STR_ONLY_FIELDS = sorted(SAFE_LOG_FIELD_NAMES - {"event", "duration_ms"})
BAD_SCALARS = (None, 1, 1.5, True, False, [1], {"a": 1}, ("x", ), b"x")


@pytest.mark.parametrize("field", STR_ONLY_FIELDS)
def test_str_fields_reject_every_non_str_at_runtime(field, capsys):
    _configure()
    logger = logging.getLogger("trpc_service.test")
    safe_log(logger, logging.INFO, "probe.typed", **{field: "ok"})
    for bad in BAD_SCALARS:
        with pytest.raises(TypeError):
            safe_log(logger, logging.INFO, "probe.typed", **{field: bad})


@pytest.mark.parametrize("value", [0, 1, 12.5, 1e9])
def test_duration_ms_accepts_finite_numbers(value, capsys):
    _configure()
    safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.dur", duration_ms=value)
    parsed, _ = _emitted_json(capsys)
    assert parsed[-1]["duration_ms"] == value


@pytest.mark.parametrize("value", [None, True, False, "12.5", float("nan"), float("inf"), [1]])
def test_duration_ms_rejects_all_other_values(value, capsys):
    _configure()
    with pytest.raises(TypeError):
        safe_log(logging.getLogger("trpc_service.test"), logging.INFO, "probe.dur", duration_ms=value)
