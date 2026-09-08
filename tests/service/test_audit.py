# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the audit log model, logger and secret masking."""

from __future__ import annotations

import io
import logging
import logging.config
import sys

from trpc_service.log import AuditLogEntry
from trpc_service.log import AuditLogger
from trpc_service.log import SecretMasker
from trpc_service.log import safe_error_message


async def test_audit_entry_fields():
    entry = AuditLogEntry(
        tenant_id="tenant_a",
        channel="wecom",
        user_id="u1",
        session_id="s1",
        message_id="m1",
        turn_id="turn1",
        config_revision=3,
        agent_name="cs_agent",
        tool_name="query_order",
        decision="allow",
        latency_ms=2300,
        error_type=None,
        cost=0.0032,
        trace_id="0af7651916cd43dd8448eb211c80319c",
    )
    assert entry.tenant_id == "tenant_a"
    assert entry.decision == "allow"
    assert entry.tool_name == "query_order"
    assert entry.cost == 0.0032
    assert (entry.message_id, entry.turn_id, entry.config_revision) == ("m1", "turn1", 3)


async def test_audit_logger_append_and_query():
    logger = AuditLogger()
    await logger.log(AuditLogEntry(tenant_id="tenant_a", tool_name="t1", decision="allow"))
    await logger.log(AuditLogEntry(tenant_id="tenant_a", tool_name="t2", decision="deny"))
    await logger.log(AuditLogEntry(tenant_id="tenant_b", tool_name="t1", decision="allow"))

    assert len(logger) == 3

    tenant_a = await logger.query(tenant_id="tenant_a")
    assert {e.tool_name for e in tenant_a} == {"t1", "t2"}

    denied = await logger.query(decision="deny")
    assert len(denied) == 1
    assert denied[0].tool_name == "t2"

    by_tool = await logger.query(tool_name="t1")
    assert {e.tenant_id for e in by_tool} == {"tenant_a", "tenant_b"}


async def test_audit_logger_retains_only_latest_500_entries():
    logger = AuditLogger()
    for index in range(501):
        await logger.log(AuditLogEntry(tenant_id="tenant_a", tool_name=f"tool-{index}"))

    entries = await logger.query()
    assert len(logger) == 500
    assert len(entries) == 500
    assert entries[0].tool_name == "tool-500"
    assert entries[-1].tool_name == "tool-1"


async def test_audit_logger_sink_is_invoked():
    captured = []

    async def sink(entry):
        captured.append(entry)

    logger = AuditLogger(sink=sink)
    await logger.log(AuditLogEntry(tenant_id="tenant_a", decision="allow"))
    assert len(captured) == 1
    assert captured[0].tenant_id == "tenant_a"


async def test_audit_logger_uses_persistent_source():
    requested = []

    async def source(**filters):
        requested.append(filters)
        return [AuditLogEntry(tenant_id="tenant_a", decision="deny")]

    logger = AuditLogger(source=source)
    entries = await logger.query(tenant_id="tenant_a", decision="deny")
    assert entries[0].decision == "deny"
    assert requested[0]["tenant_id"] == "tenant_a"


async def test_sql_audit_sink_persists_and_queries():
    from trpc_service.log import SqlAuditSink

    sink = SqlAuditSink("sqlite+aiosqlite:///:memory:")
    await sink(
        AuditLogEntry(
            tenant_id="tenant_a",
            tool_name="t1",
            decision="deny",
            message_id="message-1",
            turn_id="turn-1",
            config_revision=7,
        ))
    await sink(AuditLogEntry(tenant_id="tenant_b", tool_name="t2", decision="allow"))

    tenant_a = await sink.query(tenant_id="tenant_a")
    assert len(tenant_a) == 1
    assert tenant_a[0].tool_name == "t1"
    assert tenant_a[0].decision == "deny"
    assert tenant_a[0].message_id == "message-1"
    assert tenant_a[0].turn_id == "turn-1"
    assert tenant_a[0].config_revision == 7

    all_records = await sink.query()
    assert len(all_records) == 2
    await sink.close()


async def test_sql_audit_sink_query_entries_with_filters(tmp_path):
    from datetime import datetime, timezone

    from trpc_service.log import SqlAuditSink

    sink = SqlAuditSink(f"sqlite+pysqlite:///{tmp_path / 'audit.db'}", is_async=False)
    await sink(AuditLogEntry(tenant_id="tenant_a", tool_name="lookup", decision="deny", detail={"safe": True}))
    await sink(AuditLogEntry(tenant_id="tenant_a", tool_name="other", decision="allow"))
    await sink(AuditLogEntry(tenant_id="tenant_b", tool_name="lookup", decision="deny"))

    entries = await sink.query_entries(
        tenant_id="tenant_a",
        tool_name="lookup",
        decision="deny",
        since=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert len(entries) == 1
    assert entries[0].detail == {"safe": True}
    assert entries[0].tenant_id == "tenant_a"
    await sink.close()


def test_secret_masker_masks_common_secrets():
    text = "key sk-abcdefghijklmnop and Bearer abc.def and password=secret123"
    out = SecretMasker.mask_value(text)
    assert "sk-abcdefghijklmnop" not in out
    assert "Bearer abc.def" not in out
    assert "secret123" not in out
    assert SecretMasker.mask_value(123) == 123


def test_secret_masker_masks_database_url():
    out = SecretMasker.mask_value("mysql://user:pass@db:3306/mydb")
    assert "user:pass@" not in out


def test_safe_error_message_redacts():

    class Boom(Exception):
        pass

    err = Boom("api_key=sk-abcdefghijklmnop failed")
    out = safe_error_message(err)
    assert "sk-abcdefghijklmnop" not in out


def test_redacting_log_filter_masks_record():
    from trpc_service.log import RedactingLogFilter

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="token=abc123 used",
        args=(),
        exc_info=None,
    )
    f = RedactingLogFilter()
    assert f.filter(record) is True
    assert "abc123" not in record.msg


def test_redacting_log_filter_preserves_uvicorn_access_formatter_arguments():
    from uvicorn.logging import AccessFormatter
    from trpc_service.log import RedactingLogFilter

    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:1234", "POST", "/callback?token=access-secret", "1.1", 200),
        exc_info=None,
    )

    assert RedactingLogFilter().filter(record) is True
    output = AccessFormatter('%(client_addr)s - "%(request_line)s" %(status_code)s').format(record)

    assert len(record.args) == 5
    assert "access-secret" not in output
    assert "POST" in output
    assert "200" in output


def test_redacting_log_filter_masks_parameterized_and_structured_messages():
    from trpc_service.log import RedactingLogFilter

    parameterized = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="token=%s used",
        args=("abc123", ),
        exc_info=None,
    )
    structured = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg={
            "token": "abc123",
            "nested": {
                "password": "db-secret"
            },
            "items": ["api_key=list-secret"],
            "tags": {"Bearer set-secret"},
            "frozen": frozenset({"redis://user:frozen-secret@localhost"}),
        },
        args=(),
        exc_info=None,
    )

    log_filter = RedactingLogFilter()
    assert log_filter.filter(parameterized) is True
    assert parameterized.getMessage() == "token=*** used"
    assert log_filter.filter(structured) is True
    assert "abc123" not in structured.getMessage()
    assert "db-secret" not in structured.getMessage()
    assert "list-secret" not in structured.getMessage()
    assert "set-secret" not in structured.getMessage()
    assert "frozen-secret" not in structured.getMessage()


def test_redacting_log_filter_masks_mapping_arguments_and_existing_exception_text():
    from trpc_service.log import RedactingLogFilter

    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="password=%(password)s",
        args=({
            "password": "mapping-secret"
        }, ),
        exc_info=None,
    )
    record.exc_text = "authorization=existing-exception-secret"

    assert RedactingLogFilter().filter(record) is True
    output = logging.Formatter("%(message)s").format(record)
    assert "mapping-secret" not in output
    assert "existing-exception-secret" not in output


def test_redacting_log_filter_handles_invalid_format_and_recursive_data():
    from trpc_service.log import RedactingLogFilter

    class UnformattableMessage:

        def __str__(self):
            raise RuntimeError("password=object-secret")

    invalid_format = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="token=%d",
        args=("format-secret", ),
        exc_info=None,
    )
    recursive_context = {"password": "recursive-secret"}
    recursive_context["self"] = recursive_context
    structured = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=recursive_context,
        args=(),
        exc_info=None,
    )
    unformattable = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=UnformattableMessage(),
        args=(),
        exc_info=None,
    )

    log_filter = RedactingLogFilter()
    assert log_filter.filter(invalid_format) is True
    assert invalid_format.getMessage() == "token=***"
    assert "format-secret" not in invalid_format.getMessage()
    assert log_filter.filter(structured) is True
    assert "recursive-secret" not in structured.getMessage()
    assert "<recursive>" in structured.getMessage()
    assert log_filter.filter(unformattable) is True
    assert unformattable.getMessage() == "<unformattable log message>"


def test_redacting_log_filter_masks_exception_and_stack_text():
    from trpc_service.log import RedactingLogFilter

    try:
        raise RuntimeError("password=exception-secret")
    except RuntimeError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="request failed",
        args=(),
        exc_info=exc_info,
    )
    record.stack_info = "authorization=stack-secret"

    assert RedactingLogFilter().filter(record) is True
    output = logging.Formatter("%(message)s").format(record)
    assert "exception-secret" not in output
    assert "stack-secret" not in output
    assert "password=***" in output
    assert "authorization=***" in output


def test_install_redacting_log_filter_covers_dynamic_non_propagating_logger():
    from trpc_service.log import RedactingLogFilter
    from trpc_service.log import install_redacting_log_filter

    original_factory = logging.getLogRecordFactory()
    root = logging.getLogger()
    original_root_filters = list(root.filters)
    stream = io.StringIO()

    def custom_factory(*args, **kwargs):
        record = original_factory(*args, **kwargs)
        record.factory_marker = "custom-factory-kept"
        return record

    try:
        logging.setLogRecordFactory(custom_factory)
        install_redacting_log_filter()
        installed_factory = logging.getLogRecordFactory()
        install_redacting_log_filter()

        assert logging.getLogRecordFactory() is installed_factory
        assert sum(isinstance(item, RedactingLogFilter) for item in root.filters) == 1

        # Configure this handler after installation and disable propagation.
        # A root-logger-only filter cannot see this record.
        logging.config.dictConfig({
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "capture": {
                    "format": "%(factory_marker)s %(message)s token=%(token)s context=%(context)s",
                },
            },
            "handlers": {
                "capture": {
                    "class": "logging.StreamHandler",
                    "formatter": "capture",
                    "stream": stream,
                },
            },
            "loggers": {
                "enterprise.audit.dynamic": {
                    "handlers": ["capture"],
                    "level": "INFO",
                    "propagate": False,
                },
            },
        })
        logger = logging.getLogger("enterprise.audit.dynamic")
        logger.info(
            "authorization=%s",
            "message-secret",
            extra={
                "token": "extra-secret",
                "context": {
                    "password": "nested-secret"
                }
            },
        )

        output = stream.getvalue()
        assert "custom-factory-kept" in output
        assert "message-secret" not in output
        assert "extra-secret" not in output
        assert "nested-secret" not in output
        assert "authorization=***" in output
    finally:
        dynamic_logger = logging.getLogger("enterprise.audit.dynamic")
        for handler in list(dynamic_logger.handlers):
            dynamic_logger.removeHandler(handler)
            handler.close()
        logging.setLogRecordFactory(original_factory)
        root.filters[:] = original_root_filters
