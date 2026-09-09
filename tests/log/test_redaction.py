"""Regression tests ensuring credentials never reach structured logs."""

from __future__ import annotations

import json
import logging

from trpc_service.log import bind_log_context, configure_logging, redact


def test_recursive_redaction_is_bounded_and_key_aware() -> None:
    result = redact(
        {
            "authorization": "Bearer top-secret",
            "nested": {"api_key": "abc", "safe": "visible"},
            "url": "https://example.test/hook?token=abc&mode=1",
            "callback_url": (
                "https://example.test/callback?msg_signature=signed-value"
                "&timestamp=1&nonce=callback-nonce&echostr=encrypted-echo"
            ),
            "database": "postgresql+asyncpg://runtime:db-password@db.internal/agent",
            "blob": b"never-print-me",
        }
    )

    assert result["authorization"] == "[REDACTED]"
    assert result["nested"] == {"api_key": "[REDACTED]", "safe": "visible"}
    assert "abc" not in result["url"]
    assert "signed-value" not in result["callback_url"]
    assert "callback-nonce" not in result["callback_url"]
    assert "encrypted-echo" not in result["callback_url"]
    assert "db-password" not in result["database"]
    assert result["database"].endswith("[REDACTED]@db.internal/agent")
    assert result["blob"] == "<bytes:14>"


def test_json_handler_redacts_message_and_extra(capsys) -> None:
    root = logging.getLogger()
    previous_handlers, previous_level = list(root.handlers), root.level
    try:
        configure_logging("INFO")
        with bind_log_context(tenant_id="tenant-a", request_id="req-1", trace_id="trace-1"):
            logging.getLogger("redaction-test").info(
                "authorization Bearer raw-token",
                extra={"api_key": "raw-api-key", "outcome": "ok"},
            )
        record = json.loads(capsys.readouterr().err)
        assert "raw-token" not in record["message"]
        assert record["api_key"] == "[REDACTED]"
        assert record["request_id"] == "req-1"
        assert record["trace_id"] == "trace-1"
    finally:
        root.handlers.clear()
        root.handlers.extend(previous_handlers)
        root.setLevel(previous_level)


def test_exception_messages_and_tracebacks_do_not_leave_process(capsys) -> None:
    root = logging.getLogger()
    previous_handlers, previous_level = list(root.handlers), root.level
    try:
        configure_logging("INFO")
        try:
            raise RuntimeError("upstream failed with token=do-not-export")
        except RuntimeError:
            logging.getLogger("redaction-test").exception("upstream_failure")
        rendered = capsys.readouterr().err
        record = json.loads(rendered)
        assert "do-not-export" not in rendered
        assert record["exception_type"] == "RuntimeError"
    finally:
        root.handlers.clear()
        root.handlers.extend(previous_handlers)
        root.setLevel(previous_level)
