import logging
import json

from trpc_service.log import (
    SafeLogFormatter,
    SafeUvicornAccessFormatter,
    SafeUvicornDefaultFormatter,
    SensitiveDataRedactor,
    JsonLogFormatter,
    bind_log_context,
)


def test_redactor_removes_secrets_and_pii_from_nested_payloads() -> None:
    redactor = SensitiveDataRedactor()
    payload = {
        "authorization": "Bearer runtime-secret-token",
        "Set-Cookie": "session=plain-cookie-secret",
        "nested": {
            "api_key": "sk-example-secret-123456",
            "message": "联系 alice@example.com 或 13800138000",
        },
    }

    redacted = redactor.redact_mapping(payload, redact_pii=True)

    assert redacted["authorization"] == "[REDACTED]"
    assert redacted["Set-Cookie"] == "[REDACTED]"
    assert redacted["nested"] == {
        "api_key": "[REDACTED]",
        "message": "联系 [REDACTED_EMAIL] 或 [REDACTED_PHONE]",
    }
    assert "runtime-secret-token" not in str(redacted)
    assert "sk-example-secret" not in str(redacted)


def test_safe_log_formatter_redacts_secrets_after_message_rendering() -> None:
    """Credentials are removed even when a dependency logs formatted arguments."""

    record = logging.LogRecord(
        "test",
        logging.ERROR,
        __file__,
        1,
        "request failed: %s",
        ("Bearer runtime-secret-token", ),
        None,
    )

    rendered = SafeLogFormatter("%(levelname)s %(message)s").format(record)

    assert rendered == "ERROR request failed: Bearer [REDACTED]"
    assert "runtime-secret-token" not in rendered


def test_safe_uvicorn_formatters_keep_native_fields_and_redact() -> None:
    """Server and access formatters retain Uvicorn fields without leaking data."""

    server_record = logging.LogRecord(
        "uvicorn.error",
        logging.INFO,
        __file__,
        1,
        "started with %s",
        ("sk-example-secret-123456", ),
        None,
    )
    access_record = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1", "GET", "/?token=plain-runtime-secret", "1.1", 200),
        None,
    )

    server = SafeUvicornDefaultFormatter("%(levelprefix)s %(message)s").format(server_record)
    access = SafeUvicornAccessFormatter(
        '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s').format(
            access_record)

    assert "INFO" in server
    assert "[REDACTED_SECRET]" in server
    assert "127.0.0.1" in access
    assert "token=[REDACTED]" in access
    assert "plain-runtime-secret" not in access


def test_json_log_formatter_redacts_and_adds_bound_execution_context() -> None:
    record = logging.LogRecord(
        "trpc_service.agent.worker",
        logging.WARNING,
        __file__,
        1,
        "failed with %s",
        ("Bearer runtime-secret-token", ),
        None,
    )

    with bind_log_context(
            service="trpc-agent-service",
            environment="test",
            node_id="worker-1",
            node_role="worker",
            tenant_id="tenant-1",
            request_id="request-1",
            trace_id="trace-1",
    ):
        payload = json.loads(JsonLogFormatter().format(record))

    assert payload["message"] == "failed with Bearer [REDACTED]"
    assert payload["level"] == "WARNING"
    assert payload["node_id"] == "worker-1"
    assert payload["tenant_id"] == "tenant-1"
    assert payload["request_id"] == "request-1"
    assert payload["trace_id"] == "trace-1"
