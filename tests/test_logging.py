import json
import logging

from trpc_service.log.setup import JsonFormatter, SensitiveDataFilter, configure_logging


def test_json_formatter_includes_context() -> None:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.request_id = "request-1"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "hello"
    assert payload["request_id"] == "request-1"
    assert payload["level"] == "INFO"


def test_configure_logging_replaces_root_handlers() -> None:
    configure_logging("WARNING")

    root = logging.getLogger()
    assert root.level == logging.WARNING
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)


def test_formatter_whitelists_context_and_redacts_secrets() -> None:
    record = logging.LogRecord(
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Authorization=Bearer abc.def token=top-secret sk-1234567890",
        args=(),
        exc_info=None,
    )
    record.request_id = "request-1"
    record.raw_body = {"password": "must-not-appear"}
    SensitiveDataFilter().filter(record)

    rendered = JsonFormatter().format(record)

    assert "abc.def" not in rendered
    assert "top-secret" not in rendered
    assert "sk-1234567890" not in rendered
    assert "must-not-appear" not in rendered
    assert "raw_body" not in rendered
    assert json.loads(rendered)["request_id"] == "request-1"
