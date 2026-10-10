#!/usr/bin/env python3
"""Run an offline content-leak gate through the service privacy paths.

The check deliberately creates high-entropy sentinels at runtime and never
puts their values (or hashes) in the resulting report.  It validates the
redacting JSON formatter, the export-time span processor, the generic API
error handler, and report serialization.  This is development evidence only:
the production gate remains ``not_run`` until real deployed logs and traces
are scanned.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import secrets
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)

from scripts.report_io import atomic_write_json
from trpc_service.log.configure import RedactingJsonFormatter
from trpc_service.log.redaction import REDACTED, redact
from trpc_service.metrics.privacy import PrivacySpanProcessor
from trpc_service.tenant.auth import AuthenticationError
from trpc_service.web.errors import install_error_handlers

SENTINEL_LABELS = (
    "api_token",
    "database_password",
    "message_body",
    "tool_arguments",
    "tool_result",
)
_REDACTION_MARKER = REDACTED


def _generate_sentinels() -> dict[str, str]:
    """Create unique values without relying on environment or fixture secrets."""

    return {label: f"privacy-{label}-{secrets.token_urlsafe(32)}" for label in SENTINEL_LABELS}


def _scan_capture(captured: Any, sentinels: Mapping[str, str]) -> list[str]:
    """Return labels whose raw values occur in a captured representation."""

    rendered = json.dumps(captured, ensure_ascii=False, default=str, sort_keys=True)
    return sorted(label for label, value in sentinels.items() if value in rendered)


def _path_result(
    name: str,
    captured: Any,
    sentinels: Mapping[str, str],
    *,
    redaction_applied: bool,
    require_marker: bool = True,
) -> dict[str, Any]:
    leaked = _scan_capture(captured, sentinels)
    passed = not leaked and (redaction_applied or not require_marker)
    result: dict[str, Any] = {
        "name": name,
        "gate": "pass" if passed else "fail",
        "redaction_applied": redaction_applied,
        "raw_sentinel_labels": leaked,
        "sentinel_labels": list(SENTINEL_LABELS),
    }
    if not redaction_applied and require_marker:
        result["failure"] = "required redaction marker was not observed"
    if leaked:
        result["failure"] = "raw sentinel observed in captured output"
    return result


def _run_json_log_path(sentinels: Mapping[str, str]) -> dict[str, Any]:
    record = logging.LogRecord(
        "privacy-gate",
        logging.ERROR,
        __file__,
        1,
        "message_body=%s token=%s",
        (sentinels["message_body"], sentinels["api_token"]),
        None,
    )
    record.api_token = sentinels["api_token"]
    record.database_password = sentinels["database_password"]
    record.message_body = sentinels["message_body"]
    record.tool_args = {"query": sentinels["tool_arguments"]}
    record.tool_result = sentinels["tool_result"]
    rendered = RedactingJsonFormatter().format(record)
    payload = json.loads(rendered)
    return _path_result(
        "json_log",
        payload,
        sentinels,
        redaction_applied=_REDACTION_MARKER in rendered,
    )


class _CapturingExporter(SpanExporter):
    """Small in-memory exporter used only by the offline gate."""

    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None


def _run_span_path(sentinels: Mapping[str, str]) -> dict[str, Any]:
    exporter = _CapturingExporter()
    provider = TracerProvider()
    provider.add_span_processor(PrivacySpanProcessor(SimpleSpanProcessor(exporter)))
    tracer = provider.get_tracer("privacy-leak-gate")
    with tracer.start_as_current_span("privacy-leak-check") as span:
        span.set_attribute("tenant_id", "offline-privacy-tenant")
        span.set_attribute("llm.request", sentinels["message_body"])
        span.set_attribute("llm.response", sentinels["tool_result"])
        span.set_attribute("tool.arguments", sentinels["tool_arguments"])
        span.set_attribute("tool.response", sentinels["tool_result"])
        span.add_event(
            "tool.execute",
            {
                "tool.arguments": sentinels["tool_arguments"],
                "tool.response": sentinels["tool_result"],
                "api_token": sentinels["api_token"],
            },
        )
    provider.shutdown()
    captured = [
        {
            "attributes": dict(cast(Mapping[str, Any], span.attributes or {})),
            "events": [
                dict(cast(Mapping[str, Any], event.attributes or {})) for event in span.events
            ],
        }
        for span in exporter.spans
    ]
    rendered = json.dumps(captured, ensure_ascii=False, default=str)
    return _path_result(
        "privacy_span_export",
        captured,
        sentinels,
        redaction_applied=_REDACTION_MARKER in rendered,
    )


async def _error_response_capture(sentinels: Mapping[str, str]) -> dict[str, Any]:
    app = FastAPI()
    install_error_handlers(app)
    handler = cast(
        Callable[[Request, AuthenticationError], Awaitable[JSONResponse]],
        app.exception_handlers[AuthenticationError],
    )
    exception = AuthenticationError(
        "token="
        + sentinels["api_token"]
        + " password="
        + sentinels["database_password"]
        + " message_body="
        + sentinels["message_body"]
    )
    response = await handler(cast(Request, object()), exception)
    return {
        "status_code": response.status_code,
        "body": json.loads(bytes(response.body).decode("utf-8")),
    }


def _run_error_path(sentinels: Mapping[str, str]) -> dict[str, Any]:
    captured = asyncio.run(_error_response_capture(sentinels))
    return _path_result(
        "error_response",
        captured,
        sentinels,
        redaction_applied=captured.get("body") == {"error": "unauthenticated"},
        require_marker=False,
    )


def _run_report_serialization_path(sentinels: Mapping[str, str]) -> dict[str, Any]:
    # These values exist only in memory and are passed through the same
    # recursive redactor used before writing reports.  They are never included
    # in the returned report object.
    unsafe_evidence = {
        "api_token": sentinels["api_token"],
        "database_password": sentinels["database_password"],
        "message_body": sentinels["message_body"],
        "tool_args": sentinels["tool_arguments"],
        "tool_result": sentinels["tool_result"],
    }
    safe_evidence = redact(unsafe_evidence)
    rendered = json.dumps({"evidence": safe_evidence}, ensure_ascii=False, sort_keys=True)
    payload = json.loads(rendered)
    return _path_result(
        "report_serialization",
        payload,
        sentinels,
        redaction_applied=_REDACTION_MARKER in rendered,
    )


def _run_gate(sentinels: Mapping[str, str]) -> dict[str, Any]:
    path_functions = (
        ("json_log", _run_json_log_path),
        ("privacy_span_export", _run_span_path),
        ("error_response", _run_error_path),
        ("report_serialization", _run_report_serialization_path),
    )
    paths: dict[str, dict[str, Any]] = {}
    for name, function in path_functions:
        try:
            result = function(sentinels)
        except Exception as error:  # pragma: no cover - defensive report boundary
            result = {
                "name": name,
                "gate": "fail",
                "redaction_applied": False,
                "raw_sentinel_labels": [],
                "sentinel_labels": list(SENTINEL_LABELS),
                "failure": f"path raised {type(error).__name__}",
            }
        paths[name] = result
    failures = [name for name, result in paths.items() if result["gate"] != "pass"]
    leaked = sorted({label for result in paths.values() for label in result["raw_sentinel_labels"]})
    passed = not failures and not leaked
    return {
        "baseline": {
            "raw_sentinels_must_be_absent": True,
            "production_scan_required": True,
        },
        "candidate": {
            "mode": "offline_generated_sentinels",
            "sentinel_labels": list(SENTINEL_LABELS),
            "paths": paths,
            "raw_sentinel_count": len(leaked),
        },
        "case_deltas": {
            "failed_paths": len(failures),
            "leaked_sentinel_labels": leaked,
        },
        "gate": "pass" if passed else "fail",
        "rejection_reasons": (
            [] if passed else [f"privacy path failed: {name}" for name in failures]
        ),
        "production_gate": "not_run",
        "production_rejection_reasons": [
            "offline evidence does not scan real deployed logs or traces"
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/privacy-leak-offline.json")
    )
    args = parser.parse_args(argv)
    report = _run_gate(_generate_sentinels())
    rendered = atomic_write_json(args.output, report).rstrip("\n")
    print(rendered)
    return 0 if report["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
