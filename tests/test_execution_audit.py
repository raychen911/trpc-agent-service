"""Stage 6B2 Task 1: append-only execution audit contract tests (RED first).

Covers ``ExecutionAuditEvent`` (fixed event types/outcomes, category enum,
trace-id format, tenant/format strictness, pairing rules, frozen+extra-forbid),
``current_trace_id`` safe OTel extraction, and the closed-repository lifecycle
errors of ``SqlExecutionAuditRepository``.  Database-level constraints,
append-only triggers, ordering, tenant isolation, and transaction atomicity
with receipt terminal states are proven in
tests/integration/test_stage6b2_content_audit.py on real PostgreSQL.

Sentinel rule from the design: no body, reply, tool args, external identity,
or exception detail may enter these events; categories are fixed enums only.
"""

from __future__ import annotations

import asyncio
import re
import traceback
import uuid
from datetime import datetime, timezone

import pytest
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    use_span,
)
from pydantic import ValidationError

from trpc_service.audit.models import ExecutionAuditEvent, current_trace_id
from trpc_service.storage.execution_audit_repository import (
    ExecutionAuditRepositoryConfigurationError,
    ExecutionAuditRepositoryDataError,
    ExecutionAuditRepositoryUnavailableError,
    SqlExecutionAuditRepository,
)
from trpc_service.storage.schema import execution_audit_events
from trpc_service.transport.models import WorkerErrorCode

VALID_TRACE = "0" * 28 + "abcd"
VALID_TENANT = "tenant_default"


def _event(**overrides: object) -> ExecutionAuditEvent:
    defaults: dict = {
        "audit_id": uuid.uuid4(),
        "tenant_id": VALID_TENANT,
        "receipt_id": uuid.uuid4(),
        "request_id": uuid.uuid4(),
        "config_version": 3,
        "trace_id": VALID_TRACE,
        "event_type": "content_decision",
        "outcome": "allow",
        "category": "none",
        "tool_name": None,
        "error_code": None,
        "latency_ms": None,
        "occurred_at": datetime(2026, 9, 5, 12, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return ExecutionAuditEvent(**defaults)


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# event model: fixed vocabulary and pairing
# ---------------------------------------------------------------------------


class TestExecutionAuditEventModel:

    def test_valid_content_decision_event(self):
        event = _event()
        assert event.event_type == "content_decision"
        assert event.category == "none"

    def test_is_frozen_and_extra_forbidden(self):
        event = _event()
        with pytest.raises(ValidationError):
            event.outcome = "blocked"  # type: ignore[misc]
        with pytest.raises(ValidationError):
            _event(raw_text="secret body")

    @pytest.mark.parametrize("bad_type", ["prompt", "message_audit", "contentdecision", "Content_Decision"])
    def test_unknown_event_type_rejected(self, bad_type: str):
        with pytest.raises(ValidationError):
            _event(event_type=bad_type, category=None)

    @pytest.mark.parametrize("bad_outcome", ["ok", "BLOCKED", "", "denied"])
    def test_unknown_outcome_rejected(self, bad_outcome: str):
        with pytest.raises(ValidationError):
            _event(outcome=bad_outcome)

    @pytest.mark.parametrize("bad_category", ["phone_number", "None", "credential ", "regex"])
    def test_unknown_category_rejected(self, bad_category: str):
        with pytest.raises(ValidationError):
            _event(category=bad_category)

    @pytest.mark.parametrize(
        "event_type, outcome",
        [
            ("content_decision", "success"),
            ("content_decision", "delivered"),
            ("agent_result", "allow"),
            ("agent_result", "blocked"),
            ("tool_decision", "blocked"),
            ("tool_decision", "delivered"),
            ("delivery_result", "allow"),
            ("delivery_result", "error"),
        ],
    )
    def test_outcome_must_pair_with_event_type(self, event_type: str, outcome: str):
        with pytest.raises(ValidationError):
            _event(event_type=event_type,
                   outcome=outcome,
                   category=None if event_type != "content_decision" else "none")

    def test_all_four_types_accept_their_own_outcomes(self):
        for kwargs in (
            {
                "event_type": "content_decision",
                "outcome": "blocked",
                "category": "credential"
            },
            {
                "event_type": "agent_result",
                "outcome": "success",
                "category": None
            },
            {
                "event_type": "agent_result",
                "outcome": "error",
                "category": None,
                "error_code": "model_runtime"
            },
            {
                "event_type": "tool_decision",
                "outcome": "review_pending",
                "category": None,
                "tool_name": "get_current_time"
            },
            {
                "event_type": "tool_decision",
                "outcome": "deny_blocked",
                "category": None,
                "tool_name": "get_current_time"
            },
            {
                "event_type": "delivery_result",
                "outcome": "delivered",
                "category": None
            },
            {
                "event_type": "delivery_result",
                "outcome": "failed",
                "category": None,
                "error_code": "tenant_repository_unavailable"
            },
        ):
            event = _event(**kwargs)
            assert event.event_type == kwargs["event_type"]

    def test_content_decision_requires_category(self):
        with pytest.raises(ValidationError):
            _event(category=None)

    def test_non_content_decision_forbids_category(self):
        with pytest.raises(ValidationError):
            _event(event_type="agent_result", outcome="success", category="credential")

    def test_tool_decision_requires_tool_name(self):
        with pytest.raises(ValidationError):
            _event(event_type="tool_decision", outcome="allow", category=None, tool_name=None)

    @pytest.mark.parametrize("bad_tool", ["", "   ", "x" * 201])
    def test_tool_name_shape(self, bad_tool: str):
        with pytest.raises(ValidationError):
            _event(event_type="tool_decision", outcome="allow", category=None, tool_name=bad_tool)

    @pytest.mark.parametrize("bad_error", ["BOOM", "unknown_code", "x" * 100])
    def test_error_code_must_be_fixed_vocabulary(self, bad_error: str):
        with pytest.raises(ValidationError):
            _event(event_type="agent_result", outcome="error", category=None, error_code=bad_error)

    def test_error_code_restricted_outcomes(self):
        with pytest.raises(ValidationError):
            _event(event_type="content_decision", outcome="allow", category="none", error_code="model_runtime")

    @pytest.mark.parametrize("event_type,outcome", [("agent_result", "error"), ("delivery_result", "failed")])
    def test_failure_outcomes_require_error_code(self, event_type: str, outcome: str):
        with pytest.raises(ValidationError):
            _event(event_type=event_type, outcome=outcome, category=None, error_code=None)

    def test_tool_name_must_already_be_normalized(self):
        with pytest.raises(ValidationError):
            _event(event_type="tool_decision", outcome="allow", category=None, tool_name=" get_current_time ")

    @pytest.mark.parametrize(
        "event_type,outcome,category,tool_name",
        [
            ("content_decision", "allow", "none", None),
            ("agent_result", "success", None, None),
            ("tool_decision", "allow", None, "get_current_time"),
        ],
    )
    def test_only_delivery_events_may_omit_receipt(self, event_type, outcome, category, tool_name):
        with pytest.raises(ValidationError):
            _event(
                receipt_id=None,
                event_type=event_type,
                outcome=outcome,
                category=category,
                tool_name=tool_name,
            )

    def test_delivery_event_may_omit_receipt(self):
        event = _event(receipt_id=None, event_type="delivery_result", outcome="delivered", category=None)
        assert event.receipt_id is None

    def test_delivery_event_may_keep_receipt(self):
        event = _event(event_type="delivery_result", outcome="delivered", category=None)
        assert event.receipt_id is not None


# ---------------------------------------------------------------------------
# event model: strict scalars
# ---------------------------------------------------------------------------


class TestExecutionAuditScalars:

    @pytest.mark.parametrize(
        "bad_trace",
        [
            "0" * 31,
            "0" * 33,
            "ABCDEF0123456789ABCDEF0123456789",  # uppercase
            "z" * 32,
            "0x" + "0" * 30,
        ])
    def test_trace_id_must_be_32_lowercase_hex(self, bad_trace: str):
        with pytest.raises(ValidationError):
            _event(trace_id=bad_trace)

    def test_trace_id_nullable(self):
        assert _event(trace_id=None).trace_id is None

    @pytest.mark.parametrize("bad_tenant", ["Tenant_default", "X" * 64, "", "tenant default", "9tenant"])
    def test_tenant_id_format(self, bad_tenant: str):
        with pytest.raises(ValidationError):
            _event(tenant_id=bad_tenant)

    @pytest.mark.parametrize("bad_version", [0, -1, True, "3", 1.0])
    def test_config_version_strict_positive_int(self, bad_version: object):
        with pytest.raises(ValidationError):
            _event(config_version=bad_version)

    @pytest.mark.parametrize("bad_latency", [-1, True, "10"])
    def test_latency_ms_strict_non_negative(self, bad_latency: object):
        with pytest.raises(ValidationError):
            _event(latency_ms=bad_latency)

    def test_occurred_at_required_tz_aware(self):
        with pytest.raises(ValidationError):
            _event(occurred_at=datetime(2026, 9, 5, 12, 0, 0))

    def test_dump_is_fixed_fields_only(self):
        event = _event(event_type="tool_decision", outcome="deny_blocked", category=None, tool_name="get_current_time")
        dumped = event.model_dump(mode="json")
        assert set(dumped) == {
            "audit_id",
            "tenant_id",
            "receipt_id",
            "request_id",
            "config_version",
            "trace_id",
            "event_type",
            "outcome",
            "category",
            "tool_name",
            "error_code",
            "latency_ms",
            "occurred_at",
        }


# ---------------------------------------------------------------------------
# current_trace_id — safe OTel extraction
# ---------------------------------------------------------------------------


class TestCurrentTraceId:

    def test_no_active_span_returns_none(self):
        assert current_trace_id() is None

    def test_active_span_returns_lowercase_hex(self):
        # A *real* SDK tracer (as the enabled Stage 6B1 runtime hands out, app-
        # scoped — the global provider is never touched) generates a valid
        # nonzero trace id; the NoOp tracer deliberately does not.
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        try:
            span = provider.get_tracer("test").start_span("worker.request")
            with use_span(span):
                trace_id = current_trace_id()
            span.end()
        finally:
            provider.shutdown()
        assert trace_id is not None
        assert len(trace_id) == 32
        assert trace_id == trace_id.lower()
        assert int(trace_id, 16) != 0

    def test_invalid_span_context_returns_none(self):
        invalid = NonRecordingSpan(SpanContext(trace_id=0, span_id=123, is_remote=False, trace_flags=TraceFlags(0)))
        with use_span(invalid):
            assert current_trace_id() is None


# ---------------------------------------------------------------------------
# repository lifecycle without a database
# ---------------------------------------------------------------------------


class TestRepositoryLifecycle:

    def test_append_on_closed_repository_raises_unavailable(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        _run(repo.close())
        with pytest.raises(ExecutionAuditRepositoryUnavailableError):
            _run(repo.append(_event()))

    def test_list_on_closed_repository_raises_unavailable(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        _run(repo.close())
        with pytest.raises(ExecutionAuditRepositoryUnavailableError):
            _run(repo.list_for_receipt(VALID_TENANT, uuid.uuid4(), 10))

    def test_check_ready_on_closed_repository_raises_unavailable(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        _run(repo.close())
        with pytest.raises(ExecutionAuditRepositoryUnavailableError):
            _run(repo.check_ready())

    def test_close_is_idempotent(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        _run(repo.close())
        _run(repo.close())

    def test_append_rejects_non_event(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        with pytest.raises(ExecutionAuditRepositoryDataError):
            _run(repo.append({"event_type": "content_decision"}))  # type: ignore[arg-type]

    def test_limit_must_be_positive(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        with pytest.raises(ExecutionAuditRepositoryDataError):
            _run(repo.list_for_receipt(VALID_TENANT, uuid.uuid4(), 0))

    def test_list_for_request_on_closed_repository_raises_unavailable(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        _run(repo.close())
        with pytest.raises(ExecutionAuditRepositoryUnavailableError):
            _run(repo.list_for_request(VALID_TENANT, uuid.uuid4(), 10))

    def test_list_for_request_validates_limit_tenant_and_uuid(self):
        repo = SqlExecutionAuditRepository(None)  # type: ignore[arg-type]
        with pytest.raises(ExecutionAuditRepositoryDataError):
            _run(repo.list_for_request(VALID_TENANT, uuid.uuid4(), 0))
        with pytest.raises(ExecutionAuditRepositoryDataError):
            _run(repo.list_for_request("BAD TENANT", uuid.uuid4(), 10))
        with pytest.raises(ExecutionAuditRepositoryDataError):
            _run(repo.list_for_request(VALID_TENANT, "not-a-uuid", 10))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# from_env — fixed, sanitized configuration errors (no DSN ever leaks)
# ---------------------------------------------------------------------------

FAKE_DSN_SENTINEL = "fake-dsn-passwd-sentinel-9e2c"
FROM_ENV_FIXED_TEXT = "execution audit repository could not be configured"


class TestFromEnvSanitization:

    @pytest.mark.parametrize(
        "environ",
        [
            {},  # missing URL
            {
                "TRPC_DATABASE_URL": "not a url at all"
            },
            {
                "TRPC_DATABASE_URL": f"postgresql://user:{FAKE_DSN_SENTINEL}@dbhost/trpc"
            },  # wrong driver
            {
                "TRPC_DATABASE_URL": f"postgresql+asyncpg://user:{FAKE_DSN_SENTINEL}@dbhost"
            },  # no database
        ],
    )
    def test_configuration_error_is_fixed_and_causeless(self, environ):
        with pytest.raises(ExecutionAuditRepositoryConfigurationError) as raised:
            SqlExecutionAuditRepository.from_env(environ)
        exc = raised.value
        assert str(exc) == FROM_ENV_FIXED_TEXT
        assert exc.__cause__ is None
        assert exc.__suppress_context__ is True
        blob = " ".join((
            str(exc),
            repr(exc),
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        ))
        assert FAKE_DSN_SENTINEL not in blob


# ---------------------------------------------------------------------------
# Python model ↔ SQL CHECK mirroring
# ---------------------------------------------------------------------------


def _check_sql(name: str) -> str:
    for constraint in execution_audit_events.constraints:
        if getattr(constraint, "name", None) == name:
            return str(constraint.sqltext)
    raise AssertionError(f"schema is missing CHECK constraint {name!r}")


class TestSqlPythonMirror:

    def test_error_code_vocabulary_matches_worker_error_codes(self):
        sqltext = _check_sql("execution_audit_events_error_code_valid")
        codes = set(re.findall(r"'([a-z_]+)'", sqltext))
        assert codes == {code.value for code in WorkerErrorCode}

    def test_outcome_vocabulary_matches_model(self):
        from trpc_service.audit.models import _OUTCOME_BY_TYPE
        sqltext = _check_sql("execution_audit_events_outcome_valid")
        outcomes = set(re.findall(r"'([a-z_]+)'", sqltext))
        assert outcomes == set().union(*_OUTCOME_BY_TYPE.values())

    def test_tool_name_requires_normalized_bounded_text(self):
        sqltext = _check_sql("execution_audit_events_tool_name_valid")
        assert "btrim(tool_name)" in sqltext
        assert "char_length(tool_name) <= 200" in sqltext

    def test_failure_pairing_makes_error_code_mandatory(self):
        sqltext = _check_sql("execution_audit_events_error_code_required")
        assert "(event_type = 'agent_result' AND outcome = 'error')" in sqltext
        assert "(event_type = 'delivery_result' AND outcome = 'failed')" in sqltext
        assert "(error_code IS NOT NULL)" in sqltext
