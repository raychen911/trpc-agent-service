"""Minimal pure-ASGI request tracing middleware.

One fixed-name SERVER span per HTTP request, parented to an extracted
``traceparent`` when valid.  ``/health`` is excluded outright (probe traffic
must never consume spans or connect to a collector).  Non-HTTP scopes
(lifespan/websocket) pass through untouched.  No request path, query, header
or body value ever reaches the span.
"""

from __future__ import annotations

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from .propagation import extract_traceparent
from .runtime import ATTR_EXCEPTION_TYPE, ATTR_STATUS, STATUS_ERROR, STATUS_OK

_HEALTH_PATH = "/health"


class TraceRequestMiddleware:
    """Wrap each HTTP request in a fixed-name SERVER span."""

    def __init__(self, app, tracer, span_name: str) -> None:
        self._app = app
        self._tracer = tracer
        self._span_name = span_name

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or scope.get("path") == _HEALTH_PATH:
            await self._app(scope, receive, send)
            return

        headers = {}
        try:
            for key, value in scope.get("headers") or ():
                headers[bytes(key).decode("latin-1").lower()] = bytes(value).decode("latin-1")
        except Exception:
            headers = {}

        try:
            parent = extract_traceparent(headers)
        except Exception:
            parent = None

        try:
            span = self._tracer.start_span(self._span_name, context=parent, kind=SpanKind.SERVER)
        except Exception:
            await self._app(scope, receive, send)
            return

        token = otel_context.attach(trace.set_span_in_context(span))

        response_status: dict[str, int] = {}

        async def send_wrapper(message):
            if message.get("type") == "http.response.start":
                try:
                    response_status["status"] = int(message.get("status", 0))
                except Exception:
                    pass
            await send(message)

        failed = False
        try:
            await self._app(scope, receive, send_wrapper)
        except BaseException as exc:
            failed = True
            try:
                span.set_attribute(ATTR_EXCEPTION_TYPE, type(exc).__name__)
                span.set_attribute(ATTR_STATUS, STATUS_ERROR)
                span.set_status(StatusCode.ERROR)
            except Exception:
                pass
            raise
        finally:
            if not failed:
                status = response_status.get("status", 0)
                try:
                    span.set_attribute(ATTR_STATUS, STATUS_ERROR if status >= 500 else STATUS_OK)
                except Exception:
                    pass
            try:
                otel_context.detach(token)
            except Exception:
                pass
            try:
                span.end()
            except Exception:
                pass


__all__ = ["TraceRequestMiddleware"]
