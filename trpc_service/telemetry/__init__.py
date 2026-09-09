"""OpenTelemetry configuration and export-time privacy controls."""

from trpc_service.telemetry.sanitizer import SanitizingSpanExporter
from trpc_service.telemetry.setup import configure_telemetry

__all__ = ["SanitizingSpanExporter", "configure_telemetry"]
