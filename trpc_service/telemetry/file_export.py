"""Persist platform-only spans for a reproducible local multi-process acceptance."""

import json
import os
from pathlib import Path
import uuid

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class FileSpanExporter(SpanExporter):

    def __init__(self, directory, service):
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / (service + "-" + str(os.getpid()) + "-" + uuid.uuid4().hex + ".jsonl")

    def export(self, spans):
        with self.path.open("a", encoding="utf-8") as stream:
            for span in spans:
                stream.write(json.dumps({"name": span.name, "trace_id": f"{span.context.trace_id:032x}",
                                         "span_id": f"{span.context.span_id:016x}",
                                         "parent_id": f"{span.parent.span_id:016x}" if span.parent else None,
                                         "attributes": dict(span.attributes or {})}) + "\n")
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass
