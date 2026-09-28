"""Isolated document parser entry point; invoked only by the knowledge service."""

from io import BytesIO
import json
import resource
import sys

from trpc_service.storage.knowledge import KnowledgeFileParser, MAX_KNOWLEDGE_FILE_BYTES


def main() -> None:
    # Linux is the supported service/container platform. Bound parser allocation,
    # CPU and output even when a compressed file is small enough to be uploaded.
    resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_CPU, (15, 15))
    payload = sys.stdin.buffer.read(MAX_KNOWLEDGE_FILE_BYTES + 1)
    if len(payload) > MAX_KNOWLEDGE_FILE_BYTES:
        raise SystemExit(2)
    try:
        text = KnowledgeFileParser().parse(sys.argv[1], sys.argv[2], BytesIO(payload))
    except Exception:
        # Parser exceptions can contain document contents. Return no raw traceback.
        raise SystemExit(2) from None
    sys.stdout.write(json.dumps(text, ensure_ascii=False))


if __name__ == "__main__":
    main()
