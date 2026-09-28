from collections.abc import AsyncIterator
from io import BytesIO
from uuid import uuid4

import pytest

from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.storage.knowledge import (
    KnowledgeFileParser,
    TenantKnowledgeService,
    TextChunker,
)
from trpc_service.tenant.context import TenantContext


async def _content(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


@pytest.mark.parametrize(
    ("filename", "media_type", "payload", "expected"),
    [
        ("policy.txt", "text/plain", "年假制度".encode(), "年假制度"),
        ("guide.md", "text/markdown", b"# Guide\n\nUse RAG.", "Use RAG."),
    ],
)
def test_knowledge_parser_accepts_supported_text_files(
    filename: str,
    media_type: str,
    payload: bytes,
    expected: str,
) -> None:
    parser = KnowledgeFileParser()

    parsed = parser.parse(filename, media_type, BytesIO(payload))

    assert expected in parsed


def test_knowledge_parser_rejects_unsupported_files() -> None:
    parser = KnowledgeFileParser()

    with pytest.raises(ValueError, match="unsupported knowledge file"):
        parser.parse("archive.zip", "application/zip", BytesIO(b"zip"))


def test_knowledge_parser_bounds_extracted_text() -> None:
    parser = KnowledgeFileParser(max_extracted_chars=5)

    with pytest.raises(ValueError, match="too much extracted text"):
        parser.parse("large.txt", "text/plain", BytesIO(b"123456"))


def test_knowledge_parser_rejects_invalid_or_empty_text() -> None:
    with pytest.raises(ValueError, match="positive"):
        KnowledgeFileParser(max_extracted_chars=0)
    parser = KnowledgeFileParser()

    with pytest.raises(ValueError, match="UTF-8"):
        parser.parse("invalid.txt", "text/plain", BytesIO(b"\xff"))
    with pytest.raises(ValueError, match="no extractable text"):
        parser.parse("empty.md", "text/markdown", BytesIO(b"  \n"))


def test_knowledge_components_reject_invalid_limits_and_composition() -> None:
    with pytest.raises(ValueError, match="chunk size"):
        TextChunker(chunk_size=10, overlap=10)
    assert TextChunker().split("   ") == ()
    with pytest.raises(ValueError, match="limit and ingestion lease"):
        TenantKnowledgeService(
            None,  # type: ignore[arg-type]
            max_file_bytes=0,
        )
    with pytest.raises(ValueError, match="fixed stores or a storage router"):
        TenantKnowledgeService(None)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_knowledge_upload_fails_before_persistence_for_invalid_input() -> None:
    backend = build_inmemory_backend()
    assert backend.knowledge is not None and backend.artifact is not None
    service = TenantKnowledgeService(
        None,  # type: ignore[arg-type]
        knowledge=backend.knowledge,
        artifacts=backend.artifact,
        max_file_bytes=1,
    )
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-validation",
        trace_id="trace-validation",
    )

    with pytest.raises(ValueError, match="identity and file metadata"):
        await service.upload(
            context,
            principal_id="",
            filename="policy.md",
            media_type="text/markdown",
            content=_content(b"x"),
        )
    with pytest.raises(ValueError, match="unsupported knowledge file"):
        await service.upload(
            context,
            principal_id="user",
            filename="policy.exe",
            media_type="application/octet-stream",
            content=_content(b"x"),
        )
    with pytest.raises(ValueError, match="10 MB limit"):
        await service.upload(
            context,
            principal_id="user",
            filename="policy.md",
            media_type="text/markdown",
            content=_content(b"xx"),
        )


def test_text_chunker_preserves_overlap_and_source_offsets() -> None:
    chunker = TextChunker(chunk_size=12, overlap=4)

    chunks = chunker.split("第一段知识。\n\n第二段知识。\n\n第三段知识。")

    assert len(chunks) >= 2
    assert chunks[0].index == 0
    assert chunks[0].start_char == 0
    assert chunks[1].start_char < chunks[0].end_char
    assert all(chunk.content.strip() for chunk in chunks)


@pytest.mark.anyio
async def test_isolated_document_parser_accepts_docx_and_rejects_corruption() -> None:
    from docx import Document

    backend = build_inmemory_backend()
    service = TenantKnowledgeService(
        None,  # type: ignore[arg-type]
        knowledge=backend.knowledge,
        artifacts=backend.artifact,
    )
    document = Document()
    document.add_paragraph("企业知识库：新员工指南")
    payload = BytesIO()
    document.save(payload)
    assert await service._parse("guide.docx", "application/octet-stream",
                                payload.getvalue()) == ("企业知识库：新员工指南")
    with pytest.raises(ValueError, match="document parsing failed"):
        await service._parse("broken.pdf", "application/pdf", b"not a PDF")


def test_docx_expansion_bomb_is_rejected_before_document_parsing() -> None:
    from zipfile import ZIP_DEFLATED, ZipFile

    payload = BytesIO()
    with ZipFile(payload, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", b"x" * (32 * 1024 * 1024 + 1))
    payload.seek(0)
    with pytest.raises(ValueError, match="expanded file limit"):
        KnowledgeFileParser().parse("bomb.docx", "application/octet-stream", payload)
