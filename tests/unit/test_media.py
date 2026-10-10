from __future__ import annotations

import builtins
import sys
from dataclasses import dataclass
from types import ModuleType

import pytest

import trpc_service.agent.media as media_module
from trpc_service.agent.media import (
    DefaultPdfTextExtractor,
    ExtractionStatus,
    MediaExtractionResult,
    MediaExtractor,
    MediaLimits,
    OcrExtraction,
    PdfExtraction,
    extract_media,
)


def test_utf8_text_is_extracted_and_metadata_is_safe() -> None:
    result = extract_media(
        "标题\nbody".encode(),
        filename=r"C:\private\notes.txt",
        content_type="text/plain; charset=utf-8",
    )

    assert isinstance(result, MediaExtractionResult)
    assert result.text == "标题\nbody"
    assert result.status == ExtractionStatus.EXTRACTED.value
    assert result.metadata["filename"] == "notes.txt"
    assert result.metadata["content_type"] == "text/plain"
    assert result.metadata["extracted_chars"] == 7
    assert result.metadata["redacted"] is True


def test_extracted_text_uses_common_credential_redaction() -> None:
    result = extract_media(
        b"Bearer top-secret password=hunter2",
        "notes.txt",
        "text/plain",
    )

    assert "top-secret" not in result.text
    assert "hunter2" not in result.text
    assert "[REDACTED]" in result.text


def test_common_application_text_mime_is_supported_without_filename() -> None:
    result = extract_media(b'{"ok": true}', content_type="application/json")
    assert result.kind == "text"
    assert result.text == '{"ok": true}'


@dataclass
class FakePdf:
    calls: list[tuple[int, int]]

    def extract(self, data: bytes, *, max_pages: int, max_chars: int) -> PdfExtraction:
        assert data.startswith(b"pdf")
        self.calls.append((max_pages, max_chars))
        return PdfExtraction("PDF body", pages=2)


def test_injected_pdf_extractor_is_bounded_and_offline() -> None:
    fake = FakePdf([])
    result = MediaExtractor(
        limits=MediaLimits(max_bytes=100, max_pdf_pages=3, max_chars=20),
        pdf_extractor=fake,
    ).extract(b"pdf bytes", "report.pdf", "application/pdf")

    assert result.text == "PDF body"
    assert result.kind == "pdf"
    assert result.pages == 2
    assert fake.calls == [(3, 20)]


def test_scanned_pdf_uses_injected_ocr_and_unavailable_fallback() -> None:
    class EmptyPdf:
        def extract(self, _data: bytes, **_kwargs: object) -> PdfExtraction:
            return PdfExtraction("", pages=1)

    class FakeOcr:
        def extract(self, data: bytes, **kwargs: object) -> OcrExtraction:
            assert data.startswith(b"%PDF-")
            assert kwargs["content_type"] == "application/pdf"
            return OcrExtraction("scanned text")

    with_ocr = MediaExtractor(pdf_extractor=EmptyPdf(), ocr_extractor=FakeOcr()).extract(
        b"%PDF-scanned", "scan.pdf", "application/pdf"
    )
    assert with_ocr.text == "scanned text"
    assert with_ocr.metadata["extractor"] == "ocr"
    assert with_ocr.pages == 1

    without_ocr = MediaExtractor(pdf_extractor=EmptyPdf()).extract(
        b"%PDF-scanned", "scan.pdf", "application/pdf"
    )
    assert without_ocr.status == ExtractionStatus.UNAVAILABLE.value
    assert without_ocr.error_type == "ocr_unavailable"
    assert "unavailable" in without_ocr.text.lower()


def test_image_without_ocr_is_explicitly_unavailable() -> None:
    result = extract_media(b"not-decoded-image", "photo.png", "image/png")

    assert result.kind == "image"
    assert result.status == ExtractionStatus.UNAVAILABLE.value
    assert result.error_type == "ocr_unavailable"
    assert "not-decoded-image" not in result.text


def test_text_and_pdf_output_are_truncated_without_ellipsis_or_raw_data() -> None:
    text = extract_media(
        b"abcdefghij",
        "notes.txt",
        "text/plain",
        limits=MediaLimits(max_chars=4),
    )
    assert text.text == "abcd"
    assert text.truncated

    class LongPdf:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            return "0123456789"

    pdf = extract_media(
        b"pdf",
        "report.pdf",
        "application/pdf",
        limits=MediaLimits(max_chars=4),
        pdf_extractor=LongPdf(),
    )
    assert pdf.text == "0123"
    assert pdf.truncated


@pytest.mark.parametrize(
    ("payload", "error_type"),
    [(b"a\x00b", "nul_byte"), (b"\xff\xfe", "invalid_utf8"), (b"a\x01b", "binary_content")],
)
def test_text_rejects_nul_and_binary_content(payload: bytes, error_type: str) -> None:
    result = extract_media(payload, "notes.txt", "text/plain")
    assert result.status == ExtractionStatus.REJECTED.value
    assert result.error_type == error_type
    assert "a\x00b" not in result.text


def test_encrypted_and_corrupt_pdf_errors_are_stable() -> None:
    class FileNotDecryptedError(Exception):
        pass

    class PdfReadError(Exception):
        pass

    class Encrypted:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            raise FileNotDecryptedError("password=do-not-leak")

    class Corrupt:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            raise PdfReadError("SECRET-PDF-BODY")

    encrypted = extract_media(b"pdf", "secret.pdf", "application/pdf", pdf_extractor=Encrypted())
    corrupt = extract_media(b"pdf", "broken.pdf", "application/pdf", pdf_extractor=Corrupt())
    assert encrypted.error_type == "pdf_encrypted"
    assert corrupt.error_type == "pdf_corrupt"
    assert "do-not-leak" not in encrypted.text
    assert "SECRET-PDF-BODY" not in corrupt.text
    assert "do-not-leak" not in repr(encrypted)
    assert "SECRET-PDF-BODY" not in repr(corrupt)


def test_limits_and_compression_bomb_are_rejected_without_exception_text() -> None:
    oversized = extract_media(b"12345", "large.txt", "text/plain", limits=MediaLimits(max_bytes=4))
    assert oversized.status == ExtractionStatus.REJECTED.value
    assert oversized.error_type == "too_large"

    class Bomb:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            return "x" * 1_000

    bomb = extract_media(
        b"pdf",
        "bomb.pdf",
        "application/pdf",
        limits=MediaLimits(max_chars=10, max_expansion_ratio=2),
        pdf_extractor=Bomb(),
    )
    assert bomb.status == ExtractionStatus.REJECTED.value
    assert bomb.error_type == "compression_bomb"
    assert "x" * 100 not in bomb.text


def test_ocr_backend_error_does_not_leak_sensitive_message() -> None:
    class OcrBackend:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            raise RuntimeError("Bearer super-secret-token")

    result = extract_media(b"image", "photo.jpg", "image/jpeg", ocr_extractor=OcrBackend())
    assert result.error_type == "ocr_failed"
    assert "super-secret-token" not in result.text
    assert "super-secret-token" not in repr(result)


@pytest.mark.parametrize(
    "field",
    ["max_bytes", "max_pdf_pages", "max_chars", "max_filename_chars", "max_expansion_ratio"],
)
def test_media_limits_reject_non_positive_and_bool_values(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        MediaLimits(**{field: False})


def test_result_properties_expose_stable_defaults() -> None:
    result = MediaExtractionResult(text="", metadata={})
    assert result.status == "failed"
    assert result.kind == "unsupported"
    assert result.truncated is False
    assert result.error_type is None
    assert result.pages is None
    assert result.success is False

    extracted = MediaExtractionResult(
        text="ok",
        metadata={"status": "extracted", "kind": "text", "pages": "not-an-int"},
    )
    assert extracted.success is True
    assert extracted.pages is None


@pytest.mark.parametrize("content_type", [None, "", "text", "text/pl@in", "x" * 129, 42])
def test_malformed_content_type_is_discarded_without_leaking_input(content_type: object) -> None:
    result = extract_media(b"safe", "notes.txt", content_type)  # type: ignore[arg-type]
    assert result.kind == "text"
    assert result.metadata["content_type"] is None
    assert result.text == "safe"


def test_unsupported_media_and_invalid_input_are_rejected() -> None:
    unsupported = extract_media(b"archive", "archive.bin", "application/octet-stream")
    assert unsupported.status == ExtractionStatus.UNSUPPORTED.value
    assert unsupported.error_type == "unsupported_media"

    class BadLength:
        def __len__(self) -> int:
            raise TypeError("length unavailable")

    class NotBytes:
        def __len__(self) -> int:
            return 3

    bad_length = extract_media(BadLength())  # type: ignore[arg-type]
    not_bytes = extract_media(NotBytes())  # type: ignore[arg-type]
    converted = extract_media(bytearray(b"safe"), "notes.txt", "text/plain")
    assert bad_length.error_type == "invalid_input"
    assert bad_length.metadata["size_bytes"] == 0
    assert not_bytes.error_type == "invalid_input"
    assert not_bytes.metadata["size_bytes"] == 3
    assert converted.text == "safe"


def test_pdf_mapping_sequence_and_backend_method_variants() -> None:
    mapped = extract_media(
        b"pdf",
        "mapped.pdf",
        "application/pdf",
        pdf_extractor=lambda _data, **_kwargs: {
            "text": "not returned when error is set",
            "pages": "invalid",
            "truncated": True,
            "error_type": "backend_offline",
        },
    )
    assert mapped.status == ExtractionStatus.UNAVAILABLE.value
    assert mapped.error_type == "backend_offline"
    assert mapped.pages is None

    sequence = extract_media(
        b"pdf",
        "sequence.pdf",
        limits=MediaLimits(max_pdf_pages=2),
        pdf_extractor=lambda _data, **_kwargs: ["first", 99, "third"],
    )
    assert sequence.text == "first"
    assert sequence.pages == 3
    assert sequence.truncated is True

    class ExtractText:
        def extract_text(self, data: bytes) -> str:
            assert data == b"pdf"
            return "extract_text method"

    method_result = extract_media(b"pdf", "method.pdf", pdf_extractor=ExtractText())
    assert method_result.text == "extract_text method"

    callable_result = extract_media(
        b"pdf", "callable.pdf", pdf_extractor=lambda _data: "callable backend"
    )
    assert callable_result.text == "callable backend"

    invalid = extract_media(b"pdf", "invalid.pdf", pdf_extractor=object())
    assert invalid.status == ExtractionStatus.FAILED.value
    assert invalid.error_type == "pdf_extractor_failed"

    unsupported = extract_media(
        b"pdf", "unsupported.pdf", pdf_extractor=lambda _data, **_kwargs: object()
    )
    assert unsupported.status == ExtractionStatus.FAILED.value
    assert unsupported.error_type == "pdf_extractor_failed"


def test_backend_signature_fallback_and_non_text_results(monkeypatch: pytest.MonkeyPatch) -> None:
    class TinyBackend:
        def extract(self, data: bytes) -> str:
            assert data == b"pdf"
            return "signature fallback"

    original_signature = media_module.inspect.signature

    def unavailable_signature(_function: object) -> object:
        raise ValueError("signature unavailable")

    monkeypatch.setattr(media_module.inspect, "signature", unavailable_signature)
    result = extract_media(b"pdf", "tiny.pdf", pdf_extractor=TinyBackend())
    assert result.text == "signature fallback"
    monkeypatch.setattr(media_module.inspect, "signature", original_signature)

    class NonTextPdf:
        def extract(self, _data: bytes, **_kwargs: object) -> PdfExtraction:
            return PdfExtraction(["not text"])  # type: ignore[arg-type]

    non_text_pdf = extract_media(b"pdf", "non-text.pdf", pdf_extractor=NonTextPdf())
    assert non_text_pdf.error_type == "pdf_extractor_failed"

    class NonTextOcr:
        def extract(self, _data: bytes, **_kwargs: object) -> OcrExtraction:
            return OcrExtraction(["not text"])  # type: ignore[arg-type]

    non_text_ocr = extract_media(b"image", "non-text.jpg", "image/jpeg", ocr_extractor=NonTextOcr())
    assert non_text_ocr.error_type == "ocr_failed"


def test_pdf_backend_error_classes_are_classified_without_exception_text() -> None:
    class ImportFailure(Exception):
        pass

    class GenericFailure(Exception):
        pass

    unavailable = extract_media(
        b"pdf",
        "dependency.pdf",
        pdf_extractor=lambda _data, **_kwargs: (_ for _ in ()).throw(
            media_module.PdfDependencyUnavailable()
        ),
    )
    imported = extract_media(
        b"pdf",
        "import.pdf",
        pdf_extractor=lambda _data, **_kwargs: (_ for _ in ()).throw(ImportFailure("secret")),
    )
    generic = extract_media(
        b"pdf",
        "generic.pdf",
        pdf_extractor=lambda _data, **_kwargs: (_ for _ in ()).throw(
            GenericFailure("private body")
        ),
    )
    assert unavailable.error_type == "pdf_unavailable"
    assert imported.error_type == "pdf_unavailable"
    assert generic.error_type == "pdf_extractor_failed"
    assert "secret" not in imported.text
    assert "private body" not in generic.text


def test_pdf_backend_unicode_and_control_content_are_rejected() -> None:
    class UnicodePdf:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            raise UnicodeDecodeError("utf8", b"x", 0, 1, "private")

    unicode_result = extract_media(b"pdf", "unicode.pdf", pdf_extractor=UnicodePdf())
    assert unicode_result.error_type == "invalid_utf8"

    class NulPdf:
        def extract(self, _data: bytes, **_kwargs: object) -> str:
            return "safe\x00secret"

    nul_result = extract_media(b"pdf", "nul.pdf", pdf_extractor=NulPdf())
    assert nul_result.error_type == "nul_byte"
    assert "secret" not in nul_result.text


def test_ocr_mapping_empty_error_and_compression_paths() -> None:
    mapping_error = extract_media(
        b"image",
        "error.jpg",
        "image/jpeg",
        ocr_extractor=lambda _data, **_kwargs: {
            "text": "hidden",
            "error_type": "provider_unavailable",
        },
    )
    assert mapping_error.status == ExtractionStatus.UNAVAILABLE.value
    assert mapping_error.error_type == "provider_unavailable"
    assert "hidden" not in mapping_error.text

    empty = extract_media(
        b"image", "empty.jpg", "image/jpeg", ocr_extractor=lambda _data, **_kwargs: " \n"
    )
    assert empty.status == ExtractionStatus.UNAVAILABLE.value
    assert empty.error_type == "ocr_empty"

    bomb = extract_media(
        b"image",
        "bomb.jpg",
        "image/jpeg",
        limits=MediaLimits(max_chars=5, max_expansion_ratio=2),
        ocr_extractor=lambda _data, **_kwargs: "x" * 100,
    )
    assert bomb.status == ExtractionStatus.REJECTED.value
    assert bomb.error_type == "compression_bomb"

    class InvalidOcr:
        def extract(self, _data: bytes, **_kwargs: object) -> object:
            return 123

    invalid = extract_media(b"image", "invalid.jpg", "image/jpeg", ocr_extractor=InvalidOcr())
    assert invalid.status == ExtractionStatus.FAILED.value
    assert invalid.error_type == "ocr_failed"


def _fake_pypdf(monkeypatch: pytest.MonkeyPatch, reader: object) -> None:
    pypdf = ModuleType("pypdf")
    errors = ModuleType("pypdf.errors")

    class FileNotDecryptedError(Exception):
        pass

    errors.FileNotDecryptedError = FileNotDecryptedError  # type: ignore[attr-defined]
    pypdf.__path__ = []  # type: ignore[attr-defined]
    pypdf.PdfReader = reader  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pypdf", pypdf)
    monkeypatch.setitem(sys.modules, "pypdf.errors", errors)


def test_default_pdf_extractor_reads_pages_and_applies_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Page:
        def __init__(self, text: object) -> None:
            self.text = text

        def extract_text(self) -> object:
            return self.text

    class Reader:
        def __init__(self, _stream: object, *, strict: bool) -> None:
            assert strict is False
            self.is_encrypted = False
            self.pages = [Page("abc"), Page("xy"), Page("ignored")]

    _fake_pypdf(monkeypatch, Reader)
    extraction = DefaultPdfTextExtractor().extract(b"pdf", max_pages=2, max_chars=4)
    assert extraction.text == "abc\n\nx"
    assert extraction.pages == 3
    assert extraction.truncated is True

    class ExactReader:
        def __init__(self, _stream: object, *, strict: bool) -> None:
            self.is_encrypted = False
            self.pages = [Page("four")]

    _fake_pypdf(monkeypatch, ExactReader)
    exact = DefaultPdfTextExtractor().extract(b"pdf", max_pages=1, max_chars=4)
    assert exact.text == "four"
    assert exact.truncated is True

    class EmptyReader:
        def __init__(self, _stream: object, *, strict: bool) -> None:
            self.is_encrypted = False
            self.pages: list[object] = []

    _fake_pypdf(monkeypatch, EmptyReader)
    empty = DefaultPdfTextExtractor().extract(b"pdf", max_pages=1, max_chars=4)
    assert empty.text == ""
    assert empty.pages == 0
    assert empty.truncated is False

    class NonTextPage:
        def extract_text(self) -> object:
            return 123

    class NonTextReader:
        def __init__(self, _stream: object, *, strict: bool) -> None:
            self.is_encrypted = False
            self.pages = [NonTextPage()]

    _fake_pypdf(monkeypatch, NonTextReader)
    with pytest.raises(Exception) as raised:
        DefaultPdfTextExtractor().extract(b"pdf", max_pages=1, max_chars=4)
    assert type(raised.value).__name__ == "_PdfCorrupt"


@pytest.mark.parametrize("failure", ["encrypted", "decrypt", "memory", "recursion", "corrupt"])
def test_default_pdf_extractor_classifies_reader_failures(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    class Reader:
        def __init__(self, _stream: object, *, strict: bool) -> None:
            if failure == "encrypted":
                self.is_encrypted = True
                self.pages = []
            elif failure == "decrypt":
                raise sys.modules["pypdf.errors"].FileNotDecryptedError("secret")  # type: ignore[attr-defined]
            elif failure == "memory":
                raise MemoryError
            elif failure == "recursion":
                raise RecursionError
            else:
                raise ValueError("private PDF body")

    _fake_pypdf(monkeypatch, Reader)
    with pytest.raises(Exception) as raised:
        DefaultPdfTextExtractor().extract(b"pdf", max_pages=1, max_chars=10)
    if failure == "encrypted":
        assert isinstance(raised.value, media_module.MediaExtractionError)
        assert type(raised.value).__name__ == "_PdfEncrypted"
    elif failure == "decrypt":
        assert type(raised.value).__name__ == "_PdfEncrypted"
    elif failure in {"memory", "recursion"}:
        assert type(raised.value).__name__ == "_CompressionBomb"
    else:
        assert type(raised.value).__name__ == "_PdfCorrupt"


def test_default_pdf_extractor_reports_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__

    def missing_pypdf(name: str, *args: object, **kwargs: object) -> object:
        if name.startswith("pypdf"):
            raise ImportError("pypdf unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_pypdf)
    with pytest.raises(media_module.PdfDependencyUnavailable):
        DefaultPdfTextExtractor().extract(b"pdf", max_pages=1, max_chars=10)
