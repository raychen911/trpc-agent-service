"""Bounded, offline media-to-text extraction for agent prompts.

The service receives media bytes only after a channel has downloaded them.  This
module deliberately has no network or filesystem access: PDF and OCR backends
are injected (with an optional, lazy :mod:`pypdf` adapter for PDFs).  All
backend failures are converted to stable error types so that a document's
contents, and an exception's message, never become an error response.
"""

from __future__ import annotations

import inspect
import io
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast

from trpc_service.log.redaction import sanitize_text

DEFAULT_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_PDF_PAGES = 32
DEFAULT_MAX_CHARS = 50_000
DEFAULT_MAX_FILENAME_CHARS = 255
DEFAULT_MAX_EXPANSION_RATIO = 16


class MediaKind(StrEnum):
    """The supported media families."""

    TEXT = "text"
    PDF = "pdf"
    IMAGE = "image"
    UNSUPPORTED = "unsupported"


class ExtractionStatus(StrEnum):
    """Stable result status values suitable for metadata and metrics."""

    EXTRACTED = "extracted"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MediaLimits:
    """Hard limits applied before and during extraction.

    ``max_bytes`` is checked before any backend sees the content.  The
    expansion ratio is a second guard for a backend that ignores ``max_chars``
    and attempts to return a very large decompressed PDF/OCR result.
    """

    max_bytes: int = DEFAULT_MAX_BYTES
    max_pdf_pages: int = DEFAULT_MAX_PDF_PAGES
    max_chars: int = DEFAULT_MAX_CHARS
    max_filename_chars: int = DEFAULT_MAX_FILENAME_CHARS
    max_expansion_ratio: int = DEFAULT_MAX_EXPANSION_RATIO

    def __post_init__(self) -> None:
        for name in (
            "max_bytes",
            "max_pdf_pages",
            "max_chars",
            "max_filename_chars",
            "max_expansion_ratio",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


# This longer name reads better at call sites and is kept as the public name
# for callers that prefer to make the extraction intent explicit.
MediaExtractionLimits = MediaLimits


@dataclass(frozen=True, slots=True)
class PdfExtraction:
    """Normalized output accepted from an injected PDF extractor."""

    text: str
    pages: int | None = None
    truncated: bool = False
    error_type: str | None = None


@dataclass(frozen=True, slots=True)
class OcrExtraction:
    """Normalized output accepted from an injected OCR extractor."""

    text: str
    truncated: bool = False
    error_type: str | None = None


class PdfTextExtractor(Protocol):
    """Protocol for a synchronous, offline PDF text extractor."""

    def extract(self, data: bytes, *, max_pages: int, max_chars: int) -> PdfExtraction | str: ...


class OcrExtractor(Protocol):
    """Protocol for a synchronous, offline OCR implementation."""

    def extract(
        self,
        data: bytes,
        *,
        filename: str | None,
        content_type: str | None,
        max_chars: int,
    ) -> OcrExtraction | str: ...


# A few integrations use the all-caps spelling; aliases keep the protocol
# discoverable without introducing a second implementation.
OCRExtractor = OcrExtractor
PdfExtractor = PdfTextExtractor


@dataclass(frozen=True, slots=True)
class MediaExtractionResult:
    """Agent-facing text and non-sensitive extraction metadata."""

    text: str
    metadata: dict[str, object]

    @property
    def status(self) -> str:
        return str(self.metadata.get("status", "failed"))

    @property
    def kind(self) -> str:
        return str(self.metadata.get("kind", MediaKind.UNSUPPORTED.value))

    @property
    def truncated(self) -> bool:
        return bool(self.metadata.get("truncated", False))

    @property
    def error_type(self) -> str | None:
        value = self.metadata.get("error_type")
        return str(value) if value is not None else None

    @property
    def pages(self) -> int | None:
        value = self.metadata.get("pages")
        return value if isinstance(value, int) else None

    @property
    def success(self) -> bool:
        return self.status == ExtractionStatus.EXTRACTED.value


class MediaExtractionError(Exception):
    """Base for private, stable backend error classification."""


class PdfDependencyUnavailable(MediaExtractionError):
    pass


class _PdfEncrypted(MediaExtractionError):
    pass


class _PdfCorrupt(MediaExtractionError):
    pass


class _CompressionBomb(MediaExtractionError):
    pass


class _NulContent(MediaExtractionError):
    pass


class _BinaryContent(MediaExtractionError):
    pass


_TEXT_EXTENSIONS = frozenset(
    {
        ".c",
        ".cfg",
        ".conf",
        ".csv",
        ".css",
        ".go",
        ".html",
        ".htm",
        ".ini",
        ".java",
        ".js",
        ".json",
        ".log",
        ".md",
        ".py",
        ".rst",
        ".sh",
        ".tex",
        ".sql",
        ".toml",
        ".ts",
        ".tsx",
        ".txt",
        ".xml",
        ".yaml",
        ".yml",
    }
)
_TEXT_CONTENT_TYPES = frozenset(
    {
        "application/ecmascript",
        "application/javascript",
        "application/json",
        "application/ld+json",
        "application/rtf",
        "application/sql",
        "application/x-ndjson",
        "application/x-yaml",
        "application/xml",
    }
)
_IMAGE_EXTENSIONS = frozenset(
    {".avif", ".bmp", ".gif", ".heic", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
)
_ALLOWED_TEXT_CONTROLS = frozenset("\t\n\r\f")


def _safe_filename(filename: str | None, max_chars: int) -> str | None:
    if not isinstance(filename, str) or not filename:
        return None
    # Do not expose a local path in metadata.  Both separators are handled so
    # a Windows path cannot be smuggled through a POSIX deployment (or vice
    # versa).
    name = filename.replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(character for character in name if character.isprintable())
    return sanitize_text(name)[:max_chars] or None


def _content_type(content_type: str | None) -> str | None:
    if not isinstance(content_type, str):
        return None
    # Parameters can contain arbitrary user text; retain only the MIME token.
    value = content_type.split(";", 1)[0].strip().lower()
    if not value or len(value) > 128 or "/" not in value:
        return None
    if any(not (character.isalnum() or character in "!#$&^_.+-/") for character in value):
        return None
    return value


def _extension(filename: str | None) -> str:
    if not filename:
        return ""
    name = filename.rsplit("/", 1)[-1].lower()
    dot = name.rfind(".")
    return name[dot:] if dot > 0 else ""


def _kind(filename: str | None, content_type: str | None, data: bytes) -> MediaKind:
    extension = _extension(filename)
    if content_type == "application/pdf" or extension == ".pdf" or data.startswith(b"%PDF-"):
        return MediaKind.PDF
    if (content_type and content_type.startswith("image/")) or extension in _IMAGE_EXTENSIONS:
        return MediaKind.IMAGE
    if (
        (content_type and content_type.startswith("text/"))
        or content_type in _TEXT_CONTENT_TYPES
        or extension in _TEXT_EXTENSIONS
    ):
        return MediaKind.TEXT
    return MediaKind.UNSUPPORTED


def _unavailable_text(reason: str) -> str:
    # These strings intentionally contain only fixed reason categories.
    return f"[media content unavailable: {reason}]"


def _result(
    *,
    text: str,
    kind: MediaKind,
    status: ExtractionStatus,
    filename: str | None,
    content_type: str | None,
    size_bytes: int,
    truncated: bool = False,
    pages: int | None = None,
    error_type: str | None = None,
    extractor: str | None = None,
) -> MediaExtractionResult:
    metadata: dict[str, object] = {
        "kind": kind.value,
        "status": status.value,
        "filename": filename,
        "content_type": content_type,
        "size_bytes": size_bytes,
        "extracted_chars": len(text) if status == ExtractionStatus.EXTRACTED else 0,
        "truncated": truncated,
        "pages": pages,
        "error_type": error_type,
        "redacted": True,
    }
    if extractor is not None:
        metadata["extractor"] = extractor
    return MediaExtractionResult(text=text, metadata=metadata)


def _safe_text(value: object, max_chars: int) -> tuple[str, bool]:
    if not isinstance(value, str):
        raise TypeError("extractor returned a non-text value")
    if "\x00" in value:
        raise _NulContent
    if any(
        (ord(character) < 32 and character not in _ALLOWED_TEXT_CONTROLS)
        or 0x7F <= ord(character) <= 0x9F
        for character in value
    ):
        raise _BinaryContent
    sanitized = sanitize_text(value)
    return sanitized[:max_chars], len(sanitized) > max_chars


def _classify_backend_error(error: Exception, *, pdf: bool) -> str:
    name = type(error).__name__.lower()
    if isinstance(error, PdfDependencyUnavailable) or "unavailable" in name or "import" in name:
        return "pdf_unavailable" if pdf else "ocr_unavailable"
    if isinstance(error, _PdfEncrypted) or any(
        token in name for token in ("decrypt", "password", "encrypted")
    ):
        return "pdf_encrypted"
    if isinstance(error, _CompressionBomb) or isinstance(error, MemoryError):
        return "compression_bomb"
    if isinstance(error, (_NulContent,)):
        return "nul_byte"
    if isinstance(error, (_BinaryContent,)):
        return "binary_content"
    if pdf and (
        isinstance(error, _PdfCorrupt) or any(token in name for token in ("pdf", "stream"))
    ):
        return "pdf_corrupt"
    if isinstance(error, UnicodeDecodeError):
        return "invalid_utf8"
    return "pdf_extractor_failed" if pdf else "ocr_failed"


def _call_backend(backend: object, data: bytes, keyword_values: Mapping[str, object]) -> object:
    function = getattr(backend, "extract", None)
    if function is None:
        function = getattr(backend, "extract_text", None)
    if function is None and callable(backend):
        function = backend
    if function is None or not callable(function):
        raise TypeError("backend has no extract method")

    # Filter keyword arguments by signature so tiny test fakes can implement
    # ``extract(data)`` while production adapters can accept all limits.
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return cast(Callable[..., object], function)(data)
    parameters = signature.parameters
    accepts_kwargs = any(
        parameter.kind == parameter.VAR_KEYWORD for parameter in parameters.values()
    )
    kwargs = (
        dict(keyword_values)
        if accepts_kwargs
        else {name: value for name, value in keyword_values.items() if name in parameters}
    )
    return cast(Callable[..., object], function)(data, **kwargs)


def _normalize_pdf(value: object, limits: MediaLimits) -> PdfExtraction:
    if isinstance(value, PdfExtraction):
        return value
    if isinstance(value, str):
        return PdfExtraction(value)
    if isinstance(value, Mapping):
        text = value.get("text", "")
        pages = value.get("pages")
        truncated = value.get("truncated", False)
        error_type = value.get("error_type")
        return PdfExtraction(
            text=text if isinstance(text, str) else "",
            pages=pages if isinstance(pages, int) and pages >= 0 else None,
            truncated=bool(truncated),
            error_type=error_type if isinstance(error_type, str) else None,
        )
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        pages = len(value)
        selected = [item for item in value[: limits.max_pdf_pages] if isinstance(item, str)]
        return PdfExtraction(
            "\n\n".join(selected), pages=pages, truncated=pages > limits.max_pdf_pages
        )
    raise TypeError("PDF extractor returned an unsupported value")


def _normalize_ocr(value: object) -> OcrExtraction:
    if isinstance(value, OcrExtraction):
        return value
    if isinstance(value, str):
        return OcrExtraction(value)
    if isinstance(value, Mapping):
        text = value.get("text", "")
        error_type = value.get("error_type")
        return OcrExtraction(
            text=text if isinstance(text, str) else "",
            truncated=bool(value.get("truncated", False)),
            error_type=error_type if isinstance(error_type, str) else None,
        )
    raise TypeError("OCR extractor returned an unsupported value")


class DefaultPdfTextExtractor:
    """Lazy pypdf adapter; pypdf remains optional at import time."""

    def extract(self, data: bytes, *, max_pages: int, max_chars: int) -> PdfExtraction:
        try:
            from pypdf import PdfReader
            from pypdf.errors import FileNotDecryptedError
        except ImportError as error:
            raise PdfDependencyUnavailable from error

        try:
            reader = PdfReader(io.BytesIO(data), strict=False)
            if reader.is_encrypted:
                raise _PdfEncrypted
            pages = len(reader.pages)
            parts: list[str] = []
            remaining = max_chars
            truncated = pages > max_pages
            for page in reader.pages[:max_pages]:
                page_text = page.extract_text() or ""
                if not isinstance(page_text, str):
                    raise TypeError("PDF page text is not text")
                if len(page_text) > remaining:
                    parts.append(page_text[:remaining])
                    truncated = True
                    remaining = 0
                    break
                parts.append(page_text)
                remaining -= len(page_text)
                if remaining == 0:
                    truncated = True
                    break
            return PdfExtraction("\n\n".join(parts), pages=pages, truncated=truncated)
        except FileNotDecryptedError as error:
            raise _PdfEncrypted from error
        except (MemoryError, RecursionError) as error:
            raise _CompressionBomb from error
        except (_PdfEncrypted, _CompressionBomb):
            raise
        except Exception as error:
            raise _PdfCorrupt from error


class MediaExtractor:
    """Extract bounded text from already-downloaded media bytes."""

    def __init__(
        self,
        *,
        limits: MediaLimits | None = None,
        pdf_extractor: PdfTextExtractor | Callable[..., object] | None = None,
        ocr_extractor: OcrExtractor | Callable[..., object] | None = None,
    ) -> None:
        self.limits = limits or MediaLimits()
        self.pdf_extractor = (
            pdf_extractor if pdf_extractor is not None else DefaultPdfTextExtractor()
        )
        self.ocr_extractor = ocr_extractor

    def extract(
        self,
        data: bytes,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> MediaExtractionResult:
        safe_name = _safe_filename(filename, self.limits.max_filename_chars)
        normalized_type = _content_type(content_type)
        try:
            size_bytes = len(data)
        except (TypeError, AttributeError):
            return _result(
                text=_unavailable_text("invalid media bytes"),
                kind=MediaKind.UNSUPPORTED,
                status=ExtractionStatus.REJECTED,
                filename=safe_name,
                content_type=normalized_type,
                size_bytes=0,
                error_type="invalid_input",
            )
        if not isinstance(data, bytes):
            if isinstance(data, (bytearray, memoryview)):
                data = bytes(data)
                size_bytes = len(data)
            else:
                return _result(
                    text=_unavailable_text("invalid media bytes"),
                    kind=MediaKind.UNSUPPORTED,
                    status=ExtractionStatus.REJECTED,
                    filename=safe_name,
                    content_type=normalized_type,
                    size_bytes=size_bytes,
                    error_type="invalid_input",
                )
        kind = _kind(safe_name, normalized_type, data)
        if size_bytes > self.limits.max_bytes:
            return _result(
                text=_unavailable_text("size limit exceeded"),
                kind=kind,
                status=ExtractionStatus.REJECTED,
                filename=safe_name,
                content_type=normalized_type,
                size_bytes=size_bytes,
                error_type="too_large",
            )

        if kind == MediaKind.TEXT:
            return self._extract_text(data, safe_name, normalized_type, size_bytes)
        if kind == MediaKind.PDF:
            return self._extract_pdf(data, safe_name, normalized_type, size_bytes)
        if kind == MediaKind.IMAGE:
            return self._extract_ocr(data, safe_name, normalized_type, size_bytes, kind)
        return _result(
            text=_unavailable_text("unsupported media type"),
            kind=kind,
            status=ExtractionStatus.UNSUPPORTED,
            filename=safe_name,
            content_type=normalized_type,
            size_bytes=size_bytes,
            error_type="unsupported_media",
        )

    def _extract_text(
        self, data: bytes, filename: str | None, content_type: str | None, size_bytes: int
    ) -> MediaExtractionResult:
        try:
            text = data.decode("utf-8-sig")
            bounded, truncated = _safe_text(text, self.limits.max_chars)
        except UnicodeDecodeError:
            return _result(
                text=_unavailable_text("invalid UTF-8"),
                kind=MediaKind.TEXT,
                status=ExtractionStatus.REJECTED,
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                error_type="invalid_utf8",
            )
        except (_NulContent, _BinaryContent) as error:
            error_type = _classify_backend_error(error, pdf=False)
            reason = "NUL byte" if error_type == "nul_byte" else "binary content"
            return _result(
                text=_unavailable_text(reason),
                kind=MediaKind.TEXT,
                status=ExtractionStatus.REJECTED,
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                error_type=error_type,
            )
        return _result(
            text=bounded,
            kind=MediaKind.TEXT,
            status=ExtractionStatus.EXTRACTED,
            filename=filename,
            content_type=content_type,
            size_bytes=size_bytes,
            truncated=truncated,
            extractor="utf-8",
        )

    def _extract_pdf(
        self, data: bytes, filename: str | None, content_type: str | None, size_bytes: int
    ) -> MediaExtractionResult:
        try:
            raw = _call_backend(
                self.pdf_extractor,
                data,
                {"max_pages": self.limits.max_pdf_pages, "max_chars": self.limits.max_chars},
            )
            extraction = _normalize_pdf(raw, self.limits)
            if extraction.error_type:
                return _result(
                    text=_unavailable_text("PDF extraction unavailable"),
                    kind=MediaKind.PDF,
                    status=ExtractionStatus.UNAVAILABLE,
                    filename=filename,
                    content_type=content_type,
                    size_bytes=size_bytes,
                    pages=extraction.pages,
                    error_type=extraction.error_type,
                    extractor="pdf",
                )
            expansion_limit = max(
                self.limits.max_chars * self.limits.max_expansion_ratio,
                size_bytes * self.limits.max_expansion_ratio,
            )
            if len(extraction.text) > expansion_limit:
                raise _CompressionBomb
            bounded, char_truncated = _safe_text(extraction.text, self.limits.max_chars)
            page_truncated = (
                extraction.pages is not None and extraction.pages > self.limits.max_pdf_pages
            )
            truncated = extraction.truncated or char_truncated or page_truncated
            if bounded.strip():
                return _result(
                    text=bounded,
                    kind=MediaKind.PDF,
                    status=ExtractionStatus.EXTRACTED,
                    filename=filename,
                    content_type=content_type,
                    size_bytes=size_bytes,
                    truncated=truncated,
                    pages=extraction.pages,
                    extractor="pdf",
                )
        except Exception as error:
            error_type = _classify_backend_error(error, pdf=True)
            reason = {
                "pdf_unavailable": "PDF extraction unavailable",
                "pdf_encrypted": "encrypted PDF",
                "pdf_corrupt": "corrupt PDF",
                "compression_bomb": "content expansion limit exceeded",
            }.get(error_type, "PDF extraction failed")
            return _result(
                text=_unavailable_text(reason),
                kind=MediaKind.PDF,
                status=(
                    ExtractionStatus.UNAVAILABLE
                    if error_type == "pdf_unavailable"
                    else ExtractionStatus.REJECTED
                    if error_type == "compression_bomb"
                    else ExtractionStatus.FAILED
                ),
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                error_type=error_type,
                extractor="pdf",
            )

        # Empty PDF text is usually a scanned document.  OCR is intentionally
        # attempted only through the injected protocol; without it we state
        # the limitation rather than guessing from image bytes.
        if self.ocr_extractor is None:
            return _result(
                text=_unavailable_text("OCR unavailable for scanned PDF"),
                kind=MediaKind.PDF,
                status=ExtractionStatus.UNAVAILABLE,
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                pages=extraction.pages,
                error_type="ocr_unavailable",
                extractor="pdf",
            )
        return self._extract_ocr(
            data,
            filename,
            content_type,
            size_bytes,
            MediaKind.PDF,
            pages=extraction.pages,
        )

    def _extract_ocr(
        self,
        data: bytes,
        filename: str | None,
        content_type: str | None,
        size_bytes: int,
        kind: MediaKind,
        *,
        pages: int | None = None,
    ) -> MediaExtractionResult:
        if self.ocr_extractor is None:
            return _result(
                text=_unavailable_text("OCR unavailable"),
                kind=kind,
                status=ExtractionStatus.UNAVAILABLE,
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                pages=pages,
                error_type="ocr_unavailable",
            )
        try:
            raw = _call_backend(
                self.ocr_extractor,
                data,
                {
                    "filename": filename,
                    "content_type": content_type,
                    "max_chars": self.limits.max_chars,
                },
            )
            extraction = _normalize_ocr(raw)
            if extraction.error_type:
                error_type = extraction.error_type
                return _result(
                    text=_unavailable_text("OCR unavailable"),
                    kind=kind,
                    status=ExtractionStatus.UNAVAILABLE,
                    filename=filename,
                    content_type=content_type,
                    size_bytes=size_bytes,
                    pages=pages,
                    error_type=error_type,
                    extractor="ocr",
                )
            expansion_limit = max(
                self.limits.max_chars * self.limits.max_expansion_ratio,
                size_bytes * self.limits.max_expansion_ratio,
            )
            if len(extraction.text) > expansion_limit:
                raise _CompressionBomb
            bounded, char_truncated = _safe_text(extraction.text, self.limits.max_chars)
            if not bounded.strip():
                return _result(
                    text=_unavailable_text("OCR returned no text"),
                    kind=kind,
                    status=ExtractionStatus.UNAVAILABLE,
                    filename=filename,
                    content_type=content_type,
                    size_bytes=size_bytes,
                    pages=pages,
                    error_type="ocr_empty",
                    extractor="ocr",
                )
            return _result(
                text=bounded,
                kind=kind,
                status=ExtractionStatus.EXTRACTED,
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                pages=pages,
                truncated=extraction.truncated or char_truncated,
                extractor="ocr",
            )
        except Exception as error:
            error_type = _classify_backend_error(error, pdf=False)
            reason = (
                "content expansion limit exceeded"
                if error_type == "compression_bomb"
                else "OCR failed"
            )
            return _result(
                text=_unavailable_text(reason),
                kind=kind,
                status=ExtractionStatus.REJECTED
                if error_type == "compression_bomb"
                else ExtractionStatus.FAILED,
                filename=filename,
                content_type=content_type,
                size_bytes=size_bytes,
                pages=pages,
                error_type=error_type,
                extractor="ocr",
            )


def extract_media(
    data: bytes,
    filename: str | None = None,
    content_type: str | None = None,
    *,
    limits: MediaLimits | None = None,
    pdf_extractor: PdfTextExtractor | Callable[..., object] | None = None,
    ocr_extractor: OcrExtractor | Callable[..., object] | None = None,
) -> MediaExtractionResult:
    """Convenience wrapper around :class:`MediaExtractor`."""

    return MediaExtractor(
        limits=limits, pdf_extractor=pdf_extractor, ocr_extractor=ocr_extractor
    ).extract(data, filename, content_type)


extract_content = extract_media
MediaContentExtractor = MediaExtractor


__all__ = [
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_CHARS",
    "DEFAULT_MAX_PDF_PAGES",
    "DefaultPdfTextExtractor",
    "ExtractionStatus",
    "MediaContentExtractor",
    "MediaExtractionLimits",
    "MediaExtractionResult",
    "MediaExtractor",
    "MediaKind",
    "MediaLimits",
    "OCRExtractor",
    "OcrExtraction",
    "OcrExtractor",
    "PdfExtraction",
    "PdfExtractor",
    "PdfTextExtractor",
    "extract_content",
    "extract_media",
]
