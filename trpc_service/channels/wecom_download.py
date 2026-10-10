"""Bounded, encrypted media downloads for Enterprise WeChat AI Bot."""

from __future__ import annotations

import asyncio
import base64
import binascii
import ipaddress
import math
import re
import time
from datetime import UTC
from email.utils import parsedate_to_datetime
from urllib.parse import unquote, urlsplit

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

_DEFAULT_CHUNK_BYTES = 64 * 1024
_MAX_FILENAME_BYTES = 512


class WeComDownloadError(RuntimeError):
    """Safe download failure with stable provider metadata."""

    def __init__(
        self,
        code: str,
        *,
        retryable: bool = False,
        status: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__("WeCom media download failed")
        self.code = code
        self.provider_code = code
        self.retryable = retryable
        self.status = status
        self.status_code = status
        self.retry_after_seconds = retry_after_seconds


WeComMediaDownloadError = WeComDownloadError


class BoundedWeComDownloadClient:
    """Download and decrypt one bounded WeCom media response at a time."""

    def __init__(
        self,
        max_plaintext_bytes: int,
        timeout_seconds: float,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        chunk_bytes: int = _DEFAULT_CHUNK_BYTES,
    ) -> None:
        if max_plaintext_bytes <= 0:
            raise ValueError("max_plaintext_bytes must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        self._max_plaintext_bytes = max_plaintext_bytes
        self._max_ciphertext_bytes = ((max_plaintext_bytes + 1 + 31) // 32) * 32
        self._chunk_bytes = chunk_bytes
        self._http = httpx.AsyncClient(
            transport=transport,
            timeout=timeout_seconds,
            follow_redirects=False,
            trust_env=False,
            headers={"Accept-Encoding": "identity"},
        )

    async def download_file(self, url: str, aes_key: str) -> tuple[bytes, str | None]:
        """Fetch, bound, decrypt, and return ``(plaintext, safe_filename)``."""

        _validate_url(url)
        key = _decode_aes_key(aes_key)
        try:
            async with self._http.stream(
                "GET",
                url,
                headers={"Accept-Encoding": "identity"},
            ) as response:
                status = response.status_code
                if not 200 <= status < 300:
                    raise _http_status_error(status, response.headers.get("Retry-After"))

                content_length = _content_length(response.headers.get("Content-Length"))
                if content_length is not None and content_length > self._max_ciphertext_bytes:
                    raise WeComDownloadError("media_too_large", status=status)

                ciphertext = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=self._chunk_bytes):
                    if len(ciphertext) + len(chunk) > self._max_ciphertext_bytes:
                        raise WeComDownloadError("media_too_large", status=status)
                    ciphertext.extend(chunk)
                filename = _content_disposition_filename(
                    response.headers.get("Content-Disposition")
                )
        except WeComDownloadError:
            raise
        except asyncio.CancelledError:
            raise
        except httpx.TimeoutException:
            raise WeComDownloadError("transport_timeout", retryable=True) from None
        except (httpx.NetworkError, httpx.ProtocolError):
            raise WeComDownloadError("transport_error", retryable=True) from None
        except httpx.HTTPError:
            raise WeComDownloadError("transport_error", retryable=True) from None
        except Exception:
            raise WeComDownloadError("transport_error", retryable=True) from None

        plaintext = _decrypt(key, bytes(ciphertext))
        if len(plaintext) > self._max_plaintext_bytes:
            raise WeComDownloadError("media_too_large")
        return plaintext, filename

    async def close(self) -> None:
        await self._http.aclose()

    async def disconnect(self) -> None:
        await self.close()


def _validate_url(url: str) -> None:
    if not isinstance(url, str):
        raise WeComDownloadError("invalid_url")
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        valid = (
            parsed.scheme.lower() == "https"
            and bool(hostname)
            and parsed.username is None
            and parsed.password is None
        )
        if valid and hostname is not None:
            try:
                address = ipaddress.ip_address(hostname)
            except ValueError:
                pass
            else:
                valid = address.is_global
    except ValueError:
        valid = False
    if not valid:
        raise WeComDownloadError("invalid_url")


def _decode_aes_key(value: str) -> bytes:
    if not isinstance(value, str) or not value or any(char.isspace() for char in value):
        raise WeComDownloadError("invalid_key")
    if len(value) % 4 == 1:
        raise WeComDownloadError("invalid_key")
    padded = value + "=" * (-len(value) % 4)
    try:
        key = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError, TypeError):
        raise WeComDownloadError("invalid_key") from None
    if len(key) != 32:
        raise WeComDownloadError("invalid_key")
    return key


def _decrypt(key: bytes, ciphertext: bytes) -> bytes:
    if not ciphertext or len(ciphertext) % 16:
        raise WeComDownloadError("decrypt_failed")
    try:
        decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
        plaintext = decryptor.update(ciphertext) + decryptor.finalize()
    except ValueError:
        raise WeComDownloadError("decrypt_failed") from None
    padding = plaintext[-1]
    if padding < 1 or padding > 32 or padding > len(plaintext):
        raise WeComDownloadError("decrypt_failed")
    if plaintext[-padding:] != bytes([padding]) * padding:
        raise WeComDownloadError("decrypt_failed")
    return plaintext[:-padding]


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        length = int(value)
    except (TypeError, ValueError):
        return None
    return length if length >= 0 else None


def _content_disposition_filename(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    extended = re.search(
        r"(?:^|;)\s*filename\*\s*=\s*(?:\"([^\"]*)\"|([^;]+))",
        value,
        flags=re.IGNORECASE,
    )
    if extended:
        encoded = (extended.group(1) or extended.group(2) or "").strip()
        pieces = encoded.split("'", 2)
        encoded = pieces[2] if len(pieces) == 3 else encoded
        filename = _safe_filename(unquote(encoded))
        if filename:
            return filename
    standard = re.search(
        r"(?:^|;)\s*filename\s*=\s*(?:\"([^\"]*)\"|([^;]+))",
        value,
        flags=re.IGNORECASE,
    )
    if not standard:
        return None
    return _safe_filename(standard.group(1) or standard.group(2))


def _safe_filename(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.replace("\x00", "").strip().strip('"')
    value = value.replace("\\", "/").rsplit("/", 1)[-1]
    if not value or any(ord(char) < 32 for char in value):
        return None
    return value[:_MAX_FILENAME_BYTES]


def _http_status_error(status: int, retry_after: str | None = None) -> WeComDownloadError:
    if 300 <= status < 400:
        return WeComDownloadError("redirect_rejected", status=status)
    if status == 404:
        return WeComDownloadError("media_not_found", status=status)
    if status == 429:
        return WeComDownloadError(
            "rate_limited",
            retryable=True,
            status=status,
            retry_after_seconds=_retry_after_seconds(retry_after),
        )
    if status >= 500:
        return WeComDownloadError("upstream_unavailable", retryable=True, status=status)
    return WeComDownloadError("upstream_rejected", status=status)


def _retry_after_seconds(raw: str | None, *, now: float | None = None) -> float | None:
    """Parse a provider Retry-After value without trusting unbounded input."""

    if not isinstance(raw, str) or not raw.strip():
        return None
    value = raw.strip()
    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        try:
            seconds = retry_at.timestamp() - (time.time() if now is None else now)
        except (OSError, OverflowError, ValueError):
            return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, 3600.0)


__all__ = [
    "BoundedWeComDownloadClient",
    "WeComDownloadError",
    "WeComMediaDownloadError",
]
