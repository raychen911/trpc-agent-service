"""Text chunking utilities that preserve encoding and sensible boundaries."""

from __future__ import annotations

import unicodedata


def split_utf8_bytes(text: str, *, max_bytes: int) -> tuple[str, ...]:
    """Split text without breaking UTF-8 code points or exceeding a byte limit."""

    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if not text:
        return ()

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining.encode("utf-8")) <= max_bytes:
            chunks.append(remaining)
            break
        end = _largest_utf8_prefix(remaining, max_bytes)
        end = _preferred_boundary(remaining, end)
        if end == 0:
            end = _largest_utf8_prefix(remaining, max_bytes)
        chunks.append(remaining[:end])
        remaining = remaining[end:]
    return tuple(chunks)


def split_telegram_text(text: str, *, max_characters: int = 4096) -> tuple[str, ...]:
    """Split Telegram text while avoiding common grapheme continuation points."""

    if max_characters < 1:
        raise ValueError("max_characters must be positive")
    if not text:
        return ()

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_characters:
            chunks.append(remaining)
            break
        end = _preferred_boundary(remaining, max_characters)
        if end == 0:
            end = max_characters
        end = _avoid_grapheme_continuation(remaining, end)
        if end == 0:
            end = max_characters
        chunks.append(remaining[:end])
        remaining = remaining[end:]
    return tuple(chunks)


def _largest_utf8_prefix(text: str, max_bytes: int) -> int:
    low, high = 1, len(text)
    best = 0
    while low <= high:
        middle = (low + high) // 2
        if len(text[:middle].encode("utf-8")) <= max_bytes:
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    if best == 0:
        raise ValueError("max_bytes is smaller than the first UTF-8 code point")
    return best


def _preferred_boundary(text: str, end: int) -> int:
    """Prefer a nearby newline or whitespace without producing tiny chunks."""

    floor = max(1, end // 2)
    newline = text.rfind("\n", floor, end + 1)
    if newline >= floor:
        return newline + 1
    for index in range(end, floor - 1, -1):
        if text[index - 1].isspace():
            return index
    return end


def _avoid_grapheme_continuation(text: str, end: int) -> int:
    """Avoid splitting directly around combining marks, variation selectors, or ZWJ."""

    while end > 0 and end < len(text):
        current = text[end]
        previous = text[end - 1]
        if (
            unicodedata.combining(current)
            or current in {"\ufe0e", "\ufe0f", "\u200d"}
            or previous == "\u200d"
        ):
            end -= 1
            continue
        break
    return end
