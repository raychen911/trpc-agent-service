"""Boundary and Unicode tests for channel text chunkers."""

from __future__ import annotations

import pytest

from trpc_service.channels.text import split_telegram_text, split_utf8_bytes


def test_split_utf8_bytes_reassembles_and_respects_limit() -> None:
    text = ("第一段\uff1a你好 👋\n" * 10) + ("第二段 agent " * 10)

    chunks = split_utf8_bytes(text, max_bytes=64)

    assert "".join(chunks) == text
    assert all(len(chunk.encode("utf-8")) <= 64 for chunk in chunks)
    assert len(chunks) > 1


def test_split_utf8_bytes_empty_and_exact_boundary() -> None:
    assert split_utf8_bytes("", max_bytes=1) == ()
    assert split_utf8_bytes("你好", max_bytes=6) == ("你好",)


def test_split_utf8_bytes_rejects_invalid_or_impossible_limit() -> None:
    with pytest.raises(ValueError, match="positive"):
        split_utf8_bytes("text", max_bytes=0)
    with pytest.raises(ValueError, match="first UTF-8"):
        split_utf8_bytes("你", max_bytes=2)


def test_telegram_chunks_reassemble_and_prefer_whitespace() -> None:
    text = "alpha beta gamma\ndelta epsilon"

    chunks = split_telegram_text(text, max_characters=12)

    assert "".join(chunks) == text
    assert all(len(chunk) <= 12 for chunk in chunks)
    assert chunks[0].endswith(" ")


def test_telegram_avoids_common_combining_and_zwj_boundaries() -> None:
    combining = "abcde\u0301fghij"
    family = "start " + "👩\u200d💻" + " finish"

    combining_chunks = split_telegram_text(combining, max_characters=6)
    family_chunks = split_telegram_text(family, max_characters=8)

    assert "".join(combining_chunks) == combining
    assert not combining_chunks[1].startswith("\u0301")
    assert "".join(family_chunks) == family
    assert all(not chunk.endswith("\u200d") for chunk in family_chunks)


def test_telegram_empty_exact_and_invalid_limit() -> None:
    assert split_telegram_text("", max_characters=1) == ()
    assert split_telegram_text("abc", max_characters=3) == ("abc",)
    with pytest.raises(ValueError, match="positive"):
        split_telegram_text("x", max_characters=0)
