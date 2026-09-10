"""Validation helpers for identifiers that cross storage or protocol boundaries."""

from __future__ import annotations

import re

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def validate_identifier(value: str, field: str = "identifier") -> str:
    value = str(value)
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid {field}")
    return value


def filesystem_component(value: str, field: str = "identifier") -> str:
    return validate_identifier(value, field)
