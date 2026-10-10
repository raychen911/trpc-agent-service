#!/usr/bin/env python3
"""Fail-closed, atomic JSON report writes for acceptance evidence."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _reject_symlink_path(path: Path) -> None:
    absolute = path.absolute()
    if absolute.is_symlink():
        raise ValueError(f"report path must not be a symlink: {path}")
    for parent in absolute.parents:
        if parent.exists() and parent.is_symlink():
            raise ValueError(f"report parent must not be a symlink: {parent}")


def atomic_write_json(path: Path, value: Any, *, indent: int = 2) -> str:
    """Serialize strict JSON and atomically replace a non-symlink target."""

    rendered = (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=indent,
            allow_nan=False,
        )
        + "\n"
    )
    _reject_symlink_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_path(path)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(rendered)
            temporary.flush()
            os.fsync(temporary.fileno())
        _reject_symlink_path(path)
        os.replace(temporary_name, path)
        temporary_name = None
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink()
            except FileNotFoundError:
                pass
    return rendered
