"""Web package exports without importing and constructing the app eagerly."""

from __future__ import annotations

from typing import Any

__all__ = ["create_app"]


def create_app(*args: Any, **kwargs: Any):
    """Lazily import the FastAPI factory.

    CLI worker processes also import modules below ``trpc_service.web``.
    Avoid constructing an unused application runtime during package import.
    """

    from trpc_service.web.app import create_app as app_factory

    return app_factory(*args, **kwargs)
