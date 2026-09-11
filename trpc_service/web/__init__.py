"""FastAPI application and HTTP schemas.

The application is deliberately constructed by :func:`create_app`, rather
than at module import time.  Workers and the Alembic CLI share
``ServiceContainer`` from this module; eagerly constructing an HTTP app would
incorrectly apply HTTP-only production authentication validation to those
non-HTTP processes.
"""

from .app import create_app

__all__ = ["create_app"]
