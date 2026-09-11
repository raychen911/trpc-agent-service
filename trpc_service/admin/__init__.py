"""Standalone Admin API for tenant configuration management."""

from trpc_service.admin.app import create_admin_app
from trpc_service.admin.auth import AdminToken

__all__ = ["AdminToken", "create_admin_app"]
