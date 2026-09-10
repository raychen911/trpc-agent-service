"""Tenant-scoped workspace and sandbox integration boundary.

The actual sandbox implementation is supplied by tRPC-Agent-Python. This
package is the platform-owned location for future per-tenant resource and
filesystem policies, keeping the documented project layout stable.
"""

from .policy import WorkspacePolicy

__all__ = ["WorkspacePolicy"]
