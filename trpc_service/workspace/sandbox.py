"""
Workspace Sandbox Environment.
Provides isolated directory and execution spaces for tenant tool operations.
"""

import os
import shutil
from pathlib import Path
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class WorkspaceSandbox:
    """
    Isolated local workspace for a tenant or session to prevent directory traversal and data leakage.
    """

    def __init__(self, base_dir: str = "./data/workspaces"):
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def get_tenant_workspace(self, tenant_id: str, session_id: Optional[str] = None) -> Path:
        """Get or create isolated workspace directory for tenant."""
        safe_tenant = "".join(c for c in tenant_id if c.isalnum() or c in "_-")
        if session_id:
            safe_session = "".join(c for c in session_id if c.isalnum() or c in "_-")
            path = self.base_dir / safe_tenant / safe_session
        else:
            path = self.base_dir / safe_tenant

        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_file(self, tenant_id: str, filename: str, content: str, session_id: Optional[str] = None) -> str:
        """Safely write file within tenant workspace boundary."""
        ws = self.get_tenant_workspace(tenant_id, session_id)
        # Prevent directory traversal
        target = (ws / filename).resolve()
        if not str(target).startswith(str(ws)):
            raise PermissionError(f"Directory traversal attack detected for filename: {filename}")

        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as f:
            f.write(content)
        return str(target)

    def read_file(self, tenant_id: str, filename: str, session_id: Optional[str] = None) -> str:
        """Safely read file within tenant workspace boundary."""
        ws = self.get_tenant_workspace(tenant_id, session_id)
        target = (ws / filename).resolve()
        if not str(target).startswith(str(ws)):
            raise PermissionError(f"Access outside tenant workspace denied: {filename}")

        if not target.exists():
            raise FileNotFoundError(f"File '{filename}' not found in workspace.")
        with open(target, "r", encoding="utf-8") as f:
            return f.read()

    def cleanup_workspace(self, tenant_id: str, session_id: Optional[str] = None):
        """Clean temporary workspace directory after session ends."""
        ws = self.get_tenant_workspace(tenant_id, session_id)
        if ws.exists():
            shutil.rmtree(ws, ignore_errors=True)


workspace_sandbox = WorkspaceSandbox()
