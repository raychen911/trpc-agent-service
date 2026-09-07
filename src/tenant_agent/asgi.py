"""Import-safe ASGI entry point for servers that do not use the operations CLI."""

from tenant_agent.main import create_app

app = create_app()
