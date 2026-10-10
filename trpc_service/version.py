"""Package version."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("trpc-agent-service")
except PackageNotFoundError:  # pragma: no cover - source checkout before install
    __version__ = "0.1.0"

__all__ = ["__version__"]
