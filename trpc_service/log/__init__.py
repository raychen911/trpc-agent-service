import logging

from .redaction import redact, redact_text

logger = logging.getLogger("trpc_service")


def configure_logging(level: str = "INFO") -> None:
    """Configure service logs for CLI deployments without duplicating external handlers."""
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False


__all__ = ["configure_logging", "logger", "redact", "redact_text"]
