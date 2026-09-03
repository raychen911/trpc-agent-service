"""
Log package initialization.
"""

from trpc_service.log.logger import (
    MaskingLogFormatter,
    mask_sensitive_text,
    setup_logger,
    app_logger,
)

__all__ = [
    "MaskingLogFormatter",
    "mask_sensitive_text",
    "setup_logger",
    "app_logger",
]
