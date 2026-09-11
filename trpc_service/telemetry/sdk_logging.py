"""Use the public SDK logger interface without forwarding diagnostic bodies."""

import logging

from trpc_agent_sdk.log import BaseLogger, LogLevel, set_default_logger


class SafeSDKLogger(BaseLogger):
    """Raw SDK diagnostics may contain provider URLs, prompts and exception bodies.

    Detailed operational evidence belongs to our typed audit/metrics pipeline. The
    SDK adapter retains severity only, including when given extra fields or exc_info.
    """

    def __init__(self):
        super().__init__(name="platform_safe_sdk", min_level=LogLevel.WARNING)

    def debug(self, format_str, *args, **kwargs):
        pass

    def info(self, format_str, *args, **kwargs):
        pass

    def warning(self, format_str, *args, **kwargs):
        from .logging import emit
        emit("sdk.warning", level=logging.WARNING)

    def error(self, format_str, *args, **kwargs):
        from .logging import emit
        emit("sdk.error", level=logging.ERROR)

    def fatal(self, format_str, *args, **kwargs):
        from .logging import emit
        emit("sdk.fatal", level=logging.CRITICAL)


def configure_sdk_logging():
    set_default_logger(SafeSDKLogger())
