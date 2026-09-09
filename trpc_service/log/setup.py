# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Service logging setup."""

import logging
from trpc_service.log.audit import mask_sensitive_text


class RedactingFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> str:
        return mask_sensitive_text(super().format(record))


def configure_logging(level: str = "INFO") -> None:
    """Configure a concise timestamped format without request payloads."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )
    formatter = RedactingFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
