"""Opt-in synthetic message API used by private acceptance environments."""

from ._router import TestMessageRequest
from ._router import create_test_message_router

__all__ = ["TestMessageRequest", "create_test_message_router"]
