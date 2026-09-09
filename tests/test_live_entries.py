"""Explicit live-test entry guards.

These tests are skipped unless the matching credentials are supplied. They are
kept separate so default pytest cannot create model cost or send real IM data.
"""

import os

import pytest

pytestmark = pytest.mark.live


def test_model_live_credentials_are_explicit():
    if not os.getenv("TRPC_AGENT_API_KEY"):
        pytest.skip("TRPC_AGENT_API_KEY is not set; no model cost was incurred")
    assert os.getenv("TRPC_AGENT_MODEL_NAME"), "TRPC_AGENT_MODEL_NAME must be explicit"


def test_wecom_live_credentials_are_explicit():
    if not os.getenv("WECOM_BOT_SECRET"):
        pytest.skip("WECOM_BOT_SECRET is not set; no real WeCom connection was opened")
    assert os.getenv("WECOM_BOT_ID"), "WECOM_BOT_ID must be explicit"


def test_wecom_kf_live_credentials_are_explicit():
    if not os.getenv("WECOM_KF_SECRET"):
        pytest.skip("WECOM_KF_SECRET is not set; no real customer-service message was sent")
    for name in ("WECOM_KF_CORP_ID", "WECOM_KF_OPEN_KFID", "WECOM_KF_TEST_EXTERNAL_USER_ID"):
        assert os.getenv(name), f"{name} must be explicit"


def test_telegram_live_credentials_are_explicit():
    if not os.getenv("TELEGRAM_BOT_TOKEN"):
        pytest.skip("TELEGRAM_BOT_TOKEN is not set; no real message was sent")
    assert os.getenv("TELEGRAM_TEST_CHAT_ID"), "TELEGRAM_TEST_CHAT_ID must be explicit"
