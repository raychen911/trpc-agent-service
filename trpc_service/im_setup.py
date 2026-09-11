"""Local first-use secret entry. No network requests, plaintext terminal output or shell interpolation."""

import getpass
import json
import os
from pathlib import Path
import secrets
import re
import warnings
from urllib.parse import urlsplit

from cryptography.fernet import Fernet

ENV_NAMES = frozenset({
    "TRPC_TELEGRAM_BOT_TOKEN", "TRPC_TELEGRAM_BOT_ID", "TRPC_WECOM_BOT_SECRET", "TRPC_WECOM_BOT_ID",
    "TRPC_MODEL_API_KEY", "TRPC_MODEL_NAME", "TRPC_MODEL_BASE_URL", "TRPC_IDENTITY_KEY", "TRPC_IM_CONTEXT_KEYS",
    "TRPC_FEISHU_APP_ID", "TRPC_FEISHU_APP_SECRET"
})


def save_bundle(path, values):
    if not isinstance(values, dict) or set(values) - ENV_NAMES or any(
            not isinstance(value, str) or not value or "\x00" in value for value in values.values()):
        raise ValueError("invalid local IM configuration")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation protects the existing identity/encryption keys on repeat setup.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as target:
        json.dump(values, target, ensure_ascii=False, indent=2)


def load_bundle(path):
    try:
        raw = Path(path).read_text(encoding="utf-8")
        if len(raw) > 65536:
            raise ValueError("configuration is too large")
        values = json.loads(raw)
        if not isinstance(values, dict) or set(values) - ENV_NAMES or any(
                not isinstance(value, str) or not value or "\x00" in value for value in values.values()):
            raise ValueError("invalid local IM configuration")
    except (ValueError, TypeError, OSError):
        raise ValueError("local IM configuration cannot be loaded") from None
    os.environ.update(values)


def configure(path=".secrets/im.json"):
    if Path(path).exists():
        raise ValueError("本地配置已存在，请编辑该文件；不要重新生成身份和加密密钥。")
    print("只保存到本机，不连接机器人。暂时没有的项目可以直接回车跳过。")
    values = {}
    token = getpass.getpass("Telegram BotFather 返回的 Token（输入隐藏）：").strip()
    if token:
        account, separator, secret = token.partition(":")
        if not separator or not account.isdecimal() or not secret:
            raise ValueError("Telegram Token 格式不正确，未保存。")
        values.update(TRPC_TELEGRAM_BOT_TOKEN=token, TRPC_TELEGRAM_BOT_ID=account)
    bot_id = input("企业微信 Bot ID：").strip()
    if bot_id:
        secret = getpass.getpass("企业微信 Secret（输入隐藏）：").strip()
        if not secret:
            raise ValueError("填写企微 Bot ID 后需要同时填写 Secret，未保存。")
        values.update(TRPC_WECOM_BOT_ID=bot_id, TRPC_WECOM_BOT_SECRET=secret)
    model = input("模型名称（例如你已开通的模型标识）：").strip()
    if model:
        endpoint = input("模型 API Base URL：").strip()
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment):
            raise ValueError("模型地址必须为不带密钥、用户名或查询参数的 HTTP(S) 地址，未保存。")
        key = getpass.getpass("模型 API Key（输入隐藏）：").strip()
        if not endpoint or not key:
            raise ValueError("模型名称、API 地址和 Key 需要同时填写，未保存。")
        values.update(TRPC_MODEL_NAME=model, TRPC_MODEL_BASE_URL=endpoint, TRPC_MODEL_API_KEY=key)
    values.update(TRPC_IDENTITY_KEY=secrets.token_urlsafe(48), TRPC_IM_CONTEXT_KEYS=Fernet.generate_key().decode())
    save_bundle(path, values)
    print(f"已保存到 {Path(path).resolve()}。该文件含密钥，仅用于本地开发；请保管好，不要提交或分享。")


def configure_feishu(path=".secrets/feishu.json", *, app_id=None):
    """Add a separate channel bundle without replacing existing identity/model keys."""
    if Path(path).exists():
        raise ValueError("飞书配置已存在，本次未覆盖。")
    app_id = (app_id or input("飞书 App ID：")).strip()
    if not re.fullmatch(r"cli_[A-Za-z0-9]{1,124}", app_id):
        raise ValueError("飞书 App ID 格式不正确，未保存。")
    print("请在本机输入飞书 App Secret。输入内容不会显示；本步骤只保存配置，不连接飞书。")
    try:
        with warnings.catch_warnings():
            # Refuse getpass's echoed-stdin fallback when no real terminal exists.
            warnings.simplefilter("error", getpass.GetPassWarning)
            secret = getpass.getpass("飞书 App Secret（输入隐藏，粘贴后按回车）：").strip()
    except (getpass.GetPassWarning, EOFError):
        raise ValueError("当前终端不支持隐藏输入，请在本机 PowerShell 中运行，未保存。") from None
    if not secret or len(secret) > 4096 or any(ord(char) < 32 or ord(char) == 127 for char in secret):
        raise ValueError("飞书 App Secret 为空或格式不正确，未保存。")
    save_bundle(path, {"TRPC_FEISHU_APP_ID": app_id, "TRPC_FEISHU_APP_SECRET": secret})
    print("飞书凭据已保存到本机配置文件。尚未验证连接；请勿提交或分享此文件。")
