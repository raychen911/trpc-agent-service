# ===================================================================
# config.redaction - 密钥 / PII 脱敏工具
# ===================================================================
# 说明: 日志、trace、错误报告中禁止出现密钥明文（PRD 4.5）。
#   提供两类脱敏:
#   1. mask_secret: 密钥字段脱敏（api_key / token / secret / password / dsn 密码）
#   2. Redactor: 按 PII 规则正则替换（手机号 / 身份证 / 银行卡 / 邮箱 / API Key）
# 规范: 日志输出前统一走 redact()，trace attribute 写入前走 mask_secret()
# ===================================================================

from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"

# 默认 PII 规则（settings.PiiSettings 未自定义时使用同一份，避免双处维护）
# 注: dsn_* 两条置于 email 之前——DSN 内嵌凭据优先整体脱敏，避免 email
#   规则只吞掉域名段而漏掉密码（审查 09-04 补漏：文本路径原先不含 DSN 规则）。
DEFAULT_PII_PATTERNS: dict[str, str] = {
    "phone": r"1[3-9]\d{9}",
    "id_card": r"\d{17}[\dXx]",
    "bank_card": r"\d{16,19}",
    "dsn_cred": r"(?<=://)[^:/@\s]+:[^@\s]+(?=@)",
    "dsn_password_only": r"(?<=://):[^@\s]+(?=@)",
    "email": r"[\w.+-]+@[\w-]+\.[\w.]+",
    "api_key": r"(sk|pk|AKIA)[A-Za-z0-9_-]{16,}",
}

# 密钥字段名 -> 值整体脱敏（保留前缀用于辨识）
_SECRET_KEYWORDS = ("api_key", "apikey", "token", "secret", "password", "passwd", "key")

# DSN 中内嵌凭据脱敏: redis://user:pass@host -> redis://user:***@host
_DSN_CRED_RE = re.compile(r"(://)([^:/@\s]+):([^@\s]+)@")
# 无用户名形态: redis://:pass@host（审查 09-04 补漏——此前该形态漏脱敏）
_DSN_PASSWORD_ONLY_RE = re.compile(r"(://):([^@\s]+)@")


def mask_secret(value: Any, key: str | None = None) -> Any:
    """密钥值脱敏: 命中密钥字段名则整体替换，DSN 内嵌密码单独替换。

    Args:
        value: 待脱敏值
        key: 字段名（可选，用于判断是否为密钥字段）

    Returns:
        脱敏后的值（非 str 原样返回）。
    """
    if not isinstance(value, str) or not value:
        return value
    if key and any(kw in key.lower() for kw in _SECRET_KEYWORDS):
        # 保留前 4 位便于定位，如 sk-abc -> sk-***[REDACTED]
        head = value[:4]
        return f"{head}***{REDACTED}" if len(value) > 8 else REDACTED
    value = _DSN_CRED_RE.sub(r"\1\2:***@", value)
    return _DSN_PASSWORD_ONLY_RE.sub(r"\1:***@", value)


def _deep_mask(obj: Any) -> Any:
    """递归脱敏 dict / list / 标量。"""
    if isinstance(obj, dict):
        return {k: mask_secret(v, k) if isinstance(v, str) else _deep_mask(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_deep_mask(v) for v in obj]
    return obj


class Redactor:
    """PII 规则脱敏器: 按配置的规则名 -> 正则替换文本中的敏感信息。"""

    def __init__(self, patterns: dict[str, str] | None = None, enabled: bool = True) -> None:
        self.enabled = enabled
        # patterns 为 None 时使用内置默认规则集
        self._compiled: list[tuple[str, re.Pattern]] = [(name, re.compile(pattern))
                                                        for name, pattern in (patterns or DEFAULT_PII_PATTERNS).items()]

    def redact(self, text: str) -> str:
        """对文本执行全部 PII 规则替换。"""
        if not self.enabled or not text:
            return text
        for _, pattern in self._compiled:
            text = pattern.sub(REDACTED, text)
        return text

    def redact_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        """对 dict 的字符串值逐字段脱敏（密钥字段 + PII 文本）。"""
        result: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, str):
                masked = mask_secret(value, key)
                result[key] = self.redact(masked)
            elif isinstance(value, dict):
                result[key] = self.redact_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self.redact_dict(item)
                    if isinstance(item, dict) else self.redact(item) if isinstance(item, str) else item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    def __call__(self, text: str) -> str:
        return self.redact(text)


def redact(text: str, patterns: dict[str, str] | None = None, enabled: bool = True) -> str:
    """便捷函数: 对单段文本做 PII 脱敏（patterns 为 None 时用内置默认规则）。"""
    return Redactor(patterns, enabled).redact(text)


def redact_secrets(data: dict[str, Any]) -> dict[str, Any]:
    """便捷函数: 对 dict 做密钥字段脱敏（不依赖 PII 规则）。"""
    return _deep_mask(data)
