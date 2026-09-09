# ===================================================================
# log.logger - 平台日志器（结构化 JSON + PII 脱敏）
# ===================================================================
# 说明: 基于标准库 logging，输出结构化 JSON 日志，便于采集与审计追溯。
#   所有日志消息经 PII 脱敏（手机号/身份证/邮箱/密钥），密钥不落盘（PRD 4.5）。
# 规范: 模块内使用 get_logger(__name__)；初始化一次 setup_logging()。
# ===================================================================

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ..config.redaction import DEFAULT_PII_PATTERNS, Redactor

# 统一 logger 名空间，避免与框架 logger 冲突
_LOGGER_NAMESPACE = "teneuris"

# 日志保留字段（对齐审计需求，PRD 4.4）
_RESERVED_KEYS = ("message", "level", "logger", "timestamp", "trace_id", "tenant_id")


def _redact_value(value: Any, key: str, redactor: Redactor) -> Any:
    """对单值脱敏：密钥字段整体掩码，字符串按 PII 规则替换。"""
    if isinstance(value, str):
        if any(kw in key.lower() for kw in ("api_key", "token", "secret", "password", "passwd")):
            head = value[:4]
            return f"{head}***[REDACTED]" if len(value) > 8 else "[REDACTED]"
        return redactor.redact(value)
    return value


def _redact_extra(extra: dict[str, Any], redactor: Redactor) -> dict[str, Any]:
    """递归脱敏 extra 字段。"""
    result: dict[str, Any] = {}
    for key, value in extra.items():
        if isinstance(value, dict):
            result[key] = _redact_extra(value, redactor)
        elif isinstance(value, list):
            result[key] = [
                _redact_extra(item, redactor) if isinstance(item, dict) else _redact_value(item, key, redactor)
                for item in value
            ]
        else:
            result[key] = _redact_value(value, key, redactor)
    return result


class JsonFormatter(logging.Formatter):
    """结构化 JSON 日志格式化器。

    输出形如:
        {"timestamp":"2026-08-24T12:00:00Z","level":"INFO","logger":"teneuris.runtime",
         "message":"agent run","trace_id":"...","tenant_id":"t1"}
    """

    def __init__(self, redactor: Optional[Redactor] = None, include_extra: bool = True) -> None:
        super().__init__()
        self._redactor = redactor or Redactor(DEFAULT_PII_PATTERNS)
        self._include_extra = include_extra

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": self._redactor.redact(record.getMessage()),
        }
        if self._include_extra and hasattr(record, "extra_fields"):
            extra = _redact_extra(record.extra_fields, self._redactor)
            entry.update(extra)
        # 异常堆栈保留，但必须先过脱敏（DB DSN / api key 常出现在 traceback，
        # 审查 09-04：此前 formatException 未脱敏属密钥泄漏旁路）
        if record.exc_info:
            entry["exc_info"] = self._redactor.redact(self.formatException(record.exc_info))
        return json.dumps(entry, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """纯文本日志格式化器（非 JSON 路径），消息同样过 PII 脱敏。

    审查 09-04：此前 json_output=False 时直接用标准 Formatter，
    消息/堆栈完全绕过脱敏（密钥泄漏旁路）。
    """

    def __init__(self, fmt: str, redactor: Optional[Redactor] = None) -> None:
        super().__init__(fmt)
        self._redactor = redactor or Redactor(DEFAULT_PII_PATTERNS)

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        redacted = self._redactor.redact(text)
        if record.exc_info:
            # formatException 的堆栈附加在末尾，统一对全文再脱敏一次
            redacted = self._redactor.redact(redacted)
        return redacted


def get_logger(name: str = "app") -> logging.Logger:
    """获取平台 logger（命名空间 teneuris.{name}）。"""
    full_name = f"{_LOGGER_NAMESPACE}.{name}" if name != _LOGGER_NAMESPACE else name
    return logging.getLogger(full_name)


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_output: bool = True,
    redactor: Optional[Redactor] = None,
) -> None:
    """初始化平台日志。

    Args:
        level: DEBUG / INFO / WARNING / ERROR
        log_file: 可选日志文件路径（同时输出到文件）
        json_output: 是否输出 JSON 结构化日志（False 输出纯文本）
        redactor: 自定义 PII 脱敏器（默认使用内置规则）
    """
    root = logging.getLogger(_LOGGER_NAMESPACE)
    root.setLevel(level.upper())
    # 清除已有 handler，避免重复初始化
    for handler in list(root.handlers):
        root.removeHandler(handler)

    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    formatter = JsonFormatter(redactor) if json_output else TextFormatter(fmt, redactor)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    root.propagate = False


def bind_logger(
    logger: logging.Logger,
    *,
    trace_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    session_id: Optional[str] = None,
    **fields: Any,
) -> logging.LoggerAdapter:
    """绑定上下文字段的 LoggerAdapter（trace/tenant/session 贯穿日志）。

    Example:
        log = bind_logger(get_logger("runtime"), tenant_id="t1", trace_id="abc")
        log.info("agent run", extra={"stage": "llm"})
    """
    base: dict[str, Any] = {}
    if trace_id:
        base["trace_id"] = trace_id
    if tenant_id:
        base["tenant_id"] = tenant_id
    if session_id:
        base["session_id"] = session_id
    base.update(fields)
    return _BoundAdapter(logger, base)


class _BoundAdapter(logging.LoggerAdapter):
    """把绑定字段与调用方 extra 合并为 extra_fields 的适配器。"""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = dict(self.extra)
        user_extra = kwargs.pop("extra", None) or {}
        if isinstance(user_extra, dict):
            extra.update(user_extra)
        kwargs["extra"] = {"extra_fields": extra}
        return msg, kwargs


def setup_logging_from_settings(settings: Any) -> None:
    """按 PlatformSettings 初始化日志（config.load_settings 结果）。"""
    log_file = None
    data_dir = getattr(settings, "data_dir", "data")
    if data_dir:
        log_file = str(Path(data_dir) / "logs" / "teneuris.log")
    setup_logging(
        level=getattr(settings, "log_level", "INFO"),
        log_file=log_file,
        redactor=Redactor(getattr(getattr(settings, "pii", None), "patterns", None),
                          getattr(getattr(settings, "pii", None), "enabled", True)),
    )
