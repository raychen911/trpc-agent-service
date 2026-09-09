# ===================================================================
# log - 日志 + 脱敏（平台层新增）
# ===================================================================
# 说明: 结构化 JSON 日志，所有消息经 PII 脱敏，密钥不落盘（PRD 4.5）。
#   get_logger() 获取 logger；bind_logger() 绑定 tenant/trace/session 上下文；
#   setup_logging() 初始化（级别 / 文件 / JSON）。
# 规范: 日志内容禁止包含密钥明文；trace/审计需要的字段走 extra。
# ===================================================================

from .logger import (
    JsonFormatter,
    bind_logger,
    get_logger,
    setup_logging,
    setup_logging_from_settings,
)

__all__ = [
    "JsonFormatter",
    "bind_logger",
    "get_logger",
    "setup_logging",
    "setup_logging_from_settings",
]
