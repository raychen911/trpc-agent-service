# ===================================================================
# config.loader - 配置加载与校验
# ===================================================================
# 说明: 加载顺序（后者覆盖前者）:
#   1. 内置默认值（settings.py 各模型默认值）
#   2. .env 本地密钥文件（不进版本库）
#   3. yaml 配置文件（可选，env 变量可引用）
#   4. 环境变量覆盖（TENEURIS_ 前缀，如 TENEURIS_GATEWAY_PORT）
# 密钥字段: 支持在 yaml 中写 `{env: TENEURIS_XXX}` 引用环境变量，
#   运行时解密到内存，禁止落盘 / 入日志。
# 规范: 全局单例 + 显式 reload；校验失败立即抛错（fail fast）。
# ===================================================================

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Union, get_args, get_origin

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, SecretStr, ValidationError

from .settings import PlatformSettings

_ENV_PREFIX = "TENEURIS_"

# 导入本模块即加载 .env（配置路径的入口，CLI 启动时最先走到这里）。
# load_dotenv 默认**不覆盖**已存在的环境变量 —— 终端里 export 的值优先于
# .env，便于临时换 key 测试；.env 不存在时静默跳过，缺 key 由
# agent.model_factory 显式报错兜底（见 DEVELOPMENT_LOG「阶段二 Spec」用例 6）。
load_dotenv()


class ConfigError(RuntimeError):
    """配置加载 / 校验失败。"""


def _resolve_env_ref(value: Any) -> Any:
    """解析 `{env: NAME}` 引用: 从环境变量取值，未设置则报错。"""
    if isinstance(value, dict) and set(value.keys()) == {"env"}:
        name = value["env"]
        if not isinstance(name, str) or not name:
            raise ConfigError(f"非法 env 引用: {value!r}")
        resolved = os.environ.get(name)
        if resolved is None:
            raise ConfigError(f"环境变量未设置: {name}")
        return resolved
    if isinstance(value, dict):
        return {k: _resolve_env_ref(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_ref(v) for v in value]
    return value


def _coerce_env_value(raw: str, annotation: Any = None) -> Any:
    """环境变量字符串 -> 基础类型（int/bool/str）。

    annotation 传入目标字段类型时做类型感知: SecretStr 字段保持原始字符串
    （纯数字密钥若被 int() 转换会破坏 pydantic 校验与密钥语义）。
    """
    if annotation is not None:
        base = annotation
        if get_origin(base) is Union:
            args = [a for a in get_args(base) if a is not type(None)]
            if len(args) == 1:
                base = args[0]
        if isinstance(base, type) and issubclass(base, SecretStr):
            return raw
    lowered = raw.strip().lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    try:
        return int(raw)
    except ValueError:
        return raw


def _unwrap_model(annotation: Any) -> type[BaseModel] | None:
    """Optional[X] 等注解剥壳后若为 BaseModel 子类则返回该模型类。"""
    base = annotation
    if get_origin(base) is Union:
        args = [a for a in get_args(base) if a is not type(None)]
        if len(args) == 1:
            base = args[0]
    if isinstance(base, type) and issubclass(base, BaseModel):
        return base
    return None


def _is_dict_field(annotation: Any) -> bool:
    return get_origin(annotation) in (dict, )


def _assign_env_value(node: dict[str, Any], parts: list[str], model: type[BaseModel], raw: str) -> bool:
    """对照 pydantic schema 贪婪匹配，把环境变量值写入 data 节点。

    按「剩余 parts 从长到短拼接为字段名」尝试匹配（TENEURIS_ADMIN_API_KEY ->
    admin.api_key），命中后: 无剩余 parts 则赋值；子字段是嵌套模型则递归；
    是 dict 字段则剩余 parts 拼为 key。任何一步失败返回 False（调用方回退
    旧的朴素嵌套逻辑，保持向后兼容）。
    """
    if not parts:
        return False
    fields = model.model_fields
    for k in range(len(parts), 0, -1):
        name = "_".join(parts[:k])
        if name not in fields:
            continue
        field = fields[name]
        rest = parts[k:]
        if not rest:
            node[name] = _coerce_env_value(raw, field.annotation)
            return True
        child_model = _unwrap_model(field.annotation)
        if child_model is not None:
            sub = node.setdefault(name, {})
            if not isinstance(sub, dict):
                sub = {}
                node[name] = sub
            return _assign_env_value(sub, rest, child_model, raw)
        if _is_dict_field(field.annotation):
            key = "_".join(rest)
            existing = node.get(name)
            merged = dict(existing) if isinstance(existing, dict) else {}
            merged[key] = raw  # dict 字段值保持原始字符串（正则等不做 int/bool 转换）
            node[name] = merged
            return True
        return False  # 标量字段后仍有剩余 parts，无法表达
    return False


def _apply_env_overrides(data: dict[str, Any], prefix: str = _ENV_PREFIX) -> dict[str, Any]:
    """按 TENEURIS_ 前缀的环境变量覆盖配置。

    优先对照 PlatformSettings schema 贪婪匹配字段名（支持 api_key 等含下划线
    的字段，如 TENEURIS_ADMIN_API_KEY -> admin.api_key）；未匹配时回退朴素
    嵌套 TENEURIS_A_B_C -> data["a"]["b"]["c"]（兼容未知扩展键）。
    """
    for name, raw in os.environ.items():
        if not name.startswith(prefix):
            continue
        parts = [part.lower() for part in name[len(prefix):].split("_") if part]
        if not parts:
            continue
        if _assign_env_value(data, parts, PlatformSettings, raw):
            continue
        # 回退: 朴素嵌套（与历史行为一致）
        node = data
        for key in parts[:-1]:
            if not isinstance(node, dict):
                node = {}
            node = node.setdefault(key, {})
        if isinstance(node, dict):
            node[parts[-1]] = _coerce_env_value(raw)
    return data


def _load_yaml_file(path: str | Path | None) -> dict[str, Any]:
    """读取 yaml 配置文件，不存在返回空 dict。"""
    if not path:
        return {}
    file = Path(path)
    if not file.is_file():
        return {}
    try:
        content = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - 依赖解析错误信息
        raise ConfigError(f"yaml 解析失败 {file}: {exc}") from exc
    if not isinstance(content, dict):
        raise ConfigError(f"配置文件根节点必须是 dict: {file}")
    return content


def load_settings(path: str | Path | None = None) -> PlatformSettings:
    """加载并校验平台配置。

    Args:
        path: 可选 yaml 配置文件路径（如 config/teneuris.yaml）

    Returns:
        PlatformSettings: 校验通过的配置对象

    Raises:
        ConfigError: 配置文件缺失字段 / 类型错误 / env 引用未设置
    """
    data: dict[str, Any] = {}
    data = _apply_env_overrides(data)
    data = _resolve_env_ref(data)
    if path:
        file_data = _load_yaml_file(path)
        file_data = _resolve_env_ref(file_data)
        # 环境变量覆盖 yaml（与文档「加载顺序 4」契约一致）:
        # 以 yaml 为基底，env 作为 override 合并
        data = _merge_dicts(file_data, data)
    try:
        return PlatformSettings.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"配置校验失败: {exc}") from exc


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """深度合并: override 覆盖 base（仅 dict 递归合并）。"""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def reload_settings(path: str | Path | None = None) -> PlatformSettings:
    """重新加载配置（灰度 / 热更新场景使用）。"""
    return load_settings(path)


def save_example_config(path: str | Path) -> None:
    """生成一份带注释的示例配置，便于部署参考。"""
    example = {
        "env": "dev",
        "log_level": "INFO",
        "data_dir": "data",
        "gateway": {
            "host": "0.0.0.0",
            "port": 8000
        },
        "admin": {
            "host": "127.0.0.1",
            "port": 8002,
            "api_key": {
                "env": "TENEURIS_ADMIN_API_KEY"
            }
        },
        "storage": {
            "redis": {
                "dsn": "redis://127.0.0.1:6379/0"
            },
            "sql": {
                "dsn": "sqlite+aiosqlite:///data/teneuris.db"
            },
        },
        "telemetry": {
            "prometheus_enabled": True
        },
        "pii": {
            "enabled": True
        },
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        yaml.safe_dump(example, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
