from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pytest

from tenant_agent.models import RedactionPolicy, SecretRef
from tenant_agent.security import (
    CompositeSecretResolver,
    RedactingFormatter,
    Redactor,
    SecretRegistry,
    SecretResolutionError,
    configure_safe_logging,
)


@pytest.mark.asyncio
async def test_environment_and_file_secrets_are_resolved_and_registered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TEST_SECRET", "super-secret-value")
    (tmp_path / "mounted").write_text("file-secret", encoding="utf-8")
    registry = SecretRegistry()
    resolver = CompositeSecretResolver(file_root=tmp_path, registry=registry)

    assert await resolver.resolve(SecretRef(uri="env://TEST_SECRET")) == "super-secret-value"
    assert await resolver.resolve(SecretRef(uri="file://mounted")) == "file-secret"
    assert "super-secret-value" not in registry.redact_exact_values("x super-secret-value y")


@pytest.mark.asyncio
async def test_secret_file_cannot_escape_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = CompositeSecretResolver(file_root=tmp_path)
    with pytest.raises(SecretResolutionError):
        await resolver.resolve(SecretRef(uri="file://../outside"))
    (tmp_path / "beta").mkdir()
    (tmp_path / "beta" / "secret").write_text("cross-tenant", encoding="utf-8")
    with pytest.raises(SecretResolutionError, match="canonical relative path"):
        await resolver.resolve(SecretRef(uri="file://alpha/../beta/secret"))
    with pytest.raises(SecretResolutionError, match="canonical relative path"):
        await resolver.resolve(SecretRef(uri="file://alpha/%2e%2e/beta/secret"))

    (tmp_path / "alpha").mkdir()
    link = tmp_path / "alpha" / "cross-tenant-link"
    try:
        link.symlink_to(tmp_path / "beta", target_is_directory=True)
    except OSError:
        original_resolve = Path.resolve

        def simulated_link_resolve(path: Path, *args: object, **kwargs: object) -> Path:
            if path == link / "secret":
                return (tmp_path / "beta" / "secret").resolve()
            return original_resolve(path, *args, **kwargs)

        monkeypatch.setattr(Path, "resolve", simulated_link_resolve)
    with pytest.raises(SecretResolutionError, match="tenant directory"):
        await resolver.resolve(SecretRef(uri="file://alpha/cross-tenant-link/secret"))


@pytest.mark.asyncio
async def test_secret_file_rejects_windows_junction_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_root = tmp_path / "alpha"
    tenant_root.mkdir()
    (tenant_root / "secret").write_text("secret", encoding="utf-8")
    resolver = CompositeSecretResolver(file_root=tmp_path)
    original_is_junction = getattr(Path, "is_junction", None)

    def simulated_junction(path: Path) -> bool:
        if path == tenant_root:
            return True
        return bool(original_is_junction(path)) if original_is_junction else False

    monkeypatch.setattr(Path, "is_junction", simulated_junction, raising=False)
    with pytest.raises(SecretResolutionError, match="reparse point"):
        await resolver.resolve(SecretRef(uri="file://alpha/secret"))


@pytest.mark.asyncio
async def test_secret_file_allows_scoped_kubernetes_atomic_writer_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamp_root = tmp_path / "..2026_08_30_00_00_00" / "alpha"
    timestamp_root.mkdir(parents=True)
    (timestamp_root / "secret").write_text("projected-secret", encoding="utf-8")
    lexical_tenant_root = tmp_path / "alpha"
    lexical_secret = lexical_tenant_root / "secret"
    data_link = tmp_path / "..data"
    resolver = CompositeSecretResolver(file_root=tmp_path)
    original_resolve = Path.resolve

    def atomic_writer_resolve(path: Path, *args: object, **kwargs: object) -> Path:
        if path == lexical_tenant_root:
            return timestamp_root
        if path == lexical_secret:
            return timestamp_root / "secret"
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", atomic_writer_resolve)
    original_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path in {lexical_tenant_root, data_link} or original_is_symlink(path),
    )
    original_readlink = os.readlink
    monkeypatch.setattr(
        os,
        "readlink",
        lambda path: (
            "..data/alpha"
            if Path(path) == lexical_tenant_root
            else "..2026_08_30_00_00_00"
            if Path(path) == data_link
            else original_readlink(path)
        ),
    )
    monkeypatch.setattr(
        CompositeSecretResolver,
        "_is_reparse_point",
        staticmethod(lambda path: path == lexical_tenant_root),
    )
    assert await resolver.resolve(SecretRef(uri="file://alpha/secret")) == "projected-secret"


def test_redactor_scrubs_credentials_and_pii_recursively() -> None:
    redactor = Redactor(RedactionPolicy())
    value = redactor.value(
        {
            "authorization": "Bearer abcdefghijklmnop",
            "body": "email alice@example.com phone +1 (415) 555-1212 api_key=abcdef123456",
        }
    )
    assert value["authorization"] == "[REDACTED]"
    assert "alice@example.com" not in value["body"]
    assert "555-1212" not in value["body"]
    assert "abcdef123456" not in value["body"]

    timestamped = redactor.text("2026-08-27 15:36:53,049 phone +1 (415) 555-1212")
    assert timestamped.startswith("2026-08-27 15:36:53,049")
    assert "555-1212" not in timestamped
    trace = "8f123456789ab0123456789abcdef01234"
    rendered = redactor.text(f"trace_id={trace} 电话13800138000")
    assert trace in rendered
    assert "13800138000" not in rendered


def test_redacting_formatter_scrubs_rendered_exception_traceback() -> None:
    registry = SecretRegistry()
    registry.register("traceback-secret-value")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(RedactingFormatter(Redactor(registry=registry), "%(message)s"))
    logger = logging.getLogger("redacting-formatter-test")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.ERROR)
    try:
        raise ValueError("traceback-secret-value")
    except ValueError:
        logger.exception("failure")
    rendered = stream.getvalue()
    assert "traceback-secret-value" not in rendered
    assert "[REDACTED]" in rendered


def test_safe_logging_hardens_existing_non_propagating_handlers() -> None:
    registry = SecretRegistry()
    registry.register("non-root-secret-value")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    isolated = logging.getLogger("isolated-sdk-logger")
    isolated.handlers = [handler]
    isolated.propagate = False
    isolated.setLevel(logging.ERROR)

    configure_safe_logging("INFO", Redactor(registry=registry))
    isolated.error("raw non-root-secret-value")

    assert "non-root-secret-value" not in stream.getvalue()
    assert "[REDACTED]" in stream.getvalue()
