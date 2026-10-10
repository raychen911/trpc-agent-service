from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_root_readme_describes_the_rendered_full_stack() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    for marker in (
        "infrastructure-statefulsets.yaml",
        "prometheus-adapter.yaml",
        "ingress.yaml",
        "trpc-infrastructure-secrets",
        "statefulset/postgres",
        "job/minio-bootstrap",
    ):
        assert marker in readme
    assert "migration Job 作为 `PreSync`" not in readme
    assert "生产应在私有部署仓库添加 Ingress" not in readme


def test_readme_does_not_overstate_storage_or_im_capabilities() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    for marker in (
        "RegisteredTenantServiceBundle",
        "未注册的组合不会静默退回",
        "Redis | 仅 SessionReady 通知和可重建投影",
        "FAILED/unsupported_capability",
        "当前两通道都能接收入站媒体",
        "代码不做隐式",
    ):
        assert marker in readme


def test_overlay_readme_matches_bundled_external_metrics_provider() -> None:
    readme = (ROOT / "deploy" / "kustomize" / "overlays" / "production" / "README.md").read_text(
        encoding="utf-8"
    )

    assert "bundled Prometheus\nAdapter exposes" in readme
    assert "old migration `PreSync` phase" in readme
    assert "managed-services-patch.example.yaml" in readme
