from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
import yaml


def _module() -> ModuleType:
    path = Path("runs/multitenant/project_kubernetes_secrets.py").resolve()
    spec = importlib.util.spec_from_file_location("project_kubernetes_secrets", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _manifest(path: Path) -> None:
    names = (
        "trpc-service-secrets",
        "trpc-worker-secrets",
        "trpc-migration-secrets",
        "trpc-metrics-secrets",
    )
    documents = []
    for name in names:
        data = {}
        if name == "trpc-service-secrets":
            data = {
                "TRPC_SERVICE_S3_ACCESS_KEY": "dXNlcg==",
                "TRPC_SERVICE_S3_SECRET_KEY": "cGFzc3dvcmQ=",
            }
        documents.append(
            {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name}, "data": data}
        )
    documents.append(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "runtime-support-secrets"},
            "data": {
                "postgres-admin-password": "YQ==",
                "runtime-password": "YQ==",
                "worker-password": "YQ==",
                "migration-password": "YQ==",
                "metrics-password": "YQ==",
            },
        }
    )
    documents.append(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "xuanyuan-pull"},
            "type": "kubernetes.io/dockerconfigjson",
            "data": {".dockerconfigjson": "eyJhdXRocyI6e319"},
        }
    )
    path.write_text(yaml.safe_dump_all(documents), encoding="utf-8")


def test_projection_is_exact_and_rewrites_namespace(tmp_path: Path) -> None:
    module = _module()
    manifest = tmp_path / "secrets.yaml"
    _manifest(manifest)

    runtime = module.project_secrets(manifest, "trpc-service", "runtime")
    support = module.project_secrets(manifest, "trpc-runtime-support", "support")

    assert len(runtime) == 5
    assert {item["metadata"]["namespace"] for item in runtime} == {"trpc-service"}
    assert [item["metadata"]["name"] for item in support] == [
        "runtime-support-secrets",
        "trpc-runtime-minio",
        "xuanyuan-pull",
    ]
    minio = support[1]
    assert minio["data"] == {
        "MINIO_ROOT_USER": "dXNlcg==",
        "MINIO_ROOT_PASSWORD": "cGFzc3dvcmQ=",
    }
    assert runtime[-1]["metadata"]["name"] == "xuanyuan-pull"
    assert support[-1]["metadata"]["namespace"] == "trpc-runtime-support"


def test_projection_rejects_empty_image_pull_secret(tmp_path: Path) -> None:
    module = _module()
    manifest = tmp_path / "secrets.yaml"
    _manifest(manifest)
    documents = list(yaml.safe_load_all(manifest.read_text(encoding="utf-8")))
    documents[-1]["data"][".dockerconfigjson"] = ""
    manifest.write_text(yaml.safe_dump_all(documents), encoding="utf-8")

    with pytest.raises(ValueError, match="image pull Secret"):
        module.project_secrets(manifest, "trpc-service", "runtime")


def test_runtime_projection_overlays_complete_fixture_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module()
    manifest = tmp_path / "secrets.yaml"
    _manifest(manifest)
    for name in module._FIXTURE_NAMES:
        monkeypatch.setenv(name, f"synthetic-{name.lower()}")

    runtime = module.project_secrets(manifest, "trpc-service", "runtime")
    service = next(item for item in runtime if item["metadata"]["name"] == "trpc-service-secrets")

    assert set(service["stringData"]) >= set(module._FIXTURE_NAMES)
    assert all(
        service["stringData"][name].startswith("synthetic-") for name in module._FIXTURE_NAMES
    )


def test_support_projection_rejects_missing_required_key(tmp_path: Path) -> None:
    module = _module()
    manifest = tmp_path / "secrets.yaml"
    _manifest(manifest)
    documents = list(yaml.safe_load_all(manifest.read_text(encoding="utf-8")))
    support = next(
        item for item in documents if item["metadata"]["name"] == "runtime-support-secrets"
    )
    support["data"].pop("metrics-password")
    manifest.write_text(yaml.safe_dump_all(documents), encoding="utf-8")

    with pytest.raises(ValueError, match="keys are incomplete"):
        module.project_secrets(manifest, "trpc-runtime-support", "support")
