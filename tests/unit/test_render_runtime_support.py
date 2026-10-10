"""Focused tests for deterministic runtime-support manifest rendering."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts import render_runtime_support as renderer

POSTGRES_IMAGE = "postgres:16@sha256:" + "a" * 64
REDIS_IMAGE = "redis:7@sha256:" + "b" * 64


def _support() -> SimpleNamespace:
    return SimpleNamespace(
        data_node="ack-data-0",
        postgres_image=POSTGRES_IMAGE,
        redis_image=REDIS_IMAGE,
        postgres_host_path="/srv/trpc/postgres",
        redis_host_path="/srv/trpc/redis",
        minio_host_path="/srv/trpc/minio",
    )


def _write_templates(tmp_path: Path) -> tuple[Path, Path, str, str]:
    support_template = tmp_path / "ack-runtime-support.yaml"
    minio_template = tmp_path / "ack-runtime-minio.yaml"
    support_text = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: old-node
        zone: data
      containers:
        - name: postgres
          image: postgres:15
        - name: metrics-sidecar
          image: metrics:1
      initContainers:
        - name: postgres-bootstrap
          image: postgres:15
      volumes:
        - name: postgres-data
          emptyDir: {}
        - name: postgres-config
          configMap:
            name: postgres-config
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
spec:
  template:
    spec:
      containers:
        - name: redis
          image: redis:7
      volumes:
        - name: redis-data
          persistentVolumeClaim:
            claimName: redis
"""
    minio_text = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  template:
    spec:
      containers:
        - name: minio
          image: minio:latest
      volumes:
        - name: minio-storage
          emptyDir: {}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: untouched
data:
  node: old-node
"""
    support_template.write_text(support_text, encoding="utf-8")
    minio_template.write_text(minio_text, encoding="utf-8")
    return support_template, minio_template, support_text, minio_text


def test_render_updates_nodes_images_and_data_volumes_without_mutating_templates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    support_template, minio_template, support_text, minio_text = _write_templates(tmp_path)
    monkeypatch.setattr(
        renderer,
        "_load_runtime_gate_config",
        lambda _path: SimpleNamespace(support=_support()),
    )

    output_dir = tmp_path / "rendered"
    output_paths = renderer.render_runtime_support(
        tmp_path / "runtime-gate.yaml",
        support_template=support_template,
        minio_template=minio_template,
        output_dir=output_dir,
    )

    assert output_paths == (
        output_dir / "ack-runtime-support.yaml",
        output_dir / "ack-runtime-minio.yaml",
    )
    assert support_template.read_text(encoding="utf-8") == support_text
    assert minio_template.read_text(encoding="utf-8") == minio_text

    rendered_support = list(yaml.safe_load_all(output_paths[0].read_text(encoding="utf-8")))
    rendered_minio = list(yaml.safe_load_all(output_paths[1].read_text(encoding="utf-8")))
    postgres = rendered_support[0]["spec"]["template"]["spec"]
    redis = rendered_support[1]["spec"]["template"]["spec"]
    minio = rendered_minio[0]["spec"]["template"]["spec"]

    assert postgres["nodeSelector"] == {
        "kubernetes.io/hostname": "ack-data-0",
        "zone": "data",
    }
    assert postgres["containers"][0]["image"] == POSTGRES_IMAGE
    assert postgres["containers"][1]["image"] == "metrics:1"
    assert postgres["initContainers"][0]["image"] == POSTGRES_IMAGE
    assert postgres["volumes"] == [
        {
            "name": "postgres-data",
            "hostPath": {
                "path": "/srv/trpc/postgres",
                "type": "DirectoryOrCreate",
            },
        },
        {"name": "postgres-config", "configMap": {"name": "postgres-config"}},
    ]
    assert redis["nodeSelector"] == {"kubernetes.io/hostname": "ack-data-0"}
    assert redis["containers"][0]["image"] == REDIS_IMAGE
    assert redis["volumes"][0] == {
        "name": "redis-data",
        "hostPath": {"path": "/srv/trpc/redis", "type": "DirectoryOrCreate"},
    }
    assert minio["nodeSelector"] == {"kubernetes.io/hostname": "ack-data-0"}
    assert minio["volumes"][0] == {
        "name": "minio-storage",
        "hostPath": {"path": "/srv/trpc/minio", "type": "DirectoryOrCreate"},
    }
    assert rendered_minio[1]["data"]["node"] == "old-node"


def test_redis_without_a_volume_gets_a_data_host_path() -> None:
    documents = [
        {
            "kind": "Deployment",
            "metadata": {"name": "redis"},
            "spec": {"template": {"spec": {"containers": [{"name": "redis", "image": "redis:7"}]}}},
        }
    ]

    rendered = renderer.render_documents(documents, _support())
    pod_spec = rendered[0]["spec"]["template"]["spec"]
    assert pod_spec["volumes"] == [
        {
            "name": "data",
            "hostPath": {"path": "/srv/trpc/redis", "type": "DirectoryOrCreate"},
        }
    ]
    assert pod_spec["containers"][0]["volumeMounts"] == [{"name": "data", "mountPath": "/data"}]


def test_render_binds_support_provider_images_pull_secret_and_prometheus_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    support_template = tmp_path / "support.yaml"
    minio_template = tmp_path / "minio.yaml"
    support_template.write_text(
        """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backlog-metric-source
spec:
  template:
    spec:
      imagePullSecrets:
        - name: ghcr-pull
      containers:
        - name: source
          image: quay.io/example/source:latest
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
spec:
  template:
    spec:
      imagePullSecrets:
        - name: ghcr-pull
      containers:
        - name: prometheus
          image: quay.io/example/prometheus:latest
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus-adapter
spec:
  template:
    spec:
      imagePullSecrets:
        - name: ghcr-pull
      containers:
        - name: adapter
          image: quay.io/example/adapter:latest
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  prometheus.yml: |
    global:
      scrape_interval: 5s
    scrape_configs:
      - job_name: trpc-runtime-backlog
        static_configs:
          - targets: [backlog-metric-source:9100]
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-adapter-config
data:
  config.yaml: |
    externalRules:
      - seriesQuery: 'trpc_session_ready_backlog{namespace!=""}'
        resources:
          overrides:
            namespace:
              resource: namespace
        name:
          matches: '^trpc_session_ready_backlog$'
          as: trpc_session_ready_backlog
        metricsQuery: 'max(<<.Series>>{<<.LabelMatchers>>}) by (namespace)'
""",
        encoding="utf-8",
    )
    minio_template.write_text(
        """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  template:
    spec:
      imagePullSecrets:
        - name: ghcr-pull
      initContainers:
        - name: prepare-data
          image: quay.io/example/minio:latest
      containers:
        - name: minio
          image: quay.io/example/minio:latest
---
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-bucket-bootstrap
spec:
  template:
    spec:
      imagePullSecrets:
        - name: ghcr-pull
      containers:
        - name: bootstrap
          image: quay.io/example/mc:latest
""",
        encoding="utf-8",
    )
    support = _support()
    support.minio_image = "registry.example/minio@sha256:" + "c" * 64
    support.minio_client_image = "registry.example/mc@sha256:" + "d" * 64
    support.prometheus_image = "registry.example/prometheus@sha256:" + "e" * 64
    support.prometheus_adapter_image = "registry.example/adapter@sha256:" + "f" * 64
    config = SimpleNamespace(
        support=support,
        image_pull_secret="runtime-pull",
        performance=SimpleNamespace(namespace="trpc-service"),
        resolved_image_references=lambda: {
            "initial": "registry.example/trpc-agent-service@sha256:" + "a" * 64
        },
    )
    monkeypatch.setattr(renderer, "_load_runtime_gate_config", lambda _path: config)

    support_output, minio_output = renderer.render_runtime_support(
        tmp_path / "runtime-gate.yaml",
        support_template=support_template,
        minio_template=minio_template,
        output_dir=tmp_path / "rendered",
    )
    rendered_support = list(yaml.safe_load_all(support_output.read_text(encoding="utf-8")))
    rendered_minio = list(yaml.safe_load_all(minio_output.read_text(encoding="utf-8")))

    deployment_images = {
        document["metadata"]["name"]: renderer._pod_specs(document)[0]["containers"][0]["image"]
        for document in rendered_support + rendered_minio
        if isinstance(document, dict) and document.get("kind") == "Deployment"
    }
    assert deployment_images == {
        "backlog-metric-source": "registry.example/trpc-agent-service@sha256:" + "a" * 64,
        "prometheus": "registry.example/prometheus@sha256:" + "e" * 64,
        "prometheus-adapter": "registry.example/adapter@sha256:" + "f" * 64,
        "minio": "registry.example/minio@sha256:" + "c" * 64,
    }
    minio_pod = next(
        renderer._pod_specs(document)[0]
        for document in rendered_minio
        if isinstance(document, dict)
        and document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "minio"
    )
    assert {container["image"] for container in minio_pod["initContainers"]} == {
        "registry.example/minio@sha256:" + "c" * 64
    }
    bootstrap = next(
        renderer._pod_specs(document)[0]
        for document in rendered_minio
        if isinstance(document, dict)
        and document.get("kind") == "Job"
        and document.get("metadata", {}).get("name") == "minio-bucket-bootstrap"
    )
    assert bootstrap["containers"][0]["image"] == "registry.example/mc@sha256:" + "d" * 64
    for document in rendered_support + rendered_minio:
        for pod_spec in renderer._pod_specs(document):
            assert pod_spec["imagePullSecrets"] == [{"name": "runtime-pull"}]

    config_map = next(
        document
        for document in rendered_support
        if isinstance(document, dict)
        and document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "prometheus-config"
    )
    prometheus = yaml.safe_load(config_map["data"]["prometheus.yml"])
    production_jobs = [
        job
        for job in prometheus["scrape_configs"]
        if job.get("job_name") == "trpc-session-ready-backlog-production"
    ]
    assert production_jobs == [
        {
            "job_name": "trpc-session-ready-backlog-production",
            "static_configs": [{"targets": ["trpc-backlog-exporter.trpc-service.svc:9100"]}],
        }
    ]
    synthetic = next(
        job for job in prometheus["scrape_configs"] if job["job_name"] == "trpc-runtime-backlog"
    )
    assert synthetic["metric_relabel_configs"] == [
        {"source_labels": ["namespace"], "regex": "^trpc\\-service$", "action": "drop"}
    ]
    adapter_config = next(
        document
        for document in rendered_support
        if isinstance(document, dict)
        and document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "prometheus-adapter-config"
    )
    adapter = yaml.safe_load(adapter_config["data"]["config.yaml"])
    assert (
        adapter["externalRules"][0]["metricsQuery"]
        == "max(<<.Series>>{<<.LabelMatchers>>}) by (namespace)"
    )
    expected_prometheus_hash = hashlib.sha256(
        json.dumps(
            config_map["data"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    expected_adapter_hash = hashlib.sha256(
        json.dumps(
            adapter_config["data"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    prometheus_deployment = next(
        document
        for document in rendered_support
        if isinstance(document, dict)
        and document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "prometheus"
    )
    adapter_deployment = next(
        document
        for document in rendered_support
        if isinstance(document, dict)
        and document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "prometheus-adapter"
    )
    assert prometheus_deployment["spec"]["template"]["metadata"]["annotations"] == {
        "trpc.io/prometheus-config-sha256": expected_prometheus_hash
    }
    assert adapter_deployment["spec"]["template"]["metadata"]["annotations"] == {
        "trpc.io/prometheus-adapter-config-sha256": expected_adapter_hash
    }
    assert "ghcr.io" not in support_output.read_text(encoding="utf-8")
    assert "quay.io" not in minio_output.read_text(encoding="utf-8")


def test_missing_support_is_rejected() -> None:
    with pytest.raises(
        renderer.RuntimeSupportRenderError,
        match=r"missing kubernetes\.support",
    ):
        renderer.render_documents([{"kind": "ConfigMap"}], None)


def test_safe_loader_preserves_documents_and_rejects_python_tags(tmp_path: Path) -> None:
    template = tmp_path / "template.yaml"
    template.write_text("---\na: 1\n---\nb: 2\n", encoding="utf-8")
    assert len(renderer._read_documents(template)) == 2

    template.write_text("!!python/object/apply:os.system ['echo unsafe']\n", encoding="utf-8")
    with pytest.raises(renderer.RuntimeSupportRenderError, match="invalid Kubernetes YAML"):
        renderer._read_documents(template)


def test_cli_reports_rendered_paths_without_applying_manifests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    support_template, minio_template, _, _ = _write_templates(tmp_path)
    monkeypatch.setattr(
        renderer,
        "_load_runtime_gate_config",
        lambda _path: SimpleNamespace(support=_support()),
    )
    output_dir = tmp_path / "cli-output"

    assert (
        renderer.main(
            [
                "--config",
                str(tmp_path / "runtime-gate.yaml"),
                "--support-template",
                str(support_template),
                "--minio-template",
                str(minio_template),
                "--output-dir",
                str(output_dir),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"support", "minio"}
    assert Path(payload["support"]).exists()
    assert Path(payload["minio"]).exists()


def test_output_path_guard_prevents_template_overwrite(tmp_path: Path) -> None:
    template = tmp_path / "template.yaml"
    with pytest.raises(renderer.RuntimeSupportRenderError, match="refusing to overwrite"):
        renderer._output_paths(template, tmp_path / "other.yaml", tmp_path)


def test_render_performance_overlay_binds_config_image_and_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workload = SimpleNamespace(
        node_label="trpc-role=workload",
        gateway=SimpleNamespace(replicas=4, database_pool_min_size=5, database_pool_max_size=6),
        worker=SimpleNamespace(
            database_pool_min_size=2,
            database_pool_max_size=8,
            offline_agent_delay_seconds=3.0,
        ),
        outbox=SimpleNamespace(replicas=1, database_pool_min_size=2, database_pool_max_size=4),
        recovery=SimpleNamespace(
            replicas=1, database_pool_min_size=1, database_pool_max_size=2, poll_seconds=1.0
        ),
    )
    performance = SimpleNamespace(
        enabled=True,
        namespace="perf-acceptance",
        worker_concurrency=50,
        max_inflight=64,
        db_pool_size=32,
        workers=4,
        workload=workload,
        environment=lambda: {
            "TRPC_PERF_K8S_ENABLED": "true",
            "TRPC_PERF_K8S_NAMESPACE": "perf-acceptance",
            "TRPC_PERF_K8S_MAX_INFLIGHT": "64",
            "TRPC_PERF_K8S_DB_POOL_SIZE": "32",
            "TRPC_PERF_K8S_WORKERS": "4",
            "TRPC_PERF_K8S_WORKER_CONCURRENCY": "50",
        },
    )
    config = SimpleNamespace(
        performance=performance,
        image_pull_secret="runtime-pull",
        object_store_endpoint="http://minio.trpc-runtime-support.svc.cluster.local:9000",
        object_store_bucket="trpc-artifacts",
        resolved_image_references=lambda: {
            "initial": "mirror.example.com/owner/trpc-agent-service@sha256:" + "a" * 64,
        },
    )
    monkeypatch.setattr(renderer, "_load_runtime_gate_config", lambda _path: config)

    output = renderer.render_performance_overlay(
        tmp_path / "runtime-gate.yaml",
        output_dir=tmp_path / "rendered-performance",
    )

    assert output.name == "rendered-performance"
    overlay = yaml.safe_load((output / "kustomization.yaml").read_text(encoding="utf-8"))
    assert overlay["namespace"] == "perf-acceptance"
    assert "im-external-egress" in overlay["resources"]
    assert (output / "im-external-egress" / "kustomization.yaml").is_file()
    assert (output / "im-external-egress" / "network-policy.yaml").is_file()
    assert overlay["images"][0]["newName"].startswith("mirror.example.com/")
    assert overlay["images"][0]["digest"] == "sha256:" + "a" * 64
    pull_patch = yaml.safe_load(
        (output / "performance-image-pull-secret-patch.yaml").read_text(encoding="utf-8")
    )
    assert pull_patch == [
        {
            "op": "add",
            "path": "/spec/template/spec/imagePullSecrets",
            "value": [{"name": "runtime-pull"}],
        }
    ]
    config_patch = yaml.safe_load(
        (output / "performance-config-patch.yaml").read_text(encoding="utf-8")
    )
    assert config_patch["data"]["TRPC_PERF_K8S_WORKER_CONCURRENCY"] == "50"
    assert config_patch["data"]["TRPC_SERVICE_WORKER_CONCURRENCY"] == "50"
    assert config_patch["data"]["TRPC_SERVICE_ENVIRONMENT"] == "test"
    assert config_patch["data"]["TRPC_SERVICE_CAPTURE_CONTENT"] == "false"
    assert config_patch["data"]["TRPC_SERVICE_OTLP_ENDPOINT"] == ""
    assert config_patch["data"]["TRPC_SERVICE_PROMETHEUS_ENABLED"] == "false"
    assert config_patch["data"]["TRPC_SERVICE_S3_ENDPOINT"] == (
        "http://minio.trpc-runtime-support.svc.cluster.local:9000"
    )
    assert config_patch["data"]["TRPC_SERVICE_S3_BUCKET"] == "trpc-artifacts"
    workload_documents = list(
        yaml.safe_load_all((output / "performance-workload-patch.yaml").read_text(encoding="utf-8"))
    )
    workload_by_name = {
        item["metadata"]["name"]: item for item in workload_documents if isinstance(item, dict)
    }
    assert workload_by_name["trpc-gateway"]["spec"]["template"]["spec"]["nodeSelector"] == {
        "trpc-role": "workload"
    }
    assert workload_by_name["trpc-gateway"]["spec"]["template"]["spec"]["containers"][0]["env"] == [
        {"name": "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE", "value": "5"},
        {"name": "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE", "value": "6"},
    ]
    assert workload_by_name["trpc-worker"]["spec"]["template"]["spec"]["containers"][0]["env"][
        -1
    ] == {
        "name": "TRPC_SERVICE_OFFLINE_AGENT_DELAY_SECONDS",
        "value": "3.0",
    }
    replicas = list(
        yaml.safe_load_all((output / "performance-replicas-patch.yaml").read_text(encoding="utf-8"))
    )
    replicas_by_name = {
        item["metadata"]["name"]: item["spec"]["replicas"]
        for item in replicas
        if isinstance(item, dict) and item.get("kind") == "Deployment"
    }
    assert replicas_by_name == {
        "trpc-gateway": 4,
        "trpc-worker": 4,
        "trpc-outbox-dispatcher": 1,
        "trpc-session-recovery": 1,
        "trpc-admin": 0,
        "trpc-artifact-gc": 0,
        "trpc-channel-dispatcher": 0,
        "trpc-post-turn-projector": 0,
        "trpc-wecom-connector": 0,
    }
    assert (output / "performance-network-policy.yaml").is_file()
    assert (output / "base" / "kustomization.yaml").is_file()


def test_render_performance_overlay_requires_explicit_enablement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        renderer,
        "_load_runtime_gate_config",
        lambda _path: SimpleNamespace(performance=SimpleNamespace(enabled=False)),
    )

    with pytest.raises(renderer.RuntimeSupportRenderError, match="enabled must be true"):
        renderer.render_performance_overlay(
            tmp_path / "runtime-gate.yaml",
            output_dir=tmp_path / "rendered-performance",
        )
