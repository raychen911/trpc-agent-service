#!/usr/bin/env python3
"""Render the ACK runtime-support Kubernetes manifests.

The manifests in ``runs/multitenant`` are templates.  This renderer reads the
shared deployment configuration, applies node, image, and node-local storage
values in memory, then writes deterministic YAML to a separate directory.  It
does not invoke ``kubectl`` or modify a template in place.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import shutil
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SUPPORT_TEMPLATE = Path("runs") / "multitenant" / "ack-runtime-support.yaml"
DEFAULT_MINIO_TEMPLATE = Path("runs") / "multitenant" / "ack-runtime-minio.yaml"
DEFAULT_OUTPUT_DIR = Path("runs") / "multitenant" / "rendered"
DEFAULT_PERFORMANCE_TEMPLATE = Path("deploy") / "kustomize" / "overlays" / "performance"
DEFAULT_IM_EGRESS_TEMPLATE = Path("deploy") / "kustomize" / "im-external-egress"
DEFAULT_PERFORMANCE_OUTPUT_DIR = DEFAULT_OUTPUT_DIR / "performance"

_REQUIRED_SUPPORT_FIELDS = (
    "data_node",
    "postgres_image",
    "redis_image",
    "postgres_host_path",
    "redis_host_path",
    "minio_host_path",
)
_DATA_VOLUME_KEYS = {
    "csi",
    "emptyDir",
    "ephemeral",
    "hostPath",
    "nfs",
    "persistentVolumeClaim",
}
_NON_DATA_VOLUME_KEYS = {"configMap", "downwardAPI", "projected", "secret"}
_NAMESPACE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_SUPPORT_IMAGE_FIELDS = (
    "minio_image",
    "minio_client_image",
    "prometheus_image",
    "prometheus_adapter_image",
)
_SUPPORT_CONFIG_ROLLOUTS = {
    "prometheus": ("prometheus-config", "trpc.io/prometheus-config-sha256"),
    "prometheus-adapter": (
        "prometheus-adapter-config",
        "trpc.io/prometheus-adapter-config-sha256",
    ),
}


class RuntimeSupportRenderError(ValueError):
    """Raised when configuration or a manifest cannot be rendered safely."""


def _load_runtime_gate_config(config_path: Path) -> Any:
    """Load the shared schema lazily, keeping this module usable in isolation."""
    from scripts.deployment_config import load_runtime_gate_config

    return load_runtime_gate_config(config_path)


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _support_values(config: Any) -> dict[str, str]:
    support = _field(config, "support")
    if support is None:
        raise RuntimeSupportRenderError(
            "runtime config is missing kubernetes.support; all runtime-support fields are required"
        )

    values: dict[str, str] = {}
    invalid: list[str] = []
    for name in _REQUIRED_SUPPORT_FIELDS:
        value = _field(support, name)
        if not isinstance(value, str) or not value.strip():
            invalid.append(name)
        else:
            values[name] = value
    if invalid:
        raise RuntimeSupportRenderError(
            "runtime config kubernetes.support has missing or invalid fields: " + ", ".join(invalid)
        )
    return values


def _read_documents(path: Path) -> list[Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            documents = list(yaml.safe_load_all(handle))
    except OSError as exc:
        raise RuntimeSupportRenderError(f"cannot read Kubernetes template {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise RuntimeSupportRenderError(f"invalid Kubernetes YAML in {path}: {exc}") from exc
    if not documents:
        raise RuntimeSupportRenderError(f"Kubernetes template {path} is empty")
    return documents


def _write_documents(documents: Iterable[Any], path: Path) -> None:
    try:
        content = yaml.safe_dump_all(
            list(documents),
            allow_unicode=True,
            default_flow_style=False,
            explicit_start=True,
            sort_keys=True,
            width=120,
        )
        path.write_text(content, encoding="utf-8")
    except (OSError, yaml.YAMLError) as exc:
        raise RuntimeSupportRenderError(
            f"cannot write rendered Kubernetes YAML {path}: {exc}"
        ) from exc


def _kind(document: Mapping[str, Any]) -> str:
    value = document.get("kind")
    return value.strip().lower() if isinstance(value, str) else ""


def _name(document: Mapping[str, Any]) -> str:
    metadata = document.get("metadata")
    value = metadata.get("name") if isinstance(metadata, Mapping) else None
    return value.strip().lower() if isinstance(value, str) else ""


def _service_name_matches(name: str, service: str) -> bool:
    return name == service or name.startswith(service + "-") or name.startswith(service)


def _pod_specs(document: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Find workload ``template.spec`` maps, including nested List items."""
    result: list[dict[str, Any]] = []
    root_kind = _kind(document)

    def visit(value: Any, path: tuple[object, ...], inherited_kind: str) -> None:
        if isinstance(value, Mapping):
            current_kind = _kind(value) or inherited_kind
            if path[-2:] == ("template", "spec") and isinstance(value, dict):
                result.append(value)
            elif current_kind == "pod" and path[-1:] == ("spec",) and isinstance(value, dict):
                result.append(value)
            for key, child in value.items():
                visit(child, (*path, key), current_kind)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, (*path, index), inherited_kind)

    visit(document, (), root_kind)
    return result


def _set_node_selector(pod_spec: dict[str, Any], data_node: str) -> None:
    selector = pod_spec.get("nodeSelector")
    if not isinstance(selector, dict):
        selector = {}
        pod_spec["nodeSelector"] = selector

    hostname_keys = [
        key
        for key in selector
        if isinstance(key, str) and (key == "hostname" or key.endswith("/hostname"))
    ]
    if not hostname_keys:
        hostname_keys = ["kubernetes.io/hostname"]
    for key in hostname_keys:
        selector[key] = data_node


def _containers(pod_spec: Mapping[str, Any], field: str) -> list[dict[str, Any]]:
    raw = pod_spec.get(field)
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _all_containers(pod_spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for field in ("containers", "initContainers", "ephemeralContainers"):
        result.extend(_containers(pod_spec, field))
    return result


def _container_name(container: Mapping[str, Any]) -> str:
    name = container.get("name")
    return name.strip().lower().replace("_", "-") if isinstance(name, str) else ""


def _set_named_bootstrap_images(pod_spec: Mapping[str, Any], image: str) -> None:
    for container in _all_containers(pod_spec):
        if _container_name(container) == "postgres-bootstrap":
            container["image"] = image


def _set_postgres_image(pod_spec: Mapping[str, Any], image: str) -> None:
    containers = _containers(pod_spec, "containers")
    matches = [
        container
        for container in containers
        if _container_name(container) in {"postgres", "postgresql"}
    ]
    if not matches and len(containers) == 1:
        matches = containers
    for container in matches:
        container["image"] = image


def _set_redis_image(pod_spec: Mapping[str, Any], image: str) -> None:
    for container in _containers(pod_spec, "containers"):
        if _container_name(container) == "redis":
            container["image"] = image


def _is_data_volume(volume: Mapping[str, Any]) -> bool:
    source_keys = set(volume) - {"name"}
    if source_keys & _NON_DATA_VOLUME_KEYS:
        return False
    if source_keys & _DATA_VOLUME_KEYS:
        return True
    name = volume.get("name")
    if not isinstance(name, str):
        return False
    normalized = name.lower().replace("_", "-")
    return any(token in normalized for token in ("data", "storage", "postgres", "redis", "minio"))


def _host_path_volume(name: str, host_path: str) -> dict[str, Any]:
    return {
        "name": name,
        "hostPath": {"path": host_path, "type": "DirectoryOrCreate"},
    }


def _replace_data_volumes(pod_spec: dict[str, Any], host_path: str) -> bool:
    raw_volumes = pod_spec.get("volumes")
    volumes = raw_volumes if isinstance(raw_volumes, list) else []
    changed = False
    for volume in volumes:
        if not isinstance(volume, dict) or not _is_data_volume(volume):
            continue
        name = volume.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        volume.clear()
        volume.update(_host_path_volume(name, host_path))
        changed = True
    if changed:
        pod_spec["volumes"] = volumes
    return changed


def _ensure_data_volume(pod_spec: dict[str, Any], host_path: str) -> None:
    """Give the Redis deployment a real /data hostPath when its template lacks one."""
    raw_volumes = pod_spec.get("volumes")
    volumes = raw_volumes if isinstance(raw_volumes, list) else []
    data_volumes = [
        volume for volume in volumes if isinstance(volume, dict) and _is_data_volume(volume)
    ]
    if data_volumes:
        _replace_data_volumes(pod_spec, host_path)
    else:
        volumes.append(_host_path_volume("data", host_path))
        pod_spec["volumes"] = volumes

    mounts = pod_spec.get("containers")
    if not isinstance(mounts, list):
        return
    for container in mounts:
        if not isinstance(container, dict):
            continue
        raw_mounts = container.get("volumeMounts")
        volume_mounts = raw_mounts if isinstance(raw_mounts, list) else []
        if any(
            isinstance(mount, Mapping) and mount.get("name") == "data" for mount in volume_mounts
        ):
            continue
        volume_mounts.append({"name": "data", "mountPath": "/data"})
        container["volumeMounts"] = volume_mounts


def _render_document(document: Any, values: Mapping[str, str]) -> None:
    if isinstance(document, list):
        for item in document:
            _render_document(item, values)
        return
    if not isinstance(document, dict):
        return

    kind = _kind(document)
    name = _name(document)
    pod_specs = _pod_specs(document)
    for pod_spec in pod_specs:
        _set_node_selector(pod_spec, values["data_node"])
        _set_named_bootstrap_images(pod_spec, values["postgres_image"])

    if kind == "deployment":
        if _service_name_matches(name, "postgres"):
            for pod_spec in pod_specs:
                _set_postgres_image(pod_spec, values["postgres_image"])
                _replace_data_volumes(pod_spec, values["postgres_host_path"])
        elif _service_name_matches(name, "redis"):
            for pod_spec in pod_specs:
                _set_redis_image(pod_spec, values["redis_image"])
                _ensure_data_volume(pod_spec, values["redis_host_path"])
        elif _service_name_matches(name, "minio"):
            for pod_spec in pod_specs:
                _replace_data_volumes(pod_spec, values["minio_host_path"])
    elif name == "postgres-bootstrap":
        for pod_spec in pod_specs:
            for container in _all_containers(pod_spec):
                container["image"] = values["postgres_image"]

    items = document.get("items")
    if isinstance(items, list):
        for item in items:
            _render_document(item, values)


def render_documents(documents: Iterable[Any], support: Any) -> list[Any]:
    """Render loaded YAML documents without mutating the caller's objects."""
    values = _support_values(type("Config", (), {"support": support})())
    rendered = copy.deepcopy(list(documents))
    for document in rendered:
        _render_document(document, values)
    return rendered


def _support_runtime_bindings(config: Any) -> dict[str, str] | None:
    """Return optional support image/Secret bindings from the shared config.

    Older support-only test configurations predate the provider images.  The
    validated runtime configuration supplies all of these fields; returning
    ``None`` for an entirely absent block keeps the low-level renderer useful
    for those template tests without allowing a partially specified binding.
    """

    support = _field(config, "support")
    image_values = {
        name: _field(support, name) if support is not None else None
        for name in _SUPPORT_IMAGE_FIELDS
    }
    pull_secret = _field(config, "image_pull_secret")
    if all(value is None for value in (*image_values.values(), pull_secret)):
        return None
    invalid = [
        name
        for name, value in (*image_values.items(), ("image_pull_secret", pull_secret))
        if not isinstance(value, str) or not value.strip()
    ]
    if invalid:
        raise RuntimeSupportRenderError(
            "runtime config support image bindings are missing or invalid: " + ", ".join(invalid)
        )
    try:
        references = config.resolved_image_references()
        initial_image = references["initial"]
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeSupportRenderError(
            "runtime config does not provide an immutable initial runtime image"
        ) from exc
    if not isinstance(initial_image, str) or not initial_image.strip():
        raise RuntimeSupportRenderError(
            "runtime config does not provide an immutable initial runtime image"
        )
    return {
        **{name: value.strip() for name, value in image_values.items() if isinstance(value, str)},
        "backlog-metric-source_image": initial_image.strip(),
        "image_pull_secret": pull_secret.strip(),
    }


def _set_support_runtime_bindings(document: Any, bindings: Mapping[str, str]) -> None:
    """Bind support provider images and pull Secret without changing templates."""

    if not isinstance(document, Mapping):
        return
    kind = _kind(document)
    name = _name(document)
    if kind in {"deployment", "job"}:
        image = None
        if kind == "job" and name == "minio-bucket-bootstrap":
            image = bindings["minio_client_image"]
        elif _service_name_matches(name, "minio"):
            image = bindings["minio_image"]
        elif name == "prometheus":
            image = bindings["prometheus_image"]
        elif name == "prometheus-adapter":
            image = bindings["prometheus_adapter_image"]
        elif name == "backlog-metric-source":
            image = bindings["backlog-metric-source_image"]
        if image is not None:
            for pod_spec in _pod_specs(document):
                containers = (
                    _all_containers(pod_spec)
                    if name == "minio"
                    else _containers(pod_spec, "containers")
                )
                for container in containers:
                    container["image"] = image

    pull_secret = bindings.get("image_pull_secret")
    if pull_secret:
        for pod_spec in _pod_specs(document):
            pod_spec["imagePullSecrets"] = [{"name": pull_secret}]

    items = document.get("items")
    if isinstance(items, list):
        for item in items:
            _set_support_runtime_bindings(item, bindings)


def _config_data_sha256(data: Mapping[str, Any]) -> str:
    """Hash ConfigMap data canonically for a deterministic rollout trigger."""

    encoded = json.dumps(
        dict(data),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _set_support_config_rollout_annotations(documents: Iterable[Any]) -> None:
    """Restart Prometheus support workloads when their ConfigMap data changes."""

    resources: list[dict[str, Any]] = []

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            resources.append(value)
            items = value.get("items")
            if isinstance(items, list):
                for item in items:
                    collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    for document in documents:
        collect(document)

    config_hashes: dict[str, str] = {}
    for config_name, _annotation_key in _SUPPORT_CONFIG_ROLLOUTS.values():
        config = next(
            (
                resource
                for resource in resources
                if _kind(resource) == "configmap" and _name(resource) == config_name
            ),
            None,
        )
        data = config.get("data") if isinstance(config, dict) else None
        if isinstance(data, Mapping):
            config_hashes[config_name] = _config_data_sha256(data)

    for deployment_name, (config_name, annotation_key) in _SUPPORT_CONFIG_ROLLOUTS.items():
        digest = config_hashes.get(config_name)
        if digest is None:
            continue
        for deployment in resources:
            if _kind(deployment) != "deployment" or _name(deployment) != deployment_name:
                continue
            spec = deployment.get("spec")
            template = spec.get("template") if isinstance(spec, dict) else None
            if not isinstance(template, dict):
                continue
            metadata = template.setdefault("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
                template["metadata"] = metadata
            annotations = metadata.setdefault("annotations", {})
            if not isinstance(annotations, dict):
                annotations = {}
                metadata["annotations"] = annotations
            annotations[annotation_key] = digest


def _inject_prometheus_scrape_target(documents: list[Any], *, namespace: str) -> None:
    """Add the production exporter target while isolating synthetic series."""

    if not _NAMESPACE_RE.fullmatch(namespace):
        raise RuntimeSupportRenderError(
            "kubernetes.performance.namespace must be a valid Kubernetes namespace"
        )
    target = f"trpc-backlog-exporter.{namespace}.svc:9100"
    config_maps = [
        document
        for document in documents
        if isinstance(document, dict)
        and _kind(document) == "configmap"
        and _name(document) == "prometheus-config"
    ]
    if len(config_maps) != 1:
        raise RuntimeSupportRenderError(
            "runtime-support template must contain one prometheus-config ConfigMap"
        )
    data = config_maps[0].get("data")
    if not isinstance(data, dict):
        raise RuntimeSupportRenderError("prometheus-config has no data mapping")
    config_text = data.get("prometheus.yml")
    if not isinstance(config_text, str) or not config_text.strip():
        raise RuntimeSupportRenderError("prometheus-config is missing prometheus.yml")
    try:
        prometheus = yaml.safe_load(config_text)
    except yaml.YAMLError as exc:
        raise RuntimeSupportRenderError("prometheus.yml is not valid YAML") from exc
    if not isinstance(prometheus, dict):
        raise RuntimeSupportRenderError("prometheus.yml must be a mapping")
    raw_jobs = prometheus.get("scrape_configs")
    if not isinstance(raw_jobs, list):
        raise RuntimeSupportRenderError("prometheus.yml has no scrape_configs")

    jobs: list[dict[str, Any]] = []
    synthetic_jobs: list[dict[str, Any]] = []
    for raw_job in raw_jobs:
        if not isinstance(raw_job, dict):
            raise RuntimeSupportRenderError("prometheus.yml contains an invalid scrape job")
        job = copy.deepcopy(raw_job)
        static_configs = job.get("static_configs")
        if isinstance(static_configs, list):
            filtered_static: list[Any] = []
            for static in static_configs:
                if not isinstance(static, dict):
                    filtered_static.append(static)
                    continue
                targets = static.get("targets")
                if isinstance(targets, list):
                    static["targets"] = [value for value in targets if value != target]
                    if any(
                        isinstance(value, str) and "backlog-metric-source" in value
                        for value in targets
                    ):
                        synthetic_jobs.append(job)
                filtered_static.append(static)
            job["static_configs"] = filtered_static
        jobs.append(job)

    # Do not let the ephemeral source emit a second series for the production
    # namespace when the support template is reused in the same cluster.
    drop_rule = {
        "source_labels": ["namespace"],
        "regex": f"^{re.escape(namespace)}$",
        "action": "drop",
    }
    for job in synthetic_jobs:
        relabels = job.get("metric_relabel_configs")
        relabels = relabels if isinstance(relabels, list) else []
        relabels = [
            item
            for item in relabels
            if not (
                isinstance(item, Mapping)
                and item.get("source_labels") == ["namespace"]
                and item.get("action") == "drop"
                and item.get("regex") == drop_rule["regex"]
            )
        ]
        relabels.append(drop_rule)
        job["metric_relabel_configs"] = relabels

    jobs = [job for job in jobs if job.get("job_name") != "trpc-session-ready-backlog-production"]
    jobs.append(
        {
            "job_name": "trpc-session-ready-backlog-production",
            "static_configs": [{"targets": [target]}],
        }
    )
    prometheus["scrape_configs"] = jobs
    data["prometheus.yml"] = yaml.safe_dump(
        prometheus,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
        width=120,
    )


def _output_paths(
    support_template: Path,
    minio_template: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    support_output = output_dir / support_template.name
    minio_output = output_dir / minio_template.name
    template_paths = {support_template.resolve(), minio_template.resolve()}
    for output_path in (support_output, minio_output):
        if output_path.resolve() in template_paths:
            raise RuntimeSupportRenderError(
                f"refusing to overwrite Kubernetes template {output_path}; "
                "choose a separate --output-dir"
            )
    if support_output == minio_output:
        raise RuntimeSupportRenderError("support and MinIO output paths must be different")
    return support_output, minio_output


def render_runtime_support(
    config: str | Path,
    *,
    support_template: str | Path = DEFAULT_SUPPORT_TEMPLATE,
    minio_template: str | Path = DEFAULT_MINIO_TEMPLATE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> tuple[Path, Path]:
    """Render both templates and return ``(support_yaml, minio_yaml)`` paths."""
    config_path = Path(config)
    support_template_path = Path(support_template)
    minio_template_path = Path(minio_template)
    output_dir_path = Path(output_dir)

    loaded_config = _load_runtime_gate_config(config_path)
    values = _support_values(loaded_config)
    support_documents = render_documents(_read_documents(support_template_path), values)
    minio_documents = render_documents(_read_documents(minio_template_path), values)
    support_bindings = _support_runtime_bindings(loaded_config)
    if support_bindings is not None:
        for document in (*support_documents, *minio_documents):
            _set_support_runtime_bindings(document, support_bindings)
    performance = _field(loaded_config, "performance")
    performance_namespace = _field(performance, "namespace") if performance is not None else None
    if performance_namespace is not None:
        if not isinstance(performance_namespace, str) or not performance_namespace.strip():
            raise RuntimeSupportRenderError(
                "kubernetes.performance.namespace must be a non-empty string"
            )
        _inject_prometheus_scrape_target(
            support_documents,
            namespace=performance_namespace.strip(),
        )
    if support_bindings is not None:
        _set_support_config_rollout_annotations(support_documents)
    support_output, minio_output = _output_paths(
        support_template_path,
        minio_template_path,
        output_dir_path,
    )
    try:
        output_dir_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeSupportRenderError(
            f"cannot create rendered-manifest directory {output_dir_path}: {exc}"
        ) from exc
    _write_documents(support_documents, support_output)
    _write_documents(minio_documents, minio_output)
    return support_output, minio_output


def _performance_config(config: Any) -> Any:
    performance = _field(config, "performance")
    if performance is None:
        raise RuntimeSupportRenderError(
            "runtime config is missing kubernetes.performance; the performance overlay is opt-in"
        )
    if _field(performance, "enabled") is not True:
        raise RuntimeSupportRenderError(
            "kubernetes.performance.enabled must be true to render the performance overlay"
        )
    return performance


def _performance_image(config: Any) -> tuple[str, str]:
    try:
        references = config.resolved_image_references()
        reference = references["initial"]
        repository, digest = reference.rsplit("@", 1)
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeSupportRenderError(
            "runtime config does not provide an immutable initial performance image"
        ) from exc
    if not repository or not digest.startswith("sha256:"):
        raise RuntimeSupportRenderError(
            "runtime config does not provide an immutable initial performance image"
        )
    return repository, digest


def _performance_manifest_values(config: Any, performance: Any) -> dict[str, str]:
    try:
        values = dict(performance.environment())
    except AttributeError as exc:
        raise RuntimeSupportRenderError(
            "runtime config performance block cannot be projected"
        ) from exc
    object_store_endpoint = _field(config, "object_store_endpoint")
    object_store_bucket = _field(config, "object_store_bucket")
    if not isinstance(object_store_endpoint, str) or not object_store_endpoint:
        raise RuntimeSupportRenderError(
            "runtime config performance rendering requires kubernetes.object_store.endpoint"
        )
    if not isinstance(object_store_bucket, str) or not object_store_bucket:
        raise RuntimeSupportRenderError(
            "runtime config performance rendering requires kubernetes.object_store.bucket"
        )
    repository, digest = _performance_image(config)
    values.update(
        {
            "TRPC_SERVICE_ENVIRONMENT": "test",
            "TRPC_SERVICE_CAPTURE_CONTENT": "false",
            "TRPC_SERVICE_OTLP_ENDPOINT": "",
            "TRPC_SERVICE_PROMETHEUS_ENABLED": "false",
            "TRPC_SERVICE_S3_ENDPOINT": object_store_endpoint,
            "TRPC_SERVICE_S3_BUCKET": object_store_bucket,
            "TRPC_K8S_RUNTIME_IMAGE": f"{repository}@{digest}",
            "TRPC_PERF_K8S_IMAGE": f"{repository}@{digest}",
            "TRPC_PERF_K8S_IMAGE_DIGEST": digest,
            "TRPC_SERVICE_WORKER_CONCURRENCY": str(_field(performance, "worker_concurrency")),
        }
    )
    return values


def _performance_workload_patch(documents: list[Any], performance: Any) -> None:
    """Bind the configured workload placement and role env to Deployment patches."""

    workload = _field(performance, "workload")
    if workload is None:
        raise RuntimeSupportRenderError(
            "runtime config kubernetes.performance.workload is required"
        )
    node_label = _field(workload, "node_label")
    if not isinstance(node_label, str) or "=" not in node_label:
        raise RuntimeSupportRenderError(
            "runtime config kubernetes.performance.workload.node_label is invalid"
        )
    node_key, node_value = node_label.split("=", 1)
    roles = {
        "trpc-gateway": (
            "gateway",
            {
                "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE": _field(
                    _field(workload, "gateway"), "database_pool_min_size"
                ),
                "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE": _field(
                    _field(workload, "gateway"), "database_pool_max_size"
                ),
            },
        ),
        "trpc-worker": (
            "worker",
            {
                "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE": _field(
                    _field(workload, "worker"), "database_pool_min_size"
                ),
                "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE": _field(
                    _field(workload, "worker"), "database_pool_max_size"
                ),
                "TRPC_SERVICE_OFFLINE_AGENT_DELAY_SECONDS": _field(
                    _field(workload, "worker"), "offline_agent_delay_seconds"
                ),
            },
        ),
        "trpc-outbox-dispatcher": (
            "outbox-dispatcher",
            {
                "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE": _field(
                    _field(workload, "outbox"), "database_pool_min_size"
                ),
                "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE": _field(
                    _field(workload, "outbox"), "database_pool_max_size"
                ),
            },
        ),
        "trpc-session-recovery": (
            "session-recovery",
            {
                "TRPC_SERVICE_DATABASE_POOL_MIN_SIZE": _field(
                    _field(workload, "recovery"), "database_pool_min_size"
                ),
                "TRPC_SERVICE_DATABASE_POOL_MAX_SIZE": _field(
                    _field(workload, "recovery"), "database_pool_max_size"
                ),
                "TRPC_SERVICE_RECOVERY_POLL_SECONDS": _field(
                    _field(workload, "recovery"), "poll_seconds"
                ),
            },
        ),
    }
    for document in documents:
        if not isinstance(document, dict) or _kind(document) != "deployment":
            continue
        name = _name(document)
        if name not in roles:
            continue
        container_name, raw_environment = roles[name]
        if any(value is None for value in raw_environment.values()):
            raise RuntimeSupportRenderError(
                f"runtime config workload settings are incomplete for {name}"
            )
        spec = document.setdefault("spec", {})
        template = spec.setdefault("template", {})
        pod_spec = template.setdefault("spec", {})
        pod_spec["nodeSelector"] = {node_key: node_value}
        containers = pod_spec.setdefault("containers", [])
        container = next(
            (
                item
                for item in containers
                if isinstance(item, dict) and item.get("name") == container_name
            ),
            None,
        )
        if container is None:
            raise RuntimeSupportRenderError(
                f"performance workload patch is missing container {container_name}"
            )
        container["env"] = [
            {"name": key, "value": str(value)} for key, value in raw_environment.items()
        ]


def _render_performance_file(
    source: Path,
    target: Path,
    *,
    config: Any,
    performance: Any,
    image_repository: str,
    image_digest: str,
    relative_base: str,
) -> None:
    documents = _read_documents(source)
    rendered = copy.deepcopy(documents)
    if source.name == "kustomization.yaml":
        document = rendered[0] if rendered and isinstance(rendered[0], dict) else None
        if document is not None:
            document["namespace"] = str(_field(performance, "namespace"))
            resources = document.get("resources")
            if isinstance(resources, list):
                document["resources"] = [
                    (
                        relative_base
                        if item == "../../base"
                        else "im-external-egress"
                        if item == "../../im-external-egress"
                        else item
                    )
                    for item in resources
                ]
            images = document.get("images")
            if isinstance(images, list) and images and isinstance(images[0], dict):
                images[0]["newName"] = image_repository
                images[0]["digest"] = image_digest
    elif source.name == "namespace.yaml":
        document = rendered[0] if rendered and isinstance(rendered[0], dict) else None
        metadata = document.get("metadata") if isinstance(document, dict) else None
        if isinstance(metadata, dict):
            metadata["name"] = str(_field(performance, "namespace"))
    elif source.name == "performance-config-patch.yaml":
        document = rendered[0] if rendered and isinstance(rendered[0], dict) else None
        if isinstance(document, dict):
            document["data"] = _performance_manifest_values(config, performance)
    elif source.name == "performance-image-pull-secret-patch.yaml":
        pull_secret = _field(config, "image_pull_secret")
        if not isinstance(pull_secret, str) or not pull_secret.strip():
            raise RuntimeSupportRenderError(
                "runtime config does not provide a valid image pull Secret"
            )
        patch_document = rendered[0] if rendered and isinstance(rendered[0], list) else None
        patch = (
            patch_document[0]
            if patch_document and len(patch_document) == 1 and isinstance(patch_document[0], dict)
            else None
        )
        value = patch.get("value") if isinstance(patch, dict) else None
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
            raise RuntimeSupportRenderError("performance image pull patch is invalid")
        value[0]["name"] = pull_secret.strip()
    elif source.name == "performance-replicas-patch.yaml":
        workload = _field(performance, "workload")
        gateway = _field(workload, "gateway") if workload is not None else None
        gateway_replicas = _field(gateway, "replicas")
        worker_replicas = _field(performance, "workers")
        outbox = _field(workload, "outbox") if workload is not None else None
        recovery = _field(workload, "recovery") if workload is not None else None
        replicas = {
            "trpc-gateway": gateway_replicas,
            "trpc-worker": worker_replicas,
            "trpc-outbox-dispatcher": _field(outbox, "replicas"),
            "trpc-session-recovery": _field(recovery, "replicas"),
        }
        if any(value is None for value in replicas.values()):
            raise RuntimeSupportRenderError(
                "runtime config performance replica settings are incomplete"
            )
        for document in rendered:
            if not isinstance(document, dict) or _kind(document) != "deployment":
                continue
            name = _name(document)
            if name in replicas:
                spec = document.setdefault("spec", {})
                spec["replicas"] = int(replicas[name])
    elif source.name == "performance-workload-patch.yaml":
        _performance_workload_patch(rendered, performance)
    _write_documents(rendered, target)


def render_performance_overlay(
    config: str | Path,
    *,
    template_dir: str | Path = DEFAULT_PERFORMANCE_TEMPLATE,
    output_dir: str | Path = DEFAULT_PERFORMANCE_OUTPUT_DIR,
) -> Path:
    """Render an immutable, config-bound performance Kustomize overlay.

    The output is a copy of the checked-in opt-in overlay.  Its image host and
    digest come from the release binding after the configured pull-registry
    rewrite, so every candidate workload uses one identical immutable image.
    """

    config_path = Path(config)
    template_path = Path(template_dir).expanduser().resolve()
    output_path = Path(output_dir).expanduser().resolve()
    if not template_path.is_dir():
        raise RuntimeSupportRenderError(f"performance overlay template is missing: {template_path}")
    if output_path == template_path:
        raise RuntimeSupportRenderError(
            f"refusing to overwrite performance overlay template {template_path}"
        )
    loaded_config = _load_runtime_gate_config(config_path)
    performance = _performance_config(loaded_config)
    image_repository, image_digest = _performance_image(loaded_config)
    base_path = (template_path.parent.parent / "base").resolve()
    im_egress_path = (template_path.parent.parent / "im-external-egress").resolve()
    rendered_base = output_path / "base"
    rendered_im_egress = output_path / "im-external-egress"
    relative_base = "base"
    if not im_egress_path.is_dir():
        raise RuntimeSupportRenderError(f"IM external egress overlay is missing: {im_egress_path}")
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        shutil.copytree(base_path, rendered_base, dirs_exist_ok=True)
        shutil.copytree(im_egress_path, rendered_im_egress, dirs_exist_ok=True)
        for source in sorted(template_path.glob("*.yaml")):
            _render_performance_file(
                source,
                output_path / source.name,
                config=loaded_config,
                performance=performance,
                image_repository=image_repository,
                image_digest=image_digest,
                relative_base=relative_base,
            )
    except OSError as exc:
        raise RuntimeSupportRenderError(
            f"cannot render performance overlay directory {output_path}: {exc}"
        ) from exc
    return output_path


render_performance_topology = render_performance_overlay


render_runtime_manifests = render_runtime_support
render_manifests = render_runtime_support


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render ACK runtime-support Kubernetes YAML without applying it."
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="shared runtime-gate config YAML",
    )
    parser.add_argument(
        "--support-template",
        type=Path,
        default=DEFAULT_SUPPORT_TEMPLATE,
        help=f"Postgres/Redis support template (default: {DEFAULT_SUPPORT_TEMPLATE})",
    )
    parser.add_argument(
        "--minio-template",
        type=Path,
        default=DEFAULT_MINIO_TEMPLATE,
        help=f"MinIO template (default: {DEFAULT_MINIO_TEMPLATE})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"separate directory for rendered YAML (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--performance-output-dir",
        type=Path,
        default=None,
        help=(
            "optional output directory for the explicitly enabled performance overlay; "
            "the checked-in template is never modified"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        support_output, minio_output = render_runtime_support(
            args.config,
            support_template=args.support_template,
            minio_template=args.minio_template,
            output_dir=args.output_dir,
        )
        performance_output = (
            render_performance_overlay(
                args.config,
                output_dir=args.performance_output_dir,
            )
            if args.performance_output_dir is not None
            else None
        )
    except (OSError, RuntimeSupportRenderError, ValueError, yaml.YAMLError) as exc:
        print(f"render failed: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "support": str(support_output),
                "minio": str(minio_output),
                **({"performance": str(performance_output)} if performance_output else {}),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    if __package__ in {None, ""}:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
