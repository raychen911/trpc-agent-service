from __future__ import annotations

from typing import Any

import yaml

from scripts.split_kubernetes_runtime_manifest import split_stage1_manifests


def _documents(value: str) -> list[dict[str, Any]]:
    return [document for document in yaml.safe_load_all(value) if isinstance(document, dict)]


def test_split_stage1_manifests_enforces_migrate_head_check_runtime_order() -> None:
    rendered = """
apiVersion: v1
kind: Namespace
metadata:
  name: trpc-service
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: trpc-service-config
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: trpc-service
---
apiVersion: batch/v1
kind: Job
metadata:
  name: trpc-schema-migration
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/name: trpc-schema-migration
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: example.invalid/service@sha256:a
          command: [trpc-service]
          args: [migrate]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trpc-gateway
spec: {}
"""

    migration, head_check, runtime = split_stage1_manifests(
        rendered,
        namespace="trpc-service",
    )

    assert [(item["kind"], item["metadata"]["name"]) for item in _documents(migration)] == [
        ("ConfigMap", "trpc-service-config"),
        ("ServiceAccount", "trpc-service"),
        ("Job", "trpc-schema-migration"),
    ]
    head_documents = _documents(head_check)
    assert len(head_documents) == 1
    assert head_documents[0]["metadata"] == {
        "name": "trpc-schema-head-check",
        "namespace": "trpc-service",
        "labels": {
            "app.kubernetes.io/name": "trpc-schema-head-check",
            "app.kubernetes.io/component": "migration-head-check",
            "trpc.io/runtime-gate": "schema-head-check",
        },
    }
    head_container = head_documents[0]["spec"]["template"]["spec"]["containers"][0]
    assert head_container["command"] == ["trpc-service"]
    assert head_container["args"] == ["migrate", "--check"]
    assert [(item["kind"], item["metadata"]["name"]) for item in _documents(runtime)] == [
        ("Deployment", "trpc-gateway")
    ]
