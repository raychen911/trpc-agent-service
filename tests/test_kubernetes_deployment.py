"""Regression checks for deployment-only Kubernetes integration contracts."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_channel_secrets_use_encrypted_tenant_secret_store() -> None:
    """Kubernetes must not restore the removed per-tenant file-secret path."""

    application = (PROJECT_ROOT / "deploy/kubernetes/application.yaml").read_text(encoding="utf-8")
    deploy_script = (PROJECT_ROOT / "deploy/kubernetes/deploy.sh").read_text(encoding="utf-8")

    assert "tenant_secret_master_key" in deploy_script
    assert "prune_unused_project_images" in deploy_script
    assert "trpc-channel-files" not in deploy_script
    assert "normalize_channel_secret_refs" not in deploy_script
    assert "channel-input" not in application
    assert "channel-output" not in application
    assert "mountPath: /run/secrets/tenants" not in application


def test_deploy_checks_cluster_readiness_before_building() -> None:
    """A disabled Docker Desktop cluster must not trigger an image build."""

    deploy_script = (PROJECT_ROOT / "deploy/kubernetes/deploy.sh").read_text(encoding="utf-8")

    assert "--request-timeout=5s get --raw=/readyz" in deploy_script
    assert deploy_script.index("get --raw=/readyz") < deploy_script.index(
        'docker build --tag "$IMAGE"')


def test_destructive_scripts_pin_the_local_cluster_context() -> None:
    """Never stop or remove a same-named namespace in another cluster."""

    for script_name in ("stop.sh", "remove.sh"):
        script = (PROJECT_ROOT / "deploy/kubernetes" / script_name).read_text(encoding="utf-8")
        assert 'kubectl config current-context' in script
        assert '!= "docker-desktop"' in script


def test_worker_scaler_has_narrow_deployment_scale_permission() -> None:
    application = (PROJECT_ROOT / "deploy/kubernetes/application.yaml").read_text(encoding="utf-8")
    config = (PROJECT_ROOT / "deploy/kubernetes/app.env").read_text(encoding="utf-8")
    deploy_script = (PROJECT_ROOT / "deploy/kubernetes/deploy.sh").read_text(encoding="utf-8")
    stop_script = (PROJECT_ROOT / "deploy/kubernetes/stop.sh").read_text(encoding="utf-8")

    assert "name: worker-scaler" in application
    assert 'resources: ["deployments/scale"]' in application
    assert 'resourceNames: ["agent-worker"]' in application
    assert "value: supervisor" in application
    assert "TRPC_SERVICE_WORKER_SCALER_MODE=kubernetes" in config
    assert "rollout status deployment/worker-scaler" in deploy_script
    assert stop_script.index("deployment/worker-scaler") < stop_script.index("deployment/gateway")

    scaler_start = application.index("kind: Deployment\nmetadata:\n  name: worker-scaler")
    scaler_end = application.index("kind: Deployment\nmetadata:\n  name: channel-runtime")
    scaler = application[scaler_start:scaler_end]
    assert "requests:\n              cpu: 10m\n              memory: 128Mi" in scaler
    assert "limits:\n              cpu: 200m\n              memory: 384Mi" in scaler


def test_redeploy_reloads_mutable_configuration_and_rotated_passwords() -> None:
    """A rollout must not keep stale ConfigMaps or file-backed credentials."""

    deploy_script = (PROJECT_ROOT / "deploy/kubernetes/deploy.sh").read_text(encoding="utf-8")

    assert "sync_database_password" in deploy_script
    assert "sync_grafana_password" in deploy_script
    assert "cat /run/secrets/platform/postgres_password" not in deploy_script
    assert "\\password trpc" in deploy_script
    assert "rollout restart deployment/tempo deployment/loki" in deploy_script
    assert "deployment/otel-collector deployment/prometheus" in deploy_script
    assert "deployment/grafana deployment/alloy" in deploy_script
    assert "rollout restart deployment/gateway deployment/agent-worker" in deploy_script
    assert "deployment/channel-runtime deployment/worker-scaler" in deploy_script
    # A newly built immutable image already changes the Pod template. Restarting
    # again creates a second Worker ReplicaSet and leaves draining Pods visible.
    application_rollout = deploy_script.split('"$DEPLOY_DIR/application.yaml" | kubectl apply -f -',
                                              1)[1]
    assert 'if [[ -n "$REUSE_IMAGE" ]]' in application_rollout
    assert "TRPC_K8S_REUSE_IMAGE" in deploy_script
    assert 'docker image inspect "$IMAGE"' in deploy_script


def test_seaweedfs_service_exposes_its_internal_volume_server() -> None:
    """The all-in-one filer must reach the volume URL it advertises via Service DNS."""

    storage = (PROJECT_ROOT / "deploy/kubernetes/storage.yaml").read_text(encoding="utf-8")
    service_start = storage.index("kind: Service\nmetadata:\n  name: seaweedfs")
    statefulset_start = storage.index("kind: StatefulSet\nmetadata:\n  name: seaweedfs")
    service = storage[service_start:statefulset_start]

    assert "name: volume" in service
    assert "port: 8080" in service
    assert "targetPort: 8080" in service
