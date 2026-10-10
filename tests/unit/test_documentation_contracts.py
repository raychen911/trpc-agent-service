from __future__ import annotations

import json
from pathlib import Path

from deploy.im_probe import feishu_control_action, wecom_control_action

ROOT = Path(__file__).resolve().parents[2]


def test_wecom_documentation_uses_websocket_not_http_callback_sequence() -> None:
    architecture = (ROOT / "docs" / "architecture.md").read_text(encoding="utf-8")
    guide = (ROOT / "docs" / "im-channels.md").read_text(encoding="utf-8")

    assert 'wecom["企业微信"] <-->|WSS authenticated frames|' in architecture
    assert 'feishu["飞书"] -->|HTTPS callback|' in architecture
    required = (
        "pg_try_advisory_lock",
        "wss:443",
        "aibot_subscribe",
        "cmd=ping",
        "aibot_msg_callback",
        "durable acceptance",
        "aibot_send_msg",
        "same req_id, errcode/errmsg ACK",
        "ambiguous",
        "pg_advisory_unlock",
        "event.disconnected_event",
    )
    for marker in required:
        assert marker in guide
    assert "no HTTP 2xx" in guide


def test_im_documentation_preserves_independent_evidence_boundaries() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    guide = (ROOT / "docs" / "im-channels.md").read_text(encoding="utf-8")
    testing = (ROOT / "docs" / "testing.md").read_text(encoding="utf-8")
    operations = (ROOT / "docs" / "operations.md").read_text(encoding="utf-8")
    probe_readme = (ROOT / "deploy" / "im_probe" / "README.md").read_text(encoding="utf-8")
    nginx = (ROOT / "deploy" / "im_probe" / "nginx-server.conf.example").read_text(encoding="utf-8")

    for marker in (
        "wecom_connection_state",
        "im_acceptance_evidence_events",
        "OpenAPI witness",
        "服务侧 replay",
        "provider delivery gap",
    ):
        assert marker in guide
    for marker in (
        "8 个 case",
        "host-only control broker",
        "callback observer",
        "OpenAPI witness",
    ):
        assert marker in testing
    assert "破坏性生产灾备" in readme
    assert "--allow-functional-dr" in readme
    assert "唯一允许" in readme
    assert "功能灾备" in readme
    assert "production_gate=not_run" in readme
    assert "authorized_not_run_gates" in testing
    assert "destructive DR" in testing
    assert "--allow-functional-dr" in operations
    assert "policy" in operations
    assert "--allow-functional-dr" in probe_readme
    assert "online_im=not_run" in probe_readme
    assert "im-acceptance/runs" in probe_readme
    assert "run_binding_sha256" in probe_readme
    assert "迁移 `0021` 和 `0022`" in probe_readme
    assert "mirror /_trpc_feishu_callback_observer" in nginx
    assert "location ^~ /feishu-openapi/" in nginx


def test_im_control_action_templates_are_placeholder_only_and_broker_pins_them() -> None:
    deploy = ROOT / "deploy" / "im_probe"
    broker = json.loads((deploy / "control-broker.example.json").read_text(encoding="utf-8"))
    feishu = json.loads((deploy / "feishu-control-action.example.json").read_text(encoding="utf-8"))
    wecom = json.loads((deploy / "wecom-control-action.example.json").read_text(encoding="utf-8"))

    assert set(feishu) == feishu_control_action.CONFIG_FIELDS
    assert set(feishu["hooks"]) == set(feishu_control_action.ACTION_CASES)
    assert all(set(hook) == feishu_control_action.HOOK_FIELDS for hook in feishu["hooks"].values())
    assert set(wecom) == wecom_control_action.CONFIG_FIELDS

    expected_actions = {
        "feishu": set(feishu_control_action.ACTION_CASES),
        "wecom": {f"wecom_{case}" for case in wecom_control_action.CASES},
    }
    for channel, actions in expected_actions.items():
        configured = broker["channels"][channel]["allowed_actions"]
        config_path = f"/etc/trpc-im-probe/{channel}-control-action.json"
        assert set(configured) == actions
        assert all(value["argv"] == ["--config", config_path] for value in configured.values())

    placeholder_values = (
        feishu["tenant_id"],
        feishu["binding_id"],
        feishu["control_token_file"],
        feishu["evidence_base_url"],
        feishu["evidence_token_file"],
        *(hook["url"] for hook in feishu["hooks"].values()),
        wecom["tenant_id"],
        wecom["binding_id"],
        wecom["account_id_sha256"],
        wecom["admin_base_url"],
        wecom["admin_token_file"],
        wecom["control_base_url"],
        wecom["control_token_file"],
    )
    assert all("<" in value and ">" in value for value in placeholder_values)


def test_im_probe_documents_atomic_control_action_install_order() -> None:
    readme = (ROOT / "deploy" / "im_probe" / "README.md").read_text(encoding="utf-8")
    section = readme.split("### action 配置的原子安装、权限与检查顺序", maxsplit=1)[1]

    markers = (
        "systemctl stop trpc-im-control-broker.service",
        "root:root 0755",
        "root:trpcagent 0640",
        "sha256sum",
        "mv -T",
        "control_broker.py --check",
        "systemctl start trpc-im-control-broker.service",
    )
    positions = [section.index(marker) for marker in markers]
    assert positions == sorted(positions)


def test_im_probe_systemd_units_preserve_distinct_socket_peer_identity() -> None:
    deploy = ROOT / "deploy" / "im_probe"
    broker = (deploy / "trpc-im-control-broker.service").read_text(encoding="utf-8")
    observer = (deploy / "trpc-im-feishu-callback-observer.service").read_text(encoding="utf-8")
    witness = (deploy / "trpc-im-feishu-openapi-witness.service").read_text(encoding="utf-8")
    probe = (deploy / "trpc-im-probe.service").read_text(encoding="utf-8")
    readme = (deploy / "README.md").read_text(encoding="utf-8")
    environment = (deploy / "im-probe.env.example").read_text(encoding="utf-8")

    for host_component in (broker, observer, witness):
        assert "User=trpcimbroker" in host_component
        assert "Group=trpcagent" in host_component
        assert "RuntimeDirectory=trpc-im-probe" in host_component
        assert "RuntimeDirectoryMode=0750" in host_component

    assert "User=trpcagent" in probe
    assert "Group=trpcagent" in probe
    assert "User=trpcimbroker" not in probe
    assert "RuntimeDirectory=" not in probe
    assert '"socket_mode": "0660"' in (deploy / "control-broker.example.json").read_text(
        encoding="utf-8"
    )
    for marker in (
        "id -u trpcimbroker",
        "id -u trpcagent",
        "trpcimbroker:trpcagent 0750",
        "trpcimbroker:trpcagent 0660",
        "probe unit 不声明或接管该",
    ):
        assert marker in readme
    assert "TRPC_IM_PROBE_BROKER_UID=<numeric-trpcimbroker-uid>" in environment
    assert "TRPC_IM_PROBE_BROKER_GID=<numeric-trpcagent-group-gid>" in environment
