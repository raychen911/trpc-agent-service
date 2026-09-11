"""Multi-node canary rollout orchestration with health gates and rollback."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseResult:
    tenant_id: str
    target_version: int
    phases: tuple[int, ...]
    rolled_back: bool


def _call(base_url, token, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(base_url.rstrip("/") + path, data=data, method=method,
                                     headers={"Authorization": "Bearer " + token,
                                              "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {}


def _probe(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return 200 <= response.status < 300


def rollout(base_url: str, token: str, tenant_id: str, target_version: int, expected_active_version: int,
            phases: tuple[int, ...] = (1, 10, 25, 50, 100), probes: tuple[str, ...] = (),
            settle_seconds: float = 0) -> ReleaseResult:
    """Advance percentages only after every node probe passes; reset to 0 on failure."""
    completed = []
    path = f"/admin/tenants/{tenant_id}/rollout"
    for percent in phases:
        status, _ = _call(base_url, token, "POST", path, {
            "target_version": target_version, "expected_active_version": expected_active_version,
            "percent": percent,
        })
        if status != 200:
            _rollback(base_url, token, tenant_id, target_version, expected_active_version)
            return ReleaseResult(tenant_id, target_version, tuple(completed), True)
        if settle_seconds:
            time.sleep(settle_seconds)
        try:
            if not all(_probe(url) for url in probes):
                raise RuntimeError("release probe failed")
        except Exception:
            _rollback(base_url, token, tenant_id, target_version, expected_active_version)
            return ReleaseResult(tenant_id, target_version, tuple(completed), True)
        completed.append(percent)
    return ReleaseResult(tenant_id, target_version, tuple(completed), False)


def _rollback(base_url, token, tenant_id, target_version, expected_active_version):
    _call(base_url, token, "POST", f"/admin/tenants/{tenant_id}/rollout", {
        "target_version": target_version, "expected_active_version": expected_active_version, "percent": 0,
    })
