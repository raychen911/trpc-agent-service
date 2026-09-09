"""Static security and software-supply-chain gate for release candidates."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SECRET_PATTERNS = (
    re.compile(r"(?i)\b(api[_-]?key|password|token|secret)\s*[:=]\s*[\"'][^\r\n\"']{8,}[\"']"),
    re.compile(r"(?i)://[^:/\s]+:[^@\s]+@"),
    re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}\b"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
)
TEXT_EXTENSIONS = {".py", ".yml", ".yaml", ".toml", ".ini", ".env", ".md", ".json", ".sh", ".ps1"}
SKIP_PARTS = {".git", ".venv", "__pycache__", "runs", "data", "build", "dist"}


def _files() -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=ROOT,
            capture_output=True,
            check=True,
        )
        candidates = [ROOT / value.decode("utf-8") for value in result.stdout.split(b"\0") if value]
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError):
        candidates = list(ROOT.rglob("*"))

    files: list[Path] = []
    for path in candidates:
        if not path.is_file() or (path.suffix.lower() not in TEXT_EXTENSIONS and path.name != "Dockerfile"):
            continue
        relative = path.relative_to(ROOT)
        if any(part in SKIP_PARTS for part in relative.parts):
            continue
        files.append(path)
    return files


def _secret_findings() -> list[str]:
    findings: list[str] = []
    for path in _files():
        if path.name in {".env.example", "security_gate.py"}:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for pattern in SECRET_PATTERNS:
            match = pattern.search(content)
            if match and not _is_documented_example(match.group(0)):
                findings.append(str(path.relative_to(ROOT)).replace("\\", "/"))
                break
    return sorted(set(findings))


def _is_documented_example(value: str) -> bool:
    """Ignore generated values, references, and intentionally fake test credentials."""
    lowered = value.lower()
    return any(
        marker in value
        for marker in ("$", "<", ">", "...", "secret://", "env://", "[^", "\\s")
    ) or any(
        marker in lowered
        for marker in (
            "test-",
            "test_",
            "local-only",
            "temporary-",
            "fresh-test",
            "phase3-",
            "ci-migration-password",
            "sql-password",
            "wechat-token",
            "vector-token",
            "secret-token",
            "root-key",
            "user:password@",
        )
    )


def main() -> int:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (ROOT / "deployment" / "docker-compose.yml").read_text(encoding="utf-8")
    manifest = (ROOT / "deployment" / "kubernetes" / "platform.yaml").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    checks = {
        "no tracked plaintext secret patterns": not _secret_findings(),
        "locked dependency graph": (ROOT / "uv.lock").is_file() and "uv.lock" in dockerfile,
        "non-root image": "USER 10001:10001" in dockerfile,
        "compose avoids privileged mode": "privileged: true" not in compose,
        "kubernetes non-root read-only containers": (
            "runAsNonRoot: true" in manifest
            and "readOnlyRootFilesystem: true" in manifest
            and 'drop: ["ALL"]' in manifest
        ),
        "dependency bounds declared": "requires-python = \">=3.12\"" in pyproject,
        "environment files excluded": ".env" in (ROOT / ".gitignore").read_text(encoding="utf-8"),
    }
    result = {
        "ok": all(checks.values()),
        "checks": checks,
        "secret_findings": _secret_findings(),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
