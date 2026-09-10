"""Run the local checks that define the supported development baseline."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import write_json

ROOT = Path(__file__).resolve().parents[1]


def run(label: str, command: list[str]) -> dict[str, object]:
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    return {
        "name": label,
        "command": " ".join(command),
        "returncode": result.returncode,
        "output": (result.stdout + result.stderr)[-3000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the tRPC Agent development quality gate")
    parser.add_argument("--unittest", action="store_true", help="also run unittest discovery")
    parser.add_argument("--output", type=Path, help="write the structured gate report to this path")
    args = parser.parse_args()

    checks = [
        run("compileall", [sys.executable, "-m", "compileall", "-q", "trpc_service", "tests"]),
        run("pytest", [sys.executable, "-m", "pytest", "-q"]),
        # Tests are validated by pytest; flake8 is intentionally scoped to
        # production code and release scripts so generated/coverage fixtures
        # cannot mask defects in the shipped service.
        run("flake8", [sys.executable, "-m", "flake8", "trpc_service", "scripts"]),
        run("ruff", [sys.executable, "-m", "ruff", "check", "trpc_service", "scripts", "tests"]),
    ]
    if args.unittest:
        checks.insert(
            2,
            run("unittest", [sys.executable, "-m", "unittest", "discover", "-s", "tests"]),
        )

    passed = all(item["returncode"] == 0 for item in checks)
    report = {"schema_version": 1, "gate": "quality", "ok": passed, "checks": checks}
    if args.output:
        write_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
