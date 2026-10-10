#!/usr/bin/env python3
"""Enforce line and branch coverage independently."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.report_io import atomic_write_json


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--minimum", type=float, default=90.0)
    parser.add_argument("--branch-minimum", type=float)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    totals = json.loads(args.report.read_text(encoding="utf-8"))["totals"]
    line = float(totals["percent_statements_covered"])
    branch = float(totals["percent_branches_covered"])
    branch_minimum = args.minimum if args.branch_minimum is None else args.branch_minimum
    result = {
        "baseline": {"line_percent": args.minimum, "branch_percent": branch_minimum},
        "candidate": {"line_percent": line, "branch_percent": branch},
        "case_deltas": {
            "line_percent": line - args.minimum,
            "branch_percent": branch - branch_minimum,
        },
        "gate": ("pass" if line >= args.minimum and branch >= branch_minimum else "fail"),
        "rejection_reasons": [
            name
            for name, value, minimum in (
                ("line_coverage", line, args.minimum),
                ("branch_coverage", branch, branch_minimum),
            )
            if value < minimum
        ],
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        rendered = atomic_write_json(args.output, result).rstrip("\n")
    print(rendered)
    return 0 if result["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
