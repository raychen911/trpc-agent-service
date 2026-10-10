from __future__ import annotations

import json

from scripts.privacy_leak_gate import (
    SENTINEL_LABELS,
    _generate_sentinels,
    _path_result,
    _run_gate,
    main,
)


def test_offline_privacy_paths_pass_without_recording_raw_sentinels() -> None:
    sentinels = _generate_sentinels()
    assert tuple(sentinels) == SENTINEL_LABELS
    assert len(set(sentinels.values())) == len(SENTINEL_LABELS)
    assert all(len(value) >= 50 for value in sentinels.values())

    report = _run_gate(sentinels)
    rendered = json.dumps(report, ensure_ascii=False)

    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["raw_sentinel_count"] == 0
    assert set(report["candidate"]["paths"]) == {
        "json_log",
        "privacy_span_export",
        "error_response",
        "report_serialization",
    }
    assert all(value not in rendered for value in sentinels.values())


def test_raw_sentinel_scan_is_a_hard_failure() -> None:
    sentinels = _generate_sentinels()
    result = _path_result(
        "test",
        {"captured": sentinels["message_body"]},
        sentinels,
        redaction_applied=False,
    )

    assert result["gate"] == "fail"
    assert result["raw_sentinel_labels"] == ["message_body"]
    assert "message_body" in result["sentinel_labels"]
    assert sentinels["message_body"] not in json.dumps(result)


def test_privacy_gate_writes_json_first_report(tmp_path) -> None:
    output = tmp_path / "privacy-leak-offline.json"

    assert main(["--output", str(output)]) == 0

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["production_rejection_reasons"] == [
        "offline evidence does not scan real deployed logs or traces"
    ]
