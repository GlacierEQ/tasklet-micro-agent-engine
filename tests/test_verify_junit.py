from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERIFIER = ROOT / "scripts" / "verify_junit.py"


def test_all_skipped_junit_cannot_be_promoted(tmp_path: Path) -> None:
    junit = tmp_path / "all-skipped.xml"
    receipt = tmp_path / "receipt.json"
    junit.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites tests="2" failures="0" errors="0" skipped="2">
  <testsuite name="skipped" tests="2" failures="0" errors="0" skipped="2">
    <testcase classname="demo" name="one"><skipped /></testcase>
    <testcase classname="demo" name="two"><skipped /></testcase>
  </testsuite>
</testsuites>
""",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(VERIFIER),
            str(junit),
            "--output",
            str(receipt),
            "--commit",
            "fixture-head",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 1
    assert "positive non-skipped executed-test count" in completed.stderr
    assert not receipt.exists()


def test_mixed_executed_and_skipped_junit_is_counted_truthfully(
    tmp_path: Path,
) -> None:
    junit = tmp_path / "mixed.xml"
    receipt = tmp_path / "receipt.json"
    junit.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites tests="3" failures="0" errors="0" skipped="1">
  <testsuite name="mixed" tests="3" failures="0" errors="0" skipped="1">
    <testcase classname="demo" name="one" />
    <testcase classname="demo" name="two" />
    <testcase classname="demo" name="three"><skipped /></testcase>
  </testsuite>
</testsuites>
""",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(VERIFIER),
            str(junit),
            "--output",
            str(receipt),
            "--commit",
            "fixture-head",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert payload["tests"] == 3
    assert payload["executed"] == 2
    assert payload["skipped"] == 1
    assert payload["conclusion"] == "PASS"
