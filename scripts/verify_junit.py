#!/usr/bin/env python3
"""Validate pytest JUnit evidence and emit a SHA-bound test receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def integer_attribute(root: ET.Element, name: str) -> int:
    raw = root.attrib.get(name, "0")
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"invalid JUnit {name}: {raw!r}") from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("junit", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit", default=os.environ.get("GITHUB_SHA", "LOCAL"))
    args = parser.parse_args()

    try:
        tree = ET.parse(args.junit)
        root = tree.getroot()
        suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        if not suites:
            raise ValueError("JUnit contains no testsuite")
        tests = sum(integer_attribute(suite, "tests") for suite in suites)
        failures = sum(integer_attribute(suite, "failures") for suite in suites)
        errors = sum(integer_attribute(suite, "errors") for suite in suites)
        skipped = sum(integer_attribute(suite, "skipped") for suite in suites)
        executed = tests - skipped
        if failures or errors:
            raise ValueError(
                f"test evidence is not green: failures={failures} errors={errors}"
            )
        if executed <= 0:
            raise ValueError("positive non-skipped executed-test count is required")
        receipt = {
            "schema": "glaciereq.tasklet.test-receipt.v1",
            "commit": args.commit,
            "tests": tests,
            "executed": executed,
            "failures": failures,
            "errors": errors,
            "skipped": skipped,
            "junit_sha256": sha256(args.junit),
            "evidence": "TEST",
            "conclusion": "PASS",
        }
        encoded = json.dumps(
            receipt,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        receipt["receipt_sha256"] = hashlib.sha256(encoded).hexdigest()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(receipt, indent=2, sort_keys=True))
        return 0
    except (ET.ParseError, OSError, ValueError) as exc:
        print(f"JUnit verification failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
