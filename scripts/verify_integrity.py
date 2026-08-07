#!/usr/bin/env python3
"""Generate or verify the Tasklet repository integrity manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / ".integrity" / "file_hashes.json"
EXCLUDED = {
    ".integrity/file_hashes.json",
}


class IntegrityError(RuntimeError):
    """Raised when repository integrity cannot be evaluated safely."""


def tracked_paths() -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise IntegrityError(
            completed.stderr.decode("utf-8", errors="replace")
            or "git ls-files failed"
        )
    paths = [
        value.decode("utf-8")
        for value in completed.stdout.split(b"\0")
        if value
    ]
    if not paths:
        raise IntegrityError("tracked-file set is empty")
    return sorted(paths)


def safe_file(relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise IntegrityError(f"unsafe tracked path: {relative!r}")
    resolved = (ROOT / path).resolve()
    try:
        resolved.relative_to(ROOT)
    except ValueError as exc:
        raise IntegrityError(f"tracked path escapes repository: {relative!r}") from exc
    if resolved.is_symlink():
        raise IntegrityError(f"symlink is not allowed in integrity scope: {relative}")
    if not resolved.is_file():
        raise IntegrityError(f"tracked path is not a regular file: {relative}")
    return resolved


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest() -> dict[str, object]:
    hashes: dict[str, str] = {}
    for relative in tracked_paths():
        if relative in EXCLUDED:
            continue
        hashes[relative] = sha256(safe_file(relative))
    return {
        "schema": "glaciereq.tasklet.integrity-manifest.v2",
        "algorithm": "sha256",
        "scope": "all_git_tracked_regular_files_except_manifest_itself",
        "file_count": len(hashes),
        "hashes": hashes,
    }


def canonical(value: dict[str, object]) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", type=Path)
    args = parser.parse_args()

    try:
        candidate = build_manifest()
        text = canonical(candidate)
        if args.write:
            output = args.write
            if not output.is_absolute():
                output = ROOT / output
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(text, encoding="utf-8")
            print(f"wrote {output}")
            return 0

        if not MANIFEST.is_file():
            raise IntegrityError("integrity manifest is missing")
        existing = json.loads(MANIFEST.read_text(encoding="utf-8"))
        if existing != candidate:
            raise IntegrityError(
                "integrity manifest drift; generate a candidate with "
                "scripts/verify_integrity.py --write <path>"
            )
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "schema": candidate["schema"],
                    "file_count": candidate["file_count"],
                },
                sort_keys=True,
            )
        )
        return 0
    except (IntegrityError, OSError, json.JSONDecodeError) as exc:
        print(f"integrity verification failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
