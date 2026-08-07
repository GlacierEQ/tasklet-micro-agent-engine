"""Repository-local health evidence for tasklet-micro-agent-engine.

The sidecar is intentionally local. It proves only that the expected repository
surfaces exist and records their SHA-256 values. It does not claim registration
with an external control plane, deployment, or provider connectivity.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

EXPECTED_PATHS = (
    Path("src/tasklet_micro_agent_engine.py"),
    Path("tests/test_tasklet.py"),
    Path("pyproject.toml"),
    Path("README.md"),
)


class MastermindSidecar:
    """Produce a bounded local repository-health receipt."""

    def __init__(
        self,
        repo_name: str = "tasklet-micro-agent-engine",
        repository_root: Path | None = None,
    ) -> None:
        self.repo_name = repo_name
        self.repository_root = repository_root or Path(__file__).resolve().parent
        self.start_time = time.monotonic()

    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def health_report(self) -> dict[str, object]:
        evidence: dict[str, dict[str, object]] = {}
        for relative_path in EXPECTED_PATHS:
            path = self.repository_root / relative_path
            present = path.is_file()
            evidence[relative_path.as_posix()] = {
                "present": present,
                "sha256": self._sha256(path) if present else None,
            }

        present_count = sum(bool(item["present"]) for item in evidence.values())
        status = "healthy" if present_count == len(EXPECTED_PATHS) else "degraded"
        return {
            "schema": "glaciereq.repository-local-health.v1",
            "repo": self.repo_name,
            "uptime_seconds": round(time.monotonic() - self.start_time, 6),
            "status": status,
            "paths_present": present_count,
            "paths_expected": len(EXPECTED_PATHS),
            "path_evidence": evidence,
            "external_registration_verified": False,
            "deployment_verified": False,
        }

    def status(self) -> str:
        return json.dumps(self.health_report(), indent=2, sort_keys=True)


if __name__ == "__main__":
    print(MastermindSidecar().status())
