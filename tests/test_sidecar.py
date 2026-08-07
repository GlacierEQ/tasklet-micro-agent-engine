from __future__ import annotations

from pathlib import Path

from mastermind_sidecar import EXPECTED_PATHS, MastermindSidecar


def test_sidecar_reports_real_repository_evidence() -> None:
    report = MastermindSidecar().health_report()
    assert report["status"] == "healthy"
    assert report["paths_present"] == len(EXPECTED_PATHS)
    assert report["paths_expected"] == len(EXPECTED_PATHS)
    assert report["external_registration_verified"] is False
    assert report["deployment_verified"] is False
    for evidence in report["path_evidence"].values():
        assert evidence["present"] is True
        assert len(evidence["sha256"]) == 64


def test_sidecar_fails_degraded_when_repository_surface_is_absent(
    tmp_path: Path,
) -> None:
    report = MastermindSidecar(repository_root=tmp_path).health_report()
    assert report["status"] == "degraded"
    assert report["paths_present"] == 0
