from __future__ import annotations

from dataclasses import dataclass

from quant.core.enums import RunStatus
import quant.jobs.update_research_ready_status_job as job_module
from quant.jobs.update_research_ready_status_job import UpdateResearchReadyStatusJob


@dataclass
class _FakeReadyResult:
    reference_ready: bool
    raw_ready: bool
    validated: bool
    adjusted_ready: bool
    feature_ready: bool
    corporate_action_sync_ready: bool
    corporate_action_sync_status: str | None
    require_corporate_action_sync_preflight: bool
    research_ready: bool
    message: str | None


class _FakeService:
    def __init__(self, session) -> None:  # noqa: ANN001
        _ = session

    def update(self, target_date: str) -> _FakeReadyResult:
        _ = target_date
        return _FakeReadyResult(
            reference_ready=True,
            raw_ready=True,
            validated=True,
            adjusted_ready=True,
            feature_ready=True,
            corporate_action_sync_ready=False,
            corporate_action_sync_status=None,
            require_corporate_action_sync_preflight=True,
            research_ready=False,
            message="not ready",
        )


def test_update_research_ready_job_emits_standard_preflight_metadata(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(job_module, "ResearchReadyService", _FakeService)
    job = UpdateResearchReadyStatusJob()

    result = job.execute(
        session=object(),
        target_date="2026-03-09",
        force=False,
        run_mode="manual",
        attempt_no=1,
    )

    assert result.status == RunStatus.WARNING
    assert result.metadata["preflight_check_name"] == "sync_corporate_action_events"
    assert result.metadata["preflight_ready"] is False
    assert result.metadata["preflight_status"] == "MISSING"
    assert result.metadata["preflight_target_date"] == "2026-03-09"
