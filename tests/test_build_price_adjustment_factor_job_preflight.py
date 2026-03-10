from __future__ import annotations

from dataclasses import dataclass

from quant.core.enums import RunStatus
import quant.jobs.build_price_adjustment_factor_job as job_module
from quant.jobs.build_price_adjustment_factor_job import BuildPriceAdjustmentFactorJob


@dataclass
class _FakeFactorBuildResult:
    row_count: int
    factor_version: str
    derived_event_count_total: int
    preflight_corporate_action_synced: bool
    preflight_corporate_action_status: str | None
    message: str | None


class _FakeService:
    def __init__(self, session) -> None:  # noqa: ANN001
        _ = session

    def build(self, target_date: str, factor_version: str) -> _FakeFactorBuildResult:
        _ = target_date
        _ = factor_version
        return _FakeFactorBuildResult(
            row_count=5,
            factor_version="v1_corporate_action",
            derived_event_count_total=2,
            preflight_corporate_action_synced=False,
            preflight_corporate_action_status=None,
            message="preflight missing",
        )


def test_factor_job_returns_warning_when_preflight_not_synced(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(job_module, "FactorBuilderService", _FakeService)
    job = BuildPriceAdjustmentFactorJob()

    result = job.execute(
        session=object(),
        target_date="2026-03-09",
        force=False,
        run_mode="manual",
        attempt_no=1,
    )

    assert result.status == RunStatus.WARNING
    assert result.row_count == 5
    assert result.metadata["preflight_corporate_action_synced"] is False
    assert result.metadata["preflight_check_name"] == "sync_corporate_action_events"
    assert result.metadata["preflight_ready"] is False
    assert result.metadata["preflight_status"] == "MISSING"
    assert result.metadata["preflight_target_date"] == "2026-03-09"
