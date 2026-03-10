from __future__ import annotations

from dataclasses import dataclass

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.pipelines.corporate_action_catchup_pipeline import (
    CorporateActionCatchupPipeline,
)


@dataclass
class FakeCorporateActionJob:
    by_date: dict[str, JobResult]
    job_name: str = "sync_corporate_action_events"

    def run(
        self,
        target_date: str | None = None,
        force: bool = False,
        run_mode: str = "manual",
        attempt_no: int = 1,
    ) -> JobResult:
        _ = force
        _ = run_mode
        _ = attempt_no
        assert target_date is not None
        return self.by_date[target_date]


def _job_result(status: RunStatus, target_date: str, rows: int) -> JobResult:
    return JobResult(
        job_name="sync_corporate_action_events",
        status=status,
        target_date=target_date,
        row_count=rows,
    )


def test_corporate_action_catchup_aggregates_warning_and_rows() -> None:
    pipeline = CorporateActionCatchupPipeline(
        job_runner=FakeCorporateActionJob(
            by_date={
                "2026-03-03": _job_result(RunStatus.SUCCESS, "2026-03-03", 3),
                "2026-03-04": _job_result(RunStatus.WARNING, "2026-03-04", 1),
            }
        )
    )

    result = pipeline.run_for_dates(target_dates=["2026-03-03", "2026-03-04"], force=False)

    assert result.status == RunStatus.WARNING
    assert result.total_rows == 4
    assert result.has_failure is False
    assert len(result.results) == 2


def test_corporate_action_catchup_stops_on_failure() -> None:
    pipeline = CorporateActionCatchupPipeline(
        job_runner=FakeCorporateActionJob(
            by_date={
                "2026-03-03": _job_result(RunStatus.SUCCESS, "2026-03-03", 3),
                "2026-03-04": _job_result(RunStatus.FAILED, "2026-03-04", 0),
                "2026-03-05": _job_result(RunStatus.SUCCESS, "2026-03-05", 2),
            }
        )
    )

    result = pipeline.run_for_dates(
        target_dates=["2026-03-03", "2026-03-04", "2026-03-05"],
        force=False,
    )

    assert result.status == RunStatus.FAILED
    assert result.total_rows == 3
    assert result.has_failure is True
    assert len(result.results) == 2
