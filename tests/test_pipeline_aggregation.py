from __future__ import annotations

from dataclasses import dataclass

from quant.core.enums import RunStatus
from quant.core.result import JobResult, PipelineResult
from quant.pipelines.eod_catchup_pipeline import EodCatchupPipeline


@dataclass
class FakeSingleDatePipeline:
    by_date: dict[str, PipelineResult]

    def run(self, target_date: str, force: bool = False, run_mode: str = "manual") -> PipelineResult:
        _ = force
        _ = run_mode
        return self.by_date[target_date]



def _single_date_result(status: RunStatus, date: str, rows: int) -> PipelineResult:
    return PipelineResult(
        pipeline_name="single_date_eod_pipeline",
        status=status,
        results=[
            JobResult(
                job_name="sample_job",
                status=status,
                target_date=date,
                row_count=rows,
            )
        ],
        message=f"{status.value} for {date}",
    )



def test_catchup_pipeline_aggregates_rows_and_warning_status() -> None:
    fake = FakeSingleDatePipeline(
        by_date={
            "2026-03-03": _single_date_result(RunStatus.SUCCESS, "2026-03-03", 10),
            "2026-03-04": _single_date_result(RunStatus.WARNING, "2026-03-04", 20),
        }
    )
    pipeline = EodCatchupPipeline(single_date_pipeline=fake)

    result = pipeline.run_for_dates(["2026-03-03", "2026-03-04"], force=False)

    assert result.status == RunStatus.WARNING
    assert result.total_rows == 30
    assert result.has_failure is False
    assert len(result.results) == 2



def test_catchup_pipeline_stops_on_failure() -> None:
    fake = FakeSingleDatePipeline(
        by_date={
            "2026-03-03": _single_date_result(RunStatus.SUCCESS, "2026-03-03", 10),
            "2026-03-04": _single_date_result(RunStatus.FAILED, "2026-03-04", 0),
            "2026-03-05": _single_date_result(RunStatus.SUCCESS, "2026-03-05", 10),
        }
    )
    pipeline = EodCatchupPipeline(single_date_pipeline=fake)

    result = pipeline.run_for_dates(["2026-03-03", "2026-03-04", "2026-03-05"], force=False)

    assert result.status == RunStatus.FAILED
    assert result.total_rows == 10
    assert result.has_failure is True
    assert len(result.results) == 2
