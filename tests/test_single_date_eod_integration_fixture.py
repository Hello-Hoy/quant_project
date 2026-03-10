from __future__ import annotations

from dataclasses import dataclass

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.pipelines.single_date_eod_pipeline import SingleDateEodPipeline


STEP_NAMES = [
    "sync_market_calendar",
    "sync_instrument_master",
    "ingest_daily_price_raw",
    "ingest_index_daily",
    "validate_daily_market_data",
    "build_price_adjustment_factor",
    "build_daily_price_adjusted",
    "build_universe_snapshot",
    "build_feature_snapshot",
    "update_research_ready_status",
]


@dataclass
class FixtureJob:
    job_name: str
    status: RunStatus
    row_count: int
    calls: list[str]

    def run(
        self,
        target_date: str | None = None,
        force: bool = False,
        run_mode: str = "manual",
        attempt_no: int = 1,
    ) -> JobResult:
        assert target_date == "2026-03-09"
        assert force is False
        assert run_mode == "manual"
        assert attempt_no == 1
        self.calls.append(self.job_name)
        return JobResult(
            job_name=self.job_name,
            status=self.status,
            target_date=target_date,
            row_count=self.row_count,
        )



def test_single_date_pipeline_runs_all_10_steps_in_order() -> None:
    calls: list[str] = []
    jobs = [FixtureJob(name, RunStatus.SUCCESS, 1, calls) for name in STEP_NAMES]
    pipeline = SingleDateEodPipeline(jobs=jobs)

    result = pipeline.run(target_date="2026-03-09", force=False)

    assert calls == STEP_NAMES
    assert result.status == RunStatus.SUCCESS
    assert len(result.results) == 10
    assert result.total_rows == 10



def test_single_date_pipeline_aggregates_warning() -> None:
    calls: list[str] = []
    jobs = [FixtureJob(name, RunStatus.SUCCESS, 1, calls) for name in STEP_NAMES]
    jobs[5] = FixtureJob(STEP_NAMES[5], RunStatus.WARNING, 1, calls)
    pipeline = SingleDateEodPipeline(jobs=jobs)

    result = pipeline.run(target_date="2026-03-09", force=False)

    assert result.status == RunStatus.WARNING
    assert len(result.results) == 10



def test_single_date_pipeline_stops_at_first_failure() -> None:
    calls: list[str] = []
    jobs = [FixtureJob(name, RunStatus.SUCCESS, 1, calls) for name in STEP_NAMES]
    jobs[3] = FixtureJob(STEP_NAMES[3], RunStatus.FAILED, 0, calls)
    pipeline = SingleDateEodPipeline(jobs=jobs)

    result = pipeline.run(target_date="2026-03-09", force=False)

    assert result.status == RunStatus.FAILED
    assert calls == STEP_NAMES[:4]
    assert len(result.results) == 4
