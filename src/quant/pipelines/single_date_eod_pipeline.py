from __future__ import annotations

from typing import Protocol

from quant.core.enums import RunStatus
from quant.core.result import JobResult, PipelineResult


class JobRunner(Protocol):
    job_name: str

    def run(
        self,
        target_date: str | None = None,
        force: bool = False,
        run_mode: str = "manual",
        attempt_no: int = 1,
    ) -> JobResult:
        ...


class SingleDateEodPipeline:
    pipeline_name = "single_date_eod_pipeline"

    def __init__(self, jobs: list[JobRunner] | None = None) -> None:
        self.jobs = jobs or self._build_default_jobs()

    def _build_default_jobs(self) -> list[JobRunner]:
        from quant.jobs.build_daily_price_adjusted_job import BuildDailyPriceAdjustedJob
        from quant.jobs.build_feature_snapshot_job import BuildFeatureSnapshotJob
        from quant.jobs.build_price_adjustment_factor_job import BuildPriceAdjustmentFactorJob
        from quant.jobs.build_universe_snapshot_job import BuildUniverseSnapshotJob
        from quant.jobs.ingest_daily_price_raw_job import IngestDailyPriceRawJob
        from quant.jobs.ingest_index_daily_job import IngestIndexDailyJob
        from quant.jobs.sync_instrument_master_job import SyncInstrumentMasterJob
        from quant.jobs.sync_market_calendar_job import SyncMarketCalendarJob
        from quant.jobs.update_research_ready_status_job import UpdateResearchReadyStatusJob
        from quant.jobs.validate_daily_market_data_job import ValidateDailyMarketDataJob

        return [
            SyncMarketCalendarJob(),
            SyncInstrumentMasterJob(),
            IngestDailyPriceRawJob(),
            IngestIndexDailyJob(),
            ValidateDailyMarketDataJob(),
            BuildPriceAdjustmentFactorJob(),
            BuildDailyPriceAdjustedJob(),
            BuildUniverseSnapshotJob(),
            BuildFeatureSnapshotJob(),
            UpdateResearchReadyStatusJob(),
        ]

    def run(self, target_date: str, force: bool = False, run_mode: str = "manual") -> PipelineResult:
        results: list[JobResult] = []
        for job in self.jobs:
            result = job.run(target_date=target_date, force=force, run_mode=run_mode, attempt_no=1)
            results.append(result)
            if result.is_failure:
                return PipelineResult(
                    self.pipeline_name,
                    RunStatus.FAILED,
                    results,
                    f"{job.job_name} failed on {target_date}",
                )
        final_status = (
            RunStatus.WARNING if any(r.status == RunStatus.WARNING for r in results) else RunStatus.SUCCESS
        )
        return PipelineResult(
            self.pipeline_name,
            final_status,
            results,
            f"Single-date EOD pipeline completed for {target_date}",
        )
