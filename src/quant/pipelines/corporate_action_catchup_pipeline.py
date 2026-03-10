from __future__ import annotations

from typing import Protocol

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.core.result import PipelineResult


class CorporateActionJobRunner(Protocol):
    job_name: str

    def run(
        self,
        target_date: str | None = None,
        force: bool = False,
        run_mode: str = "manual",
        attempt_no: int = 1,
    ) -> JobResult:
        ...


class CorporateActionCatchupPipeline:
    pipeline_name = "corporate_action_catchup_pipeline"

    def __init__(self, job_runner: CorporateActionJobRunner | None = None) -> None:
        if job_runner is not None:
            self.job_runner = job_runner
            return
        from quant.jobs.sync_corporate_action_events_job import SyncCorporateActionEventsJob

        self.job_runner = SyncCorporateActionEventsJob()

    def run_for_date(self, target_date: str, force: bool = False) -> PipelineResult:
        result = self.job_runner.run(
            target_date=target_date,
            force=force,
            run_mode="catchup",
            attempt_no=1,
        )
        return PipelineResult(
            pipeline_name=self.pipeline_name,
            status=result.status,
            results=[result],
            message=f"Corporate action sync completed for {target_date}",
        )

    def run_for_dates(self, target_dates: list[str], force: bool = False) -> PipelineResult:
        if not target_dates:
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status=RunStatus.WARNING,
                results=[],
                message="No target dates provided. Corporate action catch-up skipped.",
            )

        all_results: list[JobResult] = []
        for target_date in target_dates:
            result = self.run_for_date(target_date=target_date, force=force)
            all_results.extend(result.results)
            if result.has_failure:
                return PipelineResult(
                    pipeline_name=self.pipeline_name,
                    status=RunStatus.FAILED,
                    results=all_results,
                    message=f"Pipeline stopped at {target_date}",
                )

        final_status = (
            RunStatus.WARNING
            if any(item.status == RunStatus.WARNING for item in all_results)
            else RunStatus.SUCCESS
        )
        return PipelineResult(
            pipeline_name=self.pipeline_name,
            status=final_status,
            results=all_results,
            message=f"Corporate action catch-up completed for {len(target_dates)} date(s)",
        )
