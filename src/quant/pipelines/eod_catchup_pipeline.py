from __future__ import annotations

from typing import Protocol

from quant.core.enums import RunStatus
from quant.core.result import JobResult, PipelineResult


class SingleDateRunner(Protocol):
    def run(self, target_date: str, force: bool = False, run_mode: str = "manual") -> PipelineResult:
        ...


class EodCatchupPipeline:
    pipeline_name = "eod_catchup_pipeline"

    def __init__(self, single_date_pipeline: SingleDateRunner | None = None) -> None:
        if single_date_pipeline is not None:
            self.single_date_pipeline = single_date_pipeline
            return
        from quant.pipelines.single_date_eod_pipeline import SingleDateEodPipeline

        self.single_date_pipeline = SingleDateEodPipeline()

    def run_for_date(self, target_date: str, force: bool = False) -> PipelineResult:
        return self.single_date_pipeline.run(target_date=target_date, force=force, run_mode="catchup")

    def run_for_dates(self, target_dates: list[str], force: bool = False) -> PipelineResult:
        if not target_dates:
            return PipelineResult(
                self.pipeline_name,
                RunStatus.WARNING,
                [],
                "No target dates provided. Catch-up pipeline skipped.",
            )

        all_results: list[JobResult] = []
        for target_date in target_dates:
            result = self.run_for_date(target_date=target_date, force=force)
            all_results.extend(result.results)
            if result.has_failure:
                return PipelineResult(
                    self.pipeline_name,
                    RunStatus.FAILED,
                    all_results,
                    f"Pipeline stopped at {target_date}",
                )
        final_status = (
            RunStatus.WARNING
            if any(r.status == RunStatus.WARNING for r in all_results)
            else RunStatus.SUCCESS
        )
        return PipelineResult(
            self.pipeline_name,
            final_status,
            all_results,
            f"EOD catch-up pipeline completed for {len(target_dates)} date(s)",
        )
