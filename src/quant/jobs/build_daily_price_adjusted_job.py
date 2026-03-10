from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.adjustment.adjusted_price_builder_service import AdjustedPriceBuilderService


class BuildDailyPriceAdjustedJob(BaseJob):
    job_name = "build_daily_price_adjusted"
    data_domain = "ADJUSTED"

    def execute(self, session: Session, target_date: str | None, force: bool, run_mode: str, attempt_no: int) -> JobResult:
        if target_date is None:
            return JobResult(
                self.job_name,
                RunStatus.FAILED,
                target_date,
                message="target_date is required",
                metadata={"run_mode": run_mode, "attempt_no": attempt_no},
            )
        service = AdjustedPriceBuilderService(session=session)
        result = service.build(target_date=target_date, adjustment_version="v1_corporate_action")
        status = RunStatus.SUCCESS if result.row_count > 0 else RunStatus.FAILED
        return JobResult(
            self.job_name,
            status,
            target_date,
            result.row_count,
            message=result.message,
            artifacts=result.artifacts,
            metadata={
                "run_mode": run_mode,
                "attempt_no": attempt_no,
                "adjustment_version": result.adjustment_version,
            },
        )
