from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.validation.daily_market_validation_service import DailyMarketValidationService


class ValidateDailyMarketDataJob(BaseJob):
    job_name = "validate_daily_market_data"
    data_domain = "DAILY_PRICE"

    def execute(self, session: Session, target_date: str | None, force: bool, run_mode: str, attempt_no: int) -> JobResult:
        if target_date is None:
            return JobResult(
                self.job_name,
                RunStatus.FAILED,
                target_date,
                message="target_date is required",
                metadata={"run_mode": run_mode, "attempt_no": attempt_no},
            )
        service = DailyMarketValidationService(session=session)
        result = service.validate(target_date=target_date)
        return JobResult(
            self.job_name,
            RunStatus(result.status),
            target_date,
            result.row_count,
            message=result.message,
            metadata={"run_mode": run_mode, "attempt_no": attempt_no, "validation_count": result.validation_count},
        )
