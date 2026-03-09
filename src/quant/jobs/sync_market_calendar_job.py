from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.reference.market_calendar_service import MarketCalendarService


class SyncMarketCalendarJob(BaseJob):
    job_name = "sync_market_calendar"
    data_domain = "CALENDAR"

    def execute(self, session: Session, target_date: str | None, force: bool, run_mode: str, attempt_no: int) -> JobResult:
        service = MarketCalendarService(session=session)
        sync_result = service.sync(target_date=target_date, force=force)
        status = RunStatus.SUCCESS if sync_result.row_count > 0 else RunStatus.WARNING
        return JobResult(
            self.job_name,
            status,
            target_date,
            sync_result.row_count,
            message=sync_result.message,
            metadata={"run_mode": run_mode, "attempt_no": attempt_no},
        )
