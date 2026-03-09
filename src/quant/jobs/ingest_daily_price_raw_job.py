from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.ingestion.daily_price_ingestion_service import DailyPriceIngestionService


class IngestDailyPriceRawJob(BaseJob):
    job_name = "ingest_daily_price_raw"
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
        service = DailyPriceIngestionService(session=session)
        ingest_result = service.ingest(target_date=target_date, force=force)
        status = RunStatus.SUCCESS
        if ingest_result.message and ingest_result.row_count > 0:
            status = RunStatus.WARNING
        elif ingest_result.row_count == 0:
            status = RunStatus.FAILED
        return JobResult(
            self.job_name,
            status,
            target_date,
            ingest_result.row_count,
            message=ingest_result.message,
            artifacts=ingest_result.artifacts,
            metadata={"run_mode": run_mode, "attempt_no": attempt_no, "market_counts": ingest_result.market_counts},
        )
