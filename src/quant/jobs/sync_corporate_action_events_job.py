from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.ingestion.corporate_action_ingestion_service import (
    CorporateActionIngestionService,
)


class SyncCorporateActionEventsJob(BaseJob):
    job_name = "sync_corporate_action_events"
    data_domain = "CORPORATE_ACTION"

    def execute(
        self,
        session: Session,
        target_date: str | None,
        force: bool,
        run_mode: str,
        attempt_no: int,
    ) -> JobResult:
        if target_date is None:
            return JobResult(
                self.job_name,
                RunStatus.FAILED,
                target_date,
                message="target_date is required",
                metadata={"run_mode": run_mode, "attempt_no": attempt_no},
            )

        service = CorporateActionIngestionService(session=session)
        result = service.sync(start_date=target_date, end_date=target_date, force=force)
        status = RunStatus.SUCCESS if result.row_count > 0 else RunStatus.WARNING
        return JobResult(
            self.job_name,
            status,
            target_date,
            row_count=result.row_count,
            message=result.message,
            metadata={
                "run_mode": run_mode,
                "attempt_no": attempt_no,
                "mapped_count": result.mapped_count,
                "inserted_count": result.inserted_count,
                "updated_count": result.updated_count,
                "skipped_unmapped_count": result.skipped_unmapped_count,
                "event_type_counts": result.event_type_counts,
            },
        )
