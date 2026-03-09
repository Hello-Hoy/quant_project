from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.readiness.research_ready_service import ResearchReadyService


class UpdateResearchReadyStatusJob(BaseJob):
    job_name = "update_research_ready_status"
    data_domain = "READINESS"

    def execute(self, session: Session, target_date: str | None, force: bool, run_mode: str, attempt_no: int) -> JobResult:
        if target_date is None:
            return JobResult(
                self.job_name,
                RunStatus.FAILED,
                target_date,
                message="target_date is required",
                metadata={"run_mode": run_mode, "attempt_no": attempt_no},
            )
        service = ResearchReadyService(session=session)
        result = service.update(target_date=target_date)
        status = RunStatus.SUCCESS if result.research_ready else RunStatus.WARNING
        return JobResult(
            self.job_name,
            status,
            target_date,
            1,
            message=result.message,
            metadata={
                "run_mode": run_mode,
                "attempt_no": attempt_no,
                "reference_ready": result.reference_ready,
                "raw_ready": result.raw_ready,
                "validated": result.validated,
                "adjusted_ready": result.adjusted_ready,
                "feature_ready": result.feature_ready,
                "research_ready": result.research_ready,
            },
        )
