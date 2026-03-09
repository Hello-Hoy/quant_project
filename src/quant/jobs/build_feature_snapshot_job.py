from __future__ import annotations

from sqlalchemy.orm import Session

from quant.bootstrap.settings import settings
from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.feature.feature_builder_service import FeatureBuilderService


class BuildFeatureSnapshotJob(BaseJob):
    job_name = "build_feature_snapshot"
    data_domain = "FEATURE"

    def execute(self, session: Session, target_date: str | None, force: bool, run_mode: str, attempt_no: int) -> JobResult:
        if target_date is None:
            return JobResult(
                self.job_name,
                RunStatus.FAILED,
                target_date,
                message="target_date is required",
                metadata={"run_mode": run_mode, "attempt_no": attempt_no},
            )
        service = FeatureBuilderService(session=session)
        result = service.build(target_date=target_date, feature_set_name=settings.default_feature_set_name, universe_name=settings.default_universe_name)
        status = RunStatus.SUCCESS if result.row_count > 0 else RunStatus.FAILED
        return JobResult(
            self.job_name,
            status,
            target_date,
            result.row_count,
            message=result.message,
            artifacts=result.artifacts,
            metadata={"run_mode": run_mode, "attempt_no": attempt_no, "feature_set_name": result.feature_set_name},
        )
