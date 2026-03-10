from __future__ import annotations

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.metadata import build_preflight_metadata
from quant.core.result import JobResult
from quant.jobs.base import BaseJob
from quant.services.adjustment.factor_builder_service import FactorBuilderService


class BuildPriceAdjustmentFactorJob(BaseJob):
    job_name = "build_price_adjustment_factor"
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
        service = FactorBuilderService(session=session)
        result = service.build(target_date=target_date, factor_version="v1_corporate_action")
        status = RunStatus.SUCCESS if result.row_count > 0 else RunStatus.FAILED
        if result.row_count > 0 and not result.preflight_corporate_action_synced:
            status = RunStatus.WARNING
        preflight_metadata = build_preflight_metadata(
            check_name="sync_corporate_action_events",
            is_ready=result.preflight_corporate_action_synced,
            status=result.preflight_corporate_action_status,
            target_date=target_date,
        )
        return JobResult(
            self.job_name,
            status,
            target_date,
            result.row_count,
            message=result.message,
            metadata={
                "run_mode": run_mode,
                "attempt_no": attempt_no,
                "factor_version": result.factor_version,
                "derived_event_count_total": result.derived_event_count_total,
                "preflight_corporate_action_synced": result.preflight_corporate_action_synced,
                "preflight_corporate_action_status": result.preflight_corporate_action_status,
                **preflight_metadata,
            },
        )
