from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.core.result import JobResult
from quant.core.time_utils import to_trade_date
from quant.services.ops.ingestion_logging_service import IngestionLoggingService
from quant.storage.db.session import SessionLocal


class BaseJob(ABC):
    job_name: str = "base_job"
    data_domain: str = "UNKNOWN"

    def run(
        self,
        target_date: str | None = None,
        force: bool = False,
        run_mode: str = "manual",
        attempt_no: int = 1,
    ) -> JobResult:
        trade_date: date | None = None
        if target_date is not None:
            try:
                trade_date = to_trade_date(target_date)
            except Exception as exc:
                return JobResult(
                    job_name=self.job_name,
                    status=RunStatus.FAILED,
                    target_date=target_date,
                    message=f"Invalid target_date={target_date}: {exc}",
                    metadata={"run_mode": run_mode, "attempt_no": attempt_no},
                )

        with SessionLocal() as session:
            logging_service = IngestionLoggingService(session)
            ingest_run_id = logging_service.start_run(
                job_name=self.job_name,
                data_domain=self.data_domain,
                target_date=trade_date,
                attempt_no=attempt_no,
            )
            # Persist run start before business logic so failures can always be recorded.
            session.commit()

            try:
                result = self.execute(
                    session=session,
                    target_date=target_date,
                    force=force,
                    run_mode=run_mode,
                    attempt_no=attempt_no,
                )
                if not result.job_name:
                    result.job_name = self.job_name
                result.metadata.setdefault("run_mode", run_mode)
                result.metadata.setdefault("attempt_no", attempt_no)
                if result.status == RunStatus.SUCCESS:
                    logging_service.finish_success(ingest_run_id=ingest_run_id, row_count=result.row_count)
                    session.commit()
                elif result.status in {RunStatus.WARNING, RunStatus.PARTIAL}:
                    logging_service.finish_warning(
                        ingest_run_id=ingest_run_id,
                        row_count=result.row_count,
                        message=result.message,
                    )
                    session.commit()
                else:
                    session.rollback()
                    logging_service.finish_failed(
                        ingest_run_id=ingest_run_id,
                        message=result.message or f"{self.job_name} returned FAILED",
                    )
                    session.commit()

                result.metadata["ingest_run_id"] = ingest_run_id
                return result
            except Exception as exc:
                session.rollback()
                logging_service.finish_failed(ingest_run_id=ingest_run_id, message=str(exc))
                session.commit()
                return JobResult(
                    job_name=self.job_name,
                    status=RunStatus.FAILED,
                    target_date=target_date,
                    message=str(exc),
                    metadata={
                        "run_mode": run_mode,
                        "attempt_no": attempt_no,
                        "ingest_run_id": ingest_run_id,
                    },
                )

    @abstractmethod
    def execute(
        self,
        session: Session,
        target_date: str | None,
        force: bool,
        run_mode: str,
        attempt_no: int,
    ) -> JobResult:
        raise NotImplementedError
