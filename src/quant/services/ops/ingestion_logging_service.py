from __future__ import annotations
from datetime import date

from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.storage.db.repositories.ingestion_run_repository import IngestionRunRepository


class IngestionLoggingService:
    def __init__(self, session: Session) -> None:
        self.repo = IngestionRunRepository(session)

    def start_run(self, job_name: str, data_domain: str, target_date: date | None, attempt_no: int = 1) -> int:
        run = self.repo.create_run(
            job_name=job_name,
            data_domain=data_domain,
            target_date=target_date,
            status=RunStatus.RUNNING,
            attempt_no=attempt_no,
        )
        return run.ingest_run_id

    def finish_success(self, ingest_run_id: int, row_count: int = 0) -> None:
        self.repo.mark_success(ingest_run_id=ingest_run_id, row_count=row_count)

    def finish_warning(self, ingest_run_id: int, row_count: int = 0, message: str | None = None) -> None:
        self.repo.mark_warning(ingest_run_id=ingest_run_id, row_count=row_count, error_message=message)

    def finish_failed(self, ingest_run_id: int, message: str) -> None:
        self.repo.mark_failed(ingest_run_id=ingest_run_id, error_message=message)
