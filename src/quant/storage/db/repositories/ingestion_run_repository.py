from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant.core.enums import RunStatus
from quant.storage.db.models.ops import IngestionRun


class IngestionRunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_run(
        self,
        job_name: str,
        data_domain: str,
        target_date: date | None,
        status: str | RunStatus,
        attempt_no: int = 1,
    ) -> IngestionRun:
        now = datetime.now(timezone.utc)
        run = IngestionRun(
            job_name=job_name,
            data_domain=data_domain,
            target_date=target_date,
            status=RunStatus(status).value,
            attempt_no=attempt_no,
            started_at=now,
            finished_at=None,
            row_count=None,
            error_message=None,
            created_at=now,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def _get_run(self, ingest_run_id: int) -> IngestionRun:
        row = self.session.scalar(
            select(IngestionRun).where(IngestionRun.ingest_run_id == ingest_run_id)
        )
        if row is None:
            raise ValueError(f"IngestionRun not found: {ingest_run_id}")
        return row

    def mark_success(self, ingest_run_id: int, row_count: int = 0) -> None:
        run = self._get_run(ingest_run_id)
        run.status = RunStatus.SUCCESS.value
        run.row_count = row_count
        run.finished_at = datetime.now(timezone.utc)
        run.error_message = None
        self.session.add(run)
        self.session.flush()

    def mark_warning(self, ingest_run_id: int, row_count: int = 0, error_message: str | None = None) -> None:
        run = self._get_run(ingest_run_id)
        run.status = RunStatus.WARNING.value
        run.row_count = row_count
        run.error_message = error_message
        run.finished_at = datetime.now(timezone.utc)
        self.session.add(run)
        self.session.flush()

    def mark_failed(self, ingest_run_id: int, error_message: str) -> None:
        run = self._get_run(ingest_run_id)
        run.status = RunStatus.FAILED.value
        run.error_message = error_message
        run.finished_at = datetime.now(timezone.utc)
        self.session.add(run)
        self.session.flush()

    def get_latest_run_by_job_and_target_date(
        self,
        job_name: str,
        target_date: date,
    ) -> IngestionRun | None:
        return self.session.scalar(
            select(IngestionRun)
            .where(
                IngestionRun.job_name == job_name,
                IngestionRun.target_date == target_date,
            )
            .order_by(IngestionRun.ingest_run_id.desc())
            .limit(1)
        )

    def get_latest_runs_by_job_in_range(
        self,
        job_name: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, IngestionRun]:
        rows = list(
            self.session.scalars(
                select(IngestionRun)
                .where(
                    IngestionRun.job_name == job_name,
                    IngestionRun.target_date >= start_date,
                    IngestionRun.target_date <= end_date,
                )
                .order_by(IngestionRun.target_date, IngestionRun.ingest_run_id.desc())
            )
        )
        out: dict[date, IngestionRun] = {}
        for row in rows:
            if row.target_date is None:
                continue
            if row.target_date not in out:
                out[row.target_date] = row
        return out
