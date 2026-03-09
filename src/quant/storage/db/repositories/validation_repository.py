from __future__ import annotations
from datetime import date, datetime, timezone
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant.storage.db.models.ops import DataValidationResult


class ValidationRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_result(
        self,
        target_date: date,
        data_domain: str,
        check_name: str,
        result: str,
        detail: str | None = None,
        run_id: int | None = None,
    ) -> DataValidationResult:
        existing = self.session.scalar(
            select(DataValidationResult).where(
                DataValidationResult.target_date == target_date,
                DataValidationResult.data_domain == data_domain,
                DataValidationResult.check_name == check_name,
                DataValidationResult.run_id == run_id,
            )
        )
        if existing is not None:
            existing.result = result
            existing.detail = detail
            self.session.add(existing)
            self.session.flush()
            return existing

        row = DataValidationResult(
            target_date=target_date,
            data_domain=data_domain,
            check_name=check_name,
            result=result,
            detail=detail,
            run_id=run_id,
            created_at=datetime.now(timezone.utc),
        )
        self.session.add(row)
        self.session.flush()
        return row

    def add_many(
        self,
        target_date: date,
        data_domain: str,
        results: Iterable[dict[str, str | None]],
        run_id: int | None = None,
    ) -> int:
        count = 0
        for item in results:
            self.add_result(
                target_date=target_date,
                data_domain=data_domain,
                check_name=str(item["check_name"]),
                result=str(item["result"]),
                detail=str(item["detail"]) if item.get("detail") is not None else None,
                run_id=run_id,
            )
            count += 1
        return count

    def get_results_by_date_domain(self, target_date: date, data_domain: str) -> list[DataValidationResult]:
        return list(
            self.session.scalars(
                select(DataValidationResult).where(
                    DataValidationResult.target_date == target_date,
                    DataValidationResult.data_domain == data_domain,
                )
            )
        )
