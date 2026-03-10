from __future__ import annotations
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant.storage.db.models.ops import ResearchReadyStatus


class ResearchReadyRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_status(
        self,
        trade_date: date,
        reference_ready: bool,
        raw_ready: bool,
        validated: bool,
        adjusted_ready: bool,
        feature_ready: bool,
        research_ready: bool,
        status_note: str | None = None,
    ) -> ResearchReadyStatus:
        existing = self.session.get(ResearchReadyStatus, trade_date)
        now = datetime.now(timezone.utc)
        if existing is None:
            existing = ResearchReadyStatus(
                trade_date=trade_date,
                reference_ready=reference_ready,
                raw_ready=raw_ready,
                validated=validated,
                adjusted_ready=adjusted_ready,
                feature_ready=feature_ready,
                research_ready=research_ready,
                status_note=status_note,
                updated_at=now,
            )
            self.session.add(existing)
            self.session.flush()
            return existing
        existing.reference_ready = reference_ready
        existing.raw_ready = raw_ready
        existing.validated = validated
        existing.adjusted_ready = adjusted_ready
        existing.feature_ready = feature_ready
        existing.research_ready = research_ready
        existing.status_note = status_note
        existing.updated_at = now
        self.session.add(existing)
        self.session.flush()
        return existing

    def get_status_map_in_range(self, start_date: date, end_date: date) -> dict[date, ResearchReadyStatus]:
        stmt = (
            select(ResearchReadyStatus)
            .where(
                ResearchReadyStatus.trade_date >= start_date,
                ResearchReadyStatus.trade_date <= end_date,
            )
            .order_by(ResearchReadyStatus.trade_date)
        )
        return {row.trade_date: row for row in self.session.scalars(stmt)}
