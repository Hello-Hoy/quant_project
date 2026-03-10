from __future__ import annotations

import json
from datetime import date
from datetime import datetime
from datetime import timezone

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from quant.storage.db.models.corporate_action import CorporateActionEvent


class CorporateActionEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_events_for_instruments_up_to_date(
        self,
        instrument_ids: list[int],
        target_date: date,
    ) -> dict[int, list[CorporateActionEvent]]:
        if not instrument_ids:
            return {}

        stmt = (
            select(CorporateActionEvent)
            .where(
                CorporateActionEvent.instrument_id.in_(instrument_ids),
                or_(
                    and_(
                        CorporateActionEvent.effective_date.is_not(None),
                        CorporateActionEvent.effective_date <= target_date,
                    ),
                    and_(
                        CorporateActionEvent.effective_date.is_(None),
                        CorporateActionEvent.ex_date.is_not(None),
                        CorporateActionEvent.ex_date <= target_date,
                    ),
                ),
            )
            .order_by(
                CorporateActionEvent.instrument_id,
                CorporateActionEvent.effective_date,
                CorporateActionEvent.ex_date,
                CorporateActionEvent.event_id,
            )
        )

        grouped: dict[int, list[CorporateActionEvent]] = {}
        for row in self.session.scalars(stmt):
            grouped.setdefault(int(row.instrument_id), []).append(row)
        return grouped

    def upsert_event(
        self,
        instrument_id: int,
        event_type: str,
        source: str,
        announce_date: date | None = None,
        ex_date: date | None = None,
        effective_date: date | None = None,
        ratio_value: float | None = None,
        cash_value: float | None = None,
        raw_payload: dict | str | None = None,
    ) -> tuple[CorporateActionEvent, bool]:
        existing = self.session.scalar(
            select(CorporateActionEvent).where(
                CorporateActionEvent.instrument_id == instrument_id,
                CorporateActionEvent.event_type == event_type,
                CorporateActionEvent.source == source,
                CorporateActionEvent.announce_date == announce_date,
                CorporateActionEvent.ex_date == ex_date,
                CorporateActionEvent.effective_date == effective_date,
            )
        )

        payload_text: str | None
        if isinstance(raw_payload, dict):
            payload_text = json.dumps(raw_payload, sort_keys=True)
        else:
            payload_text = raw_payload

        now = datetime.now(timezone.utc)
        if existing is None:
            row = CorporateActionEvent(
                instrument_id=instrument_id,
                event_type=event_type,
                announce_date=announce_date,
                ex_date=ex_date,
                effective_date=effective_date,
                ratio_value=ratio_value,
                cash_value=cash_value,
                source=source,
                raw_payload=payload_text,
                created_at=now,
                updated_at=now,
            )
            self.session.add(row)
            self.session.flush()
            return row, True

        existing.ratio_value = ratio_value
        existing.cash_value = cash_value
        existing.raw_payload = payload_text
        existing.updated_at = now
        self.session.add(existing)
        self.session.flush()
        return existing, False
