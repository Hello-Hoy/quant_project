from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from quant.providers.dart.corporate_action_provider import CorporateActionDisclosureRow
from quant.services.ingestion.corporate_action_ingestion_service import (
    CorporateActionIngestionService,
)


class _FakeProvider:
    def __init__(self, rows: list[CorporateActionDisclosureRow]) -> None:
        self.rows = rows

    def fetch_corporate_actions(self, start_date: str, end_date: str) -> list[CorporateActionDisclosureRow]:
        _ = start_date
        _ = end_date
        return self.rows

    def unavailable_message(self, capability: str, context: str | None = None) -> str:
        return f"unavailable:{capability}:{context}"


@dataclass
class _FakeInstrument:
    instrument_id: int


class _FakeInstrumentRepository:
    def __init__(self, mapping: dict[str, int]) -> None:
        self.mapping = mapping

    def get_preferred_by_symbol(self, symbol: str) -> _FakeInstrument | None:
        instrument_id = self.mapping.get(symbol)
        if instrument_id is None:
            return None
        return _FakeInstrument(instrument_id=instrument_id)


class _FakeCorporateActionEventRepository:
    def __init__(self) -> None:
        self.store: dict[tuple, dict] = {}

    def upsert_event(self, **kwargs: object) -> tuple[dict, bool]:
        key = (
            kwargs["instrument_id"],
            kwargs["event_type"],
            kwargs["source"],
            kwargs.get("announce_date"),
            kwargs.get("ex_date"),
            kwargs.get("effective_date"),
        )
        is_inserted = key not in self.store
        self.store[key] = kwargs
        return self.store[key], is_inserted


def test_corporate_action_ingestion_maps_and_skips_unmapped_symbols() -> None:
    rows = [
        CorporateActionDisclosureRow(
            symbol="005930",
            event_type="cash_dividend",
            announce_date=date(2026, 3, 1),
            ex_date=date(2026, 3, 10),
            effective_date=date(2026, 4, 1),
            cash_value=100.0,
        ),
        CorporateActionDisclosureRow(
            symbol="UNMAPPED",
            event_type="stock_split",
            announce_date=date(2026, 3, 2),
            ex_date=None,
            effective_date=None,
            ratio_value=2.0,
        ),
    ]
    event_repo = _FakeCorporateActionEventRepository()
    service = CorporateActionIngestionService(
        session=object(),
        provider=_FakeProvider(rows),
        event_repository=event_repo,
        instrument_repository=_FakeInstrumentRepository({"005930": 1}),
    )

    result = service.sync(start_date="2026-03-01", end_date="2026-03-31")

    assert result.row_count == 1
    assert result.mapped_count == 1
    assert result.inserted_count == 1
    assert result.updated_count == 0
    assert result.skipped_unmapped_count == 1
    assert result.event_type_counts == {"DIVIDEND_CASH": 1}


def test_corporate_action_ingestion_is_idempotent_for_same_key() -> None:
    row = CorporateActionDisclosureRow(
        symbol="005930",
        event_type="stock_split",
        announce_date=date(2026, 3, 1),
        ex_date=date(2026, 3, 10),
        effective_date=None,
        ratio_value=2.0,
    )
    event_repo = _FakeCorporateActionEventRepository()
    service = CorporateActionIngestionService(
        session=object(),
        provider=_FakeProvider([row]),
        event_repository=event_repo,
        instrument_repository=_FakeInstrumentRepository({"005930": 1}),
    )

    first = service.sync(start_date="2026-03-01", end_date="2026-03-31")
    second = service.sync(start_date="2026-03-01", end_date="2026-03-31")

    assert first.inserted_count == 1
    assert first.updated_count == 0
    assert second.inserted_count == 0
    assert second.updated_count == 1
