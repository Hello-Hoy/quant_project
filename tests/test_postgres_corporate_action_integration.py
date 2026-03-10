from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from quant.services.ops.catchup_date_discovery_service import CatchupDateDiscoveryService
from quant.services.adjustment.factor_builder_service import FactorBuilderService
from quant.storage.db.base import Base
import quant.storage.db.models as _models  # noqa: F401
from quant.storage.db.models.calendar import MarketCalendar
from quant.storage.db.repositories.calendar_repository import CalendarRepository
from quant.storage.db.repositories.corporate_action_event_repository import (
    CorporateActionEventRepository,
)
from quant.storage.db.repositories.ingestion_run_repository import IngestionRunRepository
from quant.storage.db.repositories.instrument_repository import InstrumentRepository
from quant.storage.db.repositories.price_adjustment_factor_repository import (
    PriceAdjustmentFactorRepository,
)
from quant.storage.db.repositories.research_ready_repository import ResearchReadyRepository
from quant.storage.parquet.daily_price_store import DailyPriceStore
from quant.storage.parquet.paths import ParquetPaths


def _require_test_db_url() -> str:
    db_url = os.getenv("TEST_POSTGRES_URL")
    if not db_url:
        pytest.skip("TEST_POSTGRES_URL is not set; skipping PostgreSQL integration test")
    return db_url


@pytest.fixture()
def pg_session() -> Session:
    db_url = _require_test_db_url()
    schema_name = f"test_quant_{uuid4().hex[:12]}"

    admin_engine = create_engine(db_url, future=True)
    with admin_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    engine = create_engine(
        db_url,
        future=True,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    Base.metadata.create_all(bind=engine)

    session_factory = sessionmaker(
        bind=engine,
        class_=Session,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )

    try:
        session = session_factory()
        yield session
        session.close()
    finally:
        engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        admin_engine.dispose()


def test_corporate_action_repository_upsert_is_idempotent(pg_session: Session) -> None:
    instrument_repo = InstrumentRepository(pg_session)
    instrument = instrument_repo.upsert_instrument(
        symbol="005930",
        name_kr="삼성전자",
        market_code="KOSPI",
        asset_type="COMMON",
        listing_status="ACTIVE",
        is_tradable=True,
        listing_date=date(1990, 1, 1),
        delisting_date=None,
    )
    repo = CorporateActionEventRepository(pg_session)

    first, inserted_first = repo.upsert_event(
        instrument_id=int(instrument.instrument_id),
        event_type="SPLIT",
        source="DART",
        announce_date=date(2026, 3, 1),
        ex_date=date(2026, 3, 10),
        effective_date=None,
        ratio_value=2.0,
        cash_value=None,
        raw_payload={"source_id": "A1"},
    )
    second, inserted_second = repo.upsert_event(
        instrument_id=int(instrument.instrument_id),
        event_type="SPLIT",
        source="DART",
        announce_date=date(2026, 3, 1),
        ex_date=date(2026, 3, 10),
        effective_date=None,
        ratio_value=2.0,
        cash_value=0.0,
        raw_payload={"source_id": "A1", "rev": 2},
    )

    pg_session.commit()

    assert inserted_first is True
    assert inserted_second is False
    assert int(first.event_id) == int(second.event_id)

    event_map = repo.get_events_for_instruments_up_to_date(
        instrument_ids=[int(instrument.instrument_id)],
        target_date=date(2026, 3, 10),
    )
    assert int(instrument.instrument_id) in event_map
    assert len(event_map[int(instrument.instrument_id)]) == 1
    stored = event_map[int(instrument.instrument_id)][0]
    assert float(stored.ratio_value or 0.0) == 2.0
    assert float(stored.cash_value or 0.0) == 0.0


def test_factor_builder_service_e2e_with_postgres_and_parquet(
    pg_session: Session,
    tmp_path: Path,
) -> None:
    instrument_repo = InstrumentRepository(pg_session)
    instrument_1 = instrument_repo.upsert_instrument(
        symbol="005930",
        name_kr="삼성전자",
        market_code="KOSPI",
        asset_type="COMMON",
        listing_status="ACTIVE",
        is_tradable=True,
        listing_date=date(1990, 1, 1),
        delisting_date=None,
    )
    instrument_2 = instrument_repo.upsert_instrument(
        symbol="000660",
        name_kr="SK하이닉스",
        market_code="KOSPI",
        asset_type="COMMON",
        listing_status="ACTIVE",
        is_tradable=True,
        listing_date=date(1990, 1, 1),
        delisting_date=None,
    )
    event_repo = CorporateActionEventRepository(pg_session)
    event_repo.upsert_event(
        instrument_id=int(instrument_1.instrument_id),
        event_type="SPLIT",
        source="DART",
        announce_date=date(2026, 3, 1),
        ex_date=date(2026, 3, 10),
        effective_date=None,
        ratio_value=2.0,
    )
    event_repo.upsert_event(
        instrument_id=int(instrument_2.instrument_id),
        event_type="DIVIDEND",
        source="DART",
        announce_date=date(2026, 3, 1),
        ex_date=date(2026, 3, 10),
        effective_date=None,
        ratio_value=5.0,
    )
    pg_session.commit()

    paths = ParquetPaths(
        raw_root=tmp_path / "raw",
        curated_root=tmp_path / "curated",
        feature_root=tmp_path / "features",
    )
    daily_store = DailyPriceStore(paths=paths)
    raw_df = pd.DataFrame(
        [
            {"trade_date": "2026-03-09", "instrument_id": int(instrument_1.instrument_id)},
            {"trade_date": "2026-03-09", "instrument_id": int(instrument_2.instrument_id)},
            {"trade_date": "2026-03-08", "instrument_id": int(instrument_1.instrument_id)},
        ]
    )
    daily_store.write_partition(raw_df, market_code="KOSPI", year=2026, month=3)

    service = FactorBuilderService(
        session=pg_session,
        daily_price_store=daily_store,
        factor_repository=PriceAdjustmentFactorRepository(pg_session),
        corporate_action_repository=event_repo,
    )

    result = service.build(target_date="2026-03-09", factor_version="v1_corporate_action")
    pg_session.commit()

    assert result.row_count == 2
    assert result.derived_event_count_total == 1

    factor_repo = PriceAdjustmentFactorRepository(pg_session)
    factor_map = factor_repo.get_factor_map_by_date(
        trade_date=date(2026, 3, 9),
        factor_version="v1_corporate_action",
    )
    assert factor_map[int(instrument_1.instrument_id)] == 2.0
    assert factor_map[int(instrument_2.instrument_id)] == 1.0


def test_catchup_discovery_can_reinclude_unsynced_preflight_dates(pg_session: Session) -> None:
    calendar_repo = CalendarRepository(pg_session)
    calendar_repo.upsert_many(
        [
            MarketCalendar(trade_date=date(2026, 3, 2), is_open=True, market_scope="KRX"),
            MarketCalendar(trade_date=date(2026, 3, 3), is_open=True, market_scope="KRX"),
        ]
    )

    ready_repo = ResearchReadyRepository(pg_session)
    ready_repo.upsert_status(
        trade_date=date(2026, 3, 2),
        reference_ready=True,
        raw_ready=True,
        validated=True,
        adjusted_ready=True,
        feature_ready=True,
        research_ready=True,
        status_note="ready",
    )
    ready_repo.upsert_status(
        trade_date=date(2026, 3, 3),
        reference_ready=True,
        raw_ready=True,
        validated=True,
        adjusted_ready=True,
        feature_ready=True,
        research_ready=True,
        status_note="ready",
    )

    run_repo = IngestionRunRepository(pg_session)
    run_repo.create_run(
        job_name="sync_corporate_action_events",
        data_domain="CORPORATE_ACTION",
        target_date=date(2026, 3, 3),
        status="SUCCESS",
        attempt_no=1,
    )
    pg_session.commit()

    service = CatchupDateDiscoveryService(session=pg_session)
    result = service.discover(
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 3),
        include_research_ready=False,
        include_unsynced_corporate_action_dates=True,
    )

    assert result.target_dates == ["2026-03-02"]
