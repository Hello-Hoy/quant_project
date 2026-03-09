from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quant.bootstrap.container import Container
from quant.storage.db.base import Base
from quant.storage.db.models import (  # noqa: F401
    CorporateActionEvent,
    DataValidationResult,
    EtfMetadata,
    IngestionRun,
    InstrumentListingHistory,
    InstrumentMaster,
    MarketCalendar,
    PriceAdjustmentFactor,
    ResearchReadyStatus,
)
from quant.storage.db.session import engine


def main() -> None:
    Container().bootstrap()
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")


if __name__ == "__main__":
    main()
