from __future__ import annotations

import inspect

from quant.jobs.build_daily_price_adjusted_job import BuildDailyPriceAdjustedJob
from quant.jobs.build_feature_snapshot_job import BuildFeatureSnapshotJob
from quant.jobs.build_price_adjustment_factor_job import BuildPriceAdjustmentFactorJob
from quant.jobs.build_universe_snapshot_job import BuildUniverseSnapshotJob
from quant.jobs.ingest_daily_price_raw_job import IngestDailyPriceRawJob
from quant.jobs.ingest_index_daily_job import IngestIndexDailyJob
from quant.jobs.sync_instrument_master_job import SyncInstrumentMasterJob
from quant.jobs.sync_market_calendar_job import SyncMarketCalendarJob
from quant.jobs.update_research_ready_status_job import UpdateResearchReadyStatusJob
from quant.jobs.validate_daily_market_data_job import ValidateDailyMarketDataJob


EXPECTED_PARAMS = ["self", "session", "target_date", "force", "run_mode", "attempt_no"]


def test_all_jobs_share_execute_signature() -> None:
    job_types = [
        SyncMarketCalendarJob,
        SyncInstrumentMasterJob,
        IngestDailyPriceRawJob,
        IngestIndexDailyJob,
        ValidateDailyMarketDataJob,
        BuildPriceAdjustmentFactorJob,
        BuildDailyPriceAdjustedJob,
        BuildUniverseSnapshotJob,
        BuildFeatureSnapshotJob,
        UpdateResearchReadyStatusJob,
    ]

    for job_type in job_types:
        params = list(inspect.signature(job_type.execute).parameters.keys())
        assert params == EXPECTED_PARAMS, f"Unexpected execute signature in {job_type.__name__}: {params}"
