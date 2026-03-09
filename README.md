# Quant Project Scaffold (KR Local-First)

Local-first scaffold for Korean equities/ETF quant research and trading.

## Scope
- Markets: KOSPI, KOSDAQ
- Assets: COMMON, ETF
- KONEX: metadata only (not tradable in initial universe)
- EOD: 10+ years target
- Intraday: request-based later
- NXT: disabled initially
- News: excluded initially

## Architecture (preserved)
- `providers/`: external API adapters only
- `storage/`: PostgreSQL and Parquet access only
- `services/`: business logic
- `jobs/`: executable units with unified lifecycle
- `pipelines/`: orchestration only
- `cli/`: entrypoints

## Storage model
- PostgreSQL: metadata/state/events/research readiness
- Parquet: raw prices, adjusted prices, universe snapshots, feature snapshots
- DuckDB: reserved for later research query layer

## EOD core pipeline order
1. `sync_market_calendar`
2. `sync_instrument_master`
3. `ingest_daily_price_raw`
4. `ingest_index_daily`
5. `validate_daily_market_data`
6. `build_price_adjustment_factor`
7. `build_daily_price_adjusted`
8. `build_universe_snapshot`
9. `build_feature_snapshot`
10. `update_research_ready_status`

## Current scaffold behavior
- KRX/KIS provider adapters are explicit placeholders.
- No fake live integrations are implemented.
- Calendar uses a weekday approximation placeholder.
- Factor builder currently writes identity factors (`v0_identity`) and marks the job as `WARNING`.
- `Container.bootstrap()` now fail-fast loads `configs/app.yaml`, `configs/providers.yaml`, and `configs/storage.yaml`.
- `settings.log_level`, `settings.timezone`, default universe, and default feature set are sourced from `configs/app.yaml` during bootstrap.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

## Initialize DB
```bash
PYTHONPATH=src python scripts/init_db.py
# or
make init-db
```

## Run pipelines
Single date:
```bash
PYTHONPATH=src python -m quant.cli.main eod run-date --date 2026-03-09
# or
make eod-date DATE=2026-03-09
```

Catch-up with range:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09
# or
make eod-catchup START=2026-03-01 END=2026-03-09
```
Range catch-up currently expands weekday dates only (calendar DB-aware discovery is a later enhancement).

Explicit date list:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --date 2026-03-07 --date 2026-03-08
```

## Validation rules implemented
- duplicate key check (`trade_date`, `instrument_id`)
- non-negative OHLCV check
- OHLC relationship check
- minimum row count check
- basic coverage ratio check

## Config files and runtime usage
- `configs/app.yaml`: bootstrap defaults (`timezone`, `log_level`, `universe_name`, `feature_set_name`)
- `configs/storage.yaml`: parquet root paths + partition metadata (path roots are used by `ParquetPaths`)
- `configs/providers.yaml`: provider-mode metadata/TODO tracking (validated at bootstrap)
- `configs/universe/*.yaml`: consumed by universe builder service
- `configs/feature_sets/*.yaml`: consumed by feature builder service

## Research-ready gate
A date becomes `research_ready=true` only when all are true:
- reference data ready (calendar + active instruments)
- raw daily price exists
- validation has no FAIL
- adjusted price exists
- feature snapshot exists

## Notes
- This scaffold is intentionally conservative and explicit.
- Provider TODOs are documented in adapter modules and `configs/providers.yaml`.
