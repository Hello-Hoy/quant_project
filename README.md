# Quant Project Scaffold (KR Local-First)

Local-first scaffold for Korean equities/ETF quant research and trading.

Architecture 기준선 문서: [docs/ARCHITECTURE_SNAPSHOT.md](/Users/hyohee/Documents/quant_project/docs/ARCHITECTURE_SNAPSHOT.md)
운영 runbook: [docs/OPS_CORPORATE_ACTION_FACTOR_RUNBOOK.md](/Users/hyohee/Documents/quant_project/docs/OPS_CORPORATE_ACTION_FACTOR_RUNBOOK.md)

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
- Live mode providers use a shared transport scaffold (`base_url` + `endpoints`) and normalize payloads into typed rows.
- Provider transport currently accepts only `list[dict]` or `{"data": list[dict]}` JSON responses.
- Factor builder computes `v1_corporate_action` factors from `corporate_action_event` records.
- Corporate action ingestion canonically maps event types (e.g. `cash_dividend` -> `DIVIDEND_CASH`, `stock_split` -> `SPLIT`).
- If no applicable corporate action events exist, factors remain `1.0` (identity fallback), but this is now a data-driven result.
- `build_price_adjustment_factor` runs a preflight check for `sync_corporate_action_events` on the same date; missing preflight marks the job as `WARNING` (non-blocking).
- Provider `mode=live`는 endpoint/base_url이 설정되지 않으면 명시적으로 실패하도록 되어 있음(조용한 가짜 성공 방지).
- `Container.bootstrap()` now fail-fast loads `configs/app.yaml`, `configs/providers.yaml`, and `configs/storage.yaml`.
- `settings.log_level`, `settings.timezone`, default universe, and default feature set are sourced from `configs/app.yaml` during bootstrap.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

## Test
```bash
PYTHONPATH=src python -m pytest -q
# or
make test
```

PostgreSQL integration tests (opt-in):
```bash
TEST_POSTGRES_URL=postgresql+psycopg://postgres:postgres@localhost:5432/quant \
PYTHONPATH=src python -m pytest -q tests/test_postgres_corporate_action_integration.py
# or
make test-postgres-integration TEST_POSTGRES_URL=postgresql+psycopg://postgres:postgres@localhost:5432/quant
```
`TEST_POSTGRES_URL`가 없으면 해당 통합 테스트는 skip된다.

## Initialize DB
```bash
PYTHONPATH=src python scripts/init_db.py
# or
make init-db
```

## Preflight (before data loading)
```bash
PYTHONPATH=src python -m quant.cli.main eod preflight
# or
make preflight
```
DB 연결만 확인하고 스키마 검사는 건너뛰려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod preflight --skip-db-schema
```
DB 점검 자체를 건너뛰고(config/path만 확인) 사전 준비 상태를 보려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod preflight --skip-db
```
데이터 적재 전 단계(테스트 + DB 제외 preflight)를 한 번에 확인:
```bash
make pre-data-ready
```

## Run pipelines
Single date:
```bash
PYTHONPATH=src python -m quant.cli.main eod run-date --date 2026-03-09
# or
make eod-date DATE=2026-03-09
```
preflight metadata 요약을 함께 보려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod run-date --date 2026-03-09 --verbose-preflight
```

Catch-up with range:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09
# or
make eod-catchup START=2026-03-01 END=2026-03-09
```
Catch-up date discovery uses `market_calendar` + `research_ready_status` first.
If DB discovery is unavailable, it falls back to weekday-based discovery.
`configs/app.yaml`의 `catchup.include_unsynced_corporate_action_dates_default`가
corporate action sync preflight(`sync_corporate_action_events`) 누락 날짜 포함 기본값을 결정한다.
CLI 플래그로 기본값을 override할 수 있다.

Explicit date list:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --date 2026-03-07 --date 2026-03-08
```
DB discovery에서 이미 `research_ready=true`인 날짜까지 포함하려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09 --include-ready
```
research_ready가 true여도 corporate action sync preflight 미완료 날짜를 catchup에 다시 포함하려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09 --include-unsynced-corp-actions
```
반대로 설정 기본값이 true인 환경에서 해당 날짜를 제외하려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09 --exclude-unsynced-corp-actions
```
catchup 결과에서 preflight 요약 출력:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09 --verbose-preflight
```
`--verbose-preflight` 출력에는 aggregate 1줄과 날짜별 aggregate(성공/경고/누락/불필요) 요약이 포함된다.

Corporate action event sync (manual command, not part of core 10-step EOD pipeline):
```bash
PYTHONPATH=src python -m quant.cli.main eod sync-corporate-actions --start-date 2026-03-01 --end-date 2026-03-09
# or
make sync-corporate-actions START=2026-03-01 END=2026-03-09
```
This command uses `CorporateActionCatchupPipeline` (date-loop orchestration in `pipelines/`, not in `cli/`).

## Validation rules implemented
- duplicate key check (`trade_date`, `instrument_id`)
- non-negative OHLCV check
- OHLC relationship check
- minimum row count check
- basic coverage ratio check

## Config files and runtime usage
- `configs/app.yaml`: bootstrap defaults (`timezone`, `log_level`, `universe_name`, `feature_set_name`)
- `configs/app.yaml`: catchup discovery defaults (`catchup.include_unsynced_corporate_action_dates_default`)
- `configs/storage.yaml`: parquet root paths + partition metadata (path roots are used by `ParquetPaths`)
- `configs/providers.yaml`: provider runtime flags (`enabled`, `mode`, `base_url`, `timeout_sec`, `endpoints`, `TODO`) used by services when creating default providers
- DART provider는 추가로 `auth` (`api_key_param_name`, `api_key_header_name`)와 `response` (`status_key`, `success_values`, `list_key`) 설정을 읽어 live transport에 반영
- DART provider는 `request` (`start_date_param_name`, `end_date_param_name`, `date_format`) 설정으로 날짜 파라미터를 endpoint 계약에 맞춰 변환
- `configs/universe/*.yaml`: consumed by universe builder service
- `configs/feature_sets/*.yaml`: consumed by feature builder service

## Research-ready gate
A date becomes `research_ready=true` only when all are true:
- reference data ready (calendar + active instruments)
- raw daily price exists
- validation has no FAIL
- adjusted price exists
- feature snapshot exists

## Corporate action factor policy (current)
- Factor-affecting event types: `SPLIT`, `REVERSE_SPLIT`, `BONUS_ISSUE`, `RIGHTS_ISSUE`
- Non-affecting (stored but ignored for factor): `DIVIDEND_CASH`, `DIVIDEND_STOCK`
- Event types are canonicalized during ingestion before DB upsert.
- `configs/app.yaml`에서 `readiness.require_corporate_action_sync_preflight=true`이면 research_ready gate가 동일 일자의 corporate action sync run 이력을 요구한다.
- `build_price_adjustment_factor`의 `JobResult.metadata`에는 표준 preflight 키(`preflight_check_name`, `preflight_ready`, `preflight_status`, `preflight_target_date`)가 포함된다.
- `update_research_ready_status`의 `JobResult.metadata`에도 동일 preflight 키가 포함되어 gate 결과 추적을 통일했다.

## Notes
- This scaffold is intentionally conservative and explicit.
- Provider TODOs are documented in adapter modules and `configs/providers.yaml`.
- Pipeline tests include a 10-step single-date fixture integration test without external API/DB coupling.
