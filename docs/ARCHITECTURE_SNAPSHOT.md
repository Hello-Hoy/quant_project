# Quant Project Architecture Snapshot

이 문서는 다음 구현 단계에서 아키텍처 경계를 잊지 않기 위한 기준선이다.
현재 코드 상태를 기준으로 모듈 책임과 EOD 실행 흐름을 고정한다.

## 1. 고정 아키텍처 규칙 (변경 금지)
- `providers/` = 외부 API 어댑터만 담당
- `storage/` = 영속화 접근만 담당 (PostgreSQL / Parquet)
- `services/` = 비즈니스 로직만 담당
- `jobs/` = 실행 단위 래핑만 담당
- `pipelines/` = 오케스트레이션만 담당

## 2. 금지사항
- Job에 비즈니스 로직 직접 구현 금지
- Service에 오케스트레이션 순서 제어 금지
- Provider에서 Storage 직접 쓰기 금지
- 레이어 축소/병합 금지

## 3. 런타임 엔트리포인트
- CLI: `python -m quant.cli.main eod ...`
- Bootstrap: `Container.bootstrap()`
- Bootstrap 책임:
- `configs/app.yaml` 로드
- `configs/providers.yaml` 로드 검증
- `configs/storage.yaml` 로드 검증
- settings 기본값 오버라이드 (`app_name`, `env`, `timezone`, `log_level`, default universe/feature set)
- 디렉토리 생성 + 로깅 초기화

## 4. 공통 실행 계약
- 공통 Job 실행 클래스: `BaseJob`
- 공통 시그니처:
- `execute(session, target_date, force, run_mode, attempt_no) -> JobResult`
- 공통 lifecycle:
- run 시작 시 `ingestion_run`에 `RUNNING` 기록
- 비즈니스 실행 후 `SUCCESS/WARNING/PARTIAL/FAILED` 처리
- 상태에 따라 commit/rollback 및 run 종료 상태 기록
- `JobResult.metadata`에 `ingest_run_id`, `run_mode`, `attempt_no` 유지

## 5. EOD 파이프라인 표준 순서
`SingleDateEodPipeline`가 표준 실행 유닛이며, `EodCatchupPipeline`은 이를 재사용한다.

| Step | Job | Service | 주요 I/O |
|---|---|---|---|
| 1 | `sync_market_calendar` | `MarketCalendarService` | KIS calendar provider read → `market_calendar` upsert |
| 2 | `sync_instrument_master` | `InstrumentMasterService` | KRX instrument provider read → instrument 관련 DB upsert |
| 3 | `ingest_daily_price_raw` | `DailyPriceIngestionService` | KRX daily provider read + instrument DB lookup → raw parquet write |
| 4 | `ingest_index_daily` | `IndexIngestionService` | KRX index provider read → raw parquet write |
| 5 | `validate_daily_market_data` | `DailyMarketValidationService` | raw/index parquet read + instrument DB read → validation DB write |
| 6 | `build_price_adjustment_factor` | `FactorBuilderService` | raw parquet read + corporate_action_event DB read → price adjustment factor DB write |
| 7 | `build_daily_price_adjusted` | `AdjustedPriceBuilderService` | raw parquet read + factor DB read → adjusted parquet write |
| 8 | `build_universe_snapshot` | `UniverseBuilderService` | adjusted/raw parquet read + instrument DB read + universe config read → universe parquet write |
| 9 | `build_feature_snapshot` | `FeatureBuilderService` | adjusted/raw/index/universe parquet read + feature config read → feature parquet write |
| 10 | `update_research_ready_status` | `ResearchReadyService` | calendar/instrument/validation DB read + parquet availability check → readiness DB upsert |

## 6. 모듈별 책임 스냅샷

### `src/quant/providers`
- 목적: 외부 연동 입력 표준화
- 구현 상태:
- `krx/*`: instrument/daily/index placeholder adapter
- `kis/calendar_provider.py`: 주말 제외 기반 임시 캘린더 (placeholder)
- `dart/*`: corporate action placeholder
- `normalize.py`: provider payload key alias/형변환 공통 유틸 (필수 필드 누락 시 명시적 예외)
- 규칙: storage import/쓰기 금지
- `enabled/mode/base_url/timeout_sec/endpoints/TODO`는 `configs/providers.yaml`에서 읽혀 서비스 기본 provider 생성 시 반영
- DART는 `auth`/`response` 설정(`api_key_*`, `status_key`, `success_values`, `list_key`)을 live transport에 반영
- DART는 `request` 설정(`start_date_param_name`, `end_date_param_name`, `date_format`)으로 endpoint 파라미터 명세를 맞춘다.
- `mode=live`인데 endpoint 설정이 없으면 명시적 예외를 발생시켜 가짜 성공을 방지

### `src/quant/storage/db`
- 목적: SQLAlchemy 모델/세션/리포지토리
- 주요 모델:
- `market_calendar`, `instrument_master`, `instrument_listing_history`, `etf_metadata`
- `price_adjustment_factor`, `corporate_action_event`
- `ingestion_run`, `data_validation_result`, `research_ready_status`
- 규칙: 비즈니스 판단 로직 금지

### `src/quant/storage/parquet`
- 목적: 파일 경로 정책 + parquet I/O
- `ParquetPaths`가 경로 규칙 단일 소스
- raw/adjusted/universe/feature 경로 분리 유지
- 규칙: 비즈니스 판단 로직 금지

### `src/quant/services`
- 목적: 도메인 로직 집행
- 하위 도메인:
- `reference`: 캘린더/종목마스터 동기화 로직
- `ingestion`: raw/index 적재 로직
- `corporate_action`: corporate action event_type canonicalization/policy 로직
- `validation`: 데이터 품질 검증 로직
- `adjustment`: 조정계수/조정가 생성 로직
- `universe`: 투자 유니버스 구성 로직
- `feature`: 피처 스냅샷 계산 로직
- `readiness`: 연구 사용 가능 여부 판정 로직
- `ops`: ingestion run 상태 기록 서비스

### `src/quant/jobs`
- 목적: 서비스 실행 래퍼 + 표준 결과 반환
- 책임:
- target_date 유효성/필수성 체크
- 서비스 호출
- `JobResult` 상태/메타데이터 표준화
- 추가 유틸성 job: `sync_corporate_action_events` (수동 실행용, core 10-step에는 미포함)
- 규칙: 비즈니스 연산 직접 구현 금지

### `src/quant/pipelines`
- 목적: 실행 순서 제어
- `single_date_eod_pipeline.py`: 표준 10-step 순서
- `eod_catchup_pipeline.py`: 날짜 리스트 반복 실행, 실패 시 중단
- `corporate_action_catchup_pipeline.py`: corporate action 동기화 날짜 반복 실행, 실패 시 중단
- 규칙: 데이터 가공 로직 금지

### `src/quant/cli`
- 목적: 사용자 진입점
- `eod run-date`, `eod catchup` 제공
- `eod sync-corporate-actions` 제공 (DART corporate action event 수동 동기화)
- `eod preflight` 제공 (config/path/DB 사전 점검, 데이터 적재 전 단계)
- `catchup` 기본 날짜 탐색은 DB(`market_calendar`, `research_ready_status`) 기반이며, 실패 시 weekday fallback
- `configs/app.yaml`의 `catchup.include_unsynced_corporate_action_dates_default`가 preflight 누락 날짜 포함 기본값을 제어
- `catchup --include-unsynced-corp-actions` / `--exclude-unsynced-corp-actions`로 설정값 override 가능
- 규칙: business/storage/provider 세부 처리 금지

## 7. 데이터 저장 분리 원칙
- Raw 일봉: `data/raw/daily_price_raw/...`
- Index 일봉: `data/raw/index_daily/...`
- Adjusted 일봉: `data/curated/daily_price_adjusted/...`
- Universe snapshot: `data/curated/universe_snapshot/...`
- Feature snapshot: `data/features/feature_snapshot/...`
- 원칙: raw와 adjusted는 절대 혼합 저장하지 않는다.

## 8. 연구 사용 가능(research_ready) 게이트
`ResearchReadyService`는 아래를 모두 만족해야 `research_ready=true`를 기록한다.
- reference_ready (calendar + active instrument)
- raw_ready
- validated (FAIL 없음)
- adjusted_ready
- feature_ready

## 9. 현재 honest placeholder 목록
- KRX instrument/daily/index 실 API 연동 미구현
- KIS 휴장일 정확 캘린더 미구현 (현재 weekday 근사)
- DART corporate action 실 API 연동 미구현
- corporate action provider 연동 전에는 이벤트 부재 시 factor가 identity(1.0)로 남을 수 있음

## 10. 다음 단계 구현 시 체크리스트
- Provider 구현 시 Service/Job/Pipeline 시그니처 유지
- 새로운 로직은 반드시 Service 레이어에 배치
- 경로 규칙 변경 시 `ParquetPaths` 단일 지점만 수정
- EOD step 순서는 `SingleDateEodPipeline`에서만 관리
- 테스트: config/parquet/validation/pipeline aggregation + single-date 10-step fixture 회귀 유지
- PostgreSQL 통합 테스트는 `TEST_POSTGRES_URL` opt-in으로 유지 (기본 테스트와 분리)
`FactorBuilderService`는 preflight로 동일 target_date의 `sync_corporate_action_events` 실행 이력을 확인하며, 미확인 시 non-blocking warning 컨텍스트를 결과에 포함한다.
