# Corporate Action to Factor Runbook

이 문서는 corporate action event를 적재하고 adjustment factor를 생성하는 최소 운영 시퀀스를 정리한다.

## 1) 사전 준비
- PostgreSQL 기동
- DART live 모드 사용 시 `DART_API_KEY` 환경 변수 설정
- 스키마 초기화:
```bash
PYTHONPATH=src python scripts/init_db.py
```

## 2) 종목 마스터 동기화
corporate action symbol 매핑을 위해 instrument master가 먼저 준비되어야 한다.

```bash
PYTHONPATH=src python -m quant.cli.main eod run-date --date 2026-03-09
```

또는 최소한 아래 두 단계가 완료되어야 한다.
- `sync_market_calendar`
- `sync_instrument_master`

## 3) corporate action event 동기화
수동/보조 파이프라인 명령:

```bash
PYTHONPATH=src python -m quant.cli.main eod sync-corporate-actions --start-date 2026-03-01 --end-date 2026-03-09
```

Makefile:
```bash
make sync-corporate-actions START=2026-03-01 END=2026-03-09
```

## 4) raw price 적재 확인
factor 빌더는 raw daily parquet 기준으로 instrument universe를 결정한다.
- `data/raw/daily_price_raw/...`에 대상 일자 데이터가 있어야 한다.

## 5) factor 빌드 실행
EOD 표준 파이프라인 step 6:
- `build_price_adjustment_factor`

전체 단일일 실행:
```bash
PYTHONPATH=src python -m quant.cli.main eod run-date --date 2026-03-09
```

## 6) 결과 확인 포인트
- DB `corporate_action_event`에 event upsert 반영
- event_type은 canonical value로 저장 (`SPLIT`, `REVERSE_SPLIT`, `BONUS_ISSUE`, `RIGHTS_ISSUE`, `DIVIDEND_CASH` 등)
- DB `price_adjustment_factor`에 `factor_version=v1_corporate_action` 반영
- 이벤트 미적용 종목은 factor=1.0 유지
- 동일 일자 corporate action sync 실행 이력이 없으면 factor job은 `WARNING`으로 완료됨 (non-blocking preflight)
- factor job 결과 metadata에서 preflight 표준 키 확인:
  - `preflight_check_name`
  - `preflight_ready`
  - `preflight_status`
  - `preflight_target_date`
- 동일 키가 `update_research_ready_status` job metadata에도 포함되어, 최종 gate 판정과 preflight 관찰 지점을 통일한다.

현재 factor 반영 정책:
- 반영: `SPLIT`, `REVERSE_SPLIT`, `BONUS_ISSUE`, `RIGHTS_ISSUE`
- 미반영(저장만): `DIVIDEND_CASH`, `DIVIDEND_STOCK`

## 7) 실패 시 점검 순서
1. PostgreSQL 연결 가능 여부 (`POSTGRES_URL`)
2. `providers.yaml`에서 `dart` provider `enabled/mode/base_url/endpoints` 설정
2.1. `dart.auth` (`api_key_param_name` or `api_key_header_name`)와 `DART_API_KEY` 설정 일치 여부
2.2. `dart.request` (`start_date_param_name`, `end_date_param_name`, `date_format`)가 endpoint 계약과 일치하는지 확인
3. symbol 매핑 실패 여부 (`sync_corporate_action_events` 결과 message/metadata)
4. raw parquet 존재 여부 (`daily_price_raw` 대상 partition)
5. `configs/app.yaml`의 `readiness.require_corporate_action_sync_preflight` 값 확인

## 8) catchup 재실행 팁
기본 포함 정책은 `configs/app.yaml`의 아래 키로 결정된다.
- `catchup.include_unsynced_corporate_action_dates_default`

이미 `research_ready=true`로 남아 있더라도 corporate action sync preflight가 누락된 날짜를 재실행하려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09 --include-unsynced-corp-actions
```
설정 기본값이 true인 환경에서 해당 날짜를 제외하려면:
```bash
PYTHONPATH=src python -m quant.cli.main eod catchup --start-date 2026-03-01 --end-date 2026-03-09 --exclude-unsynced-corp-actions
```
운영 점검 시 `--verbose-preflight`를 함께 사용하면 aggregate + 날짜별 preflight 상태 요약을 바로 확인할 수 있다.
