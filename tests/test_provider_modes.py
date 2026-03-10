from __future__ import annotations

from datetime import date, time

from quant.core.exceptions import ProviderNotImplementedError
from quant.providers.dart.corporate_action_provider import DartCorporateActionProvider
from quant.providers.kis.calendar_provider import KisCalendarProvider
from quant.providers.krx.daily_price_provider import KrxDailyPriceProvider
from quant.providers.krx.index_provider import KrxIndexProvider
from quant.providers.krx.instrument_provider import KrxInstrumentProvider



def test_krx_providers_return_empty_in_placeholder_mode() -> None:
    assert KrxInstrumentProvider(mode="placeholder").fetch_instruments("2026-03-09") == []
    assert KrxDailyPriceProvider(mode="placeholder").fetch_daily_prices("2026-03-09") == []
    assert KrxIndexProvider(mode="placeholder").fetch_index_daily("2026-03-09") == []



def test_krx_live_mode_without_endpoint_is_explicit_failure() -> None:
    provider = KrxInstrumentProvider(mode="live", base_url=None, endpoints={})
    try:
        provider.fetch_instruments("2026-03-09")
    except ProviderNotImplementedError:
        return
    raise AssertionError("Expected ProviderNotImplementedError in live mode without endpoint config")



def test_kis_placeholder_calendar_still_returns_weekday_rows() -> None:
    provider = KisCalendarProvider(mode="placeholder")
    rows = provider.fetch_calendar_range(start_date=date(2026, 3, 6), end_date=date(2026, 3, 9))
    assert len(rows) == 4
    assert any(row.is_open for row in rows)



def test_kis_live_mode_without_endpoint_is_explicit_failure() -> None:
    provider = KisCalendarProvider(mode="live", base_url=None, endpoints={})
    try:
        provider.fetch_calendar_range(start_date=date(2026, 3, 1), end_date=date(2026, 3, 2))
    except ProviderNotImplementedError:
        return
    raise AssertionError("Expected ProviderNotImplementedError in live mode without endpoint config")


def test_dart_live_mode_without_endpoint_is_explicit_failure() -> None:
    provider = DartCorporateActionProvider(mode="live", base_url=None, endpoints={})
    try:
        provider.fetch_corporate_actions(start_date="2026-03-01", end_date="2026-03-02")
    except ProviderNotImplementedError:
        return
    raise AssertionError("Expected ProviderNotImplementedError in live mode without endpoint config")


class _StubClient:
    def __init__(self, payload: list[dict]) -> None:
        self.payload = payload

    def call(self, _request: object) -> list[dict]:
        return self.payload


class _CaptureClient:
    def __init__(self, payload: list[dict]) -> None:
        self.payload = payload
        self.last_request: object | None = None

    def call(self, request: object) -> list[dict]:
        self.last_request = request
        return self.payload


def test_krx_daily_live_mode_normalizes_alias_payload() -> None:
    provider = KrxDailyPriceProvider(
        mode="live",
        client=_StubClient(
            [
                {
                    "trd_dd": "20260309",
                    "isu_cd": "005930",
                    "mrkt": "KOSPI",
                    "open_price": "70000",
                    "high_price": "71000",
                    "low_price": "69500",
                    "close_price": "70500",
                    "vol": "1000",
                }
            ]
        ),
    )
    rows = provider.fetch_daily_prices("2026-03-09")
    assert len(rows) == 1
    assert rows[0].trade_date == date(2026, 3, 9)
    assert rows[0].symbol == "005930"
    assert rows[0].market_code == "KOSPI"
    assert rows[0].close == 70500.0
    assert rows[0].volume == 1000


def test_krx_index_live_mode_normalizes_alias_payload() -> None:
    provider = KrxIndexProvider(
        mode="live",
        client=_StubClient(
            [
                {
                    "trd_dd": "2026-03-09",
                    "idx_cd": "1001",
                    "idx_nm": "KOSPI",
                    "idx_family": "KOSPI",
                    "open_price": "2700.5",
                    "high_price": "2710.0",
                    "low_price": "2680.2",
                    "close_price": "2699.9",
                    "vol": "12",
                }
            ]
        ),
    )
    rows = provider.fetch_index_daily("2026-03-09")
    assert len(rows) == 1
    assert rows[0].trade_date == date(2026, 3, 9)
    assert rows[0].index_code == "1001"
    assert rows[0].index_name == "KOSPI"
    assert rows[0].volume == 12


def test_kis_live_mode_normalizes_calendar_alias_payload() -> None:
    provider = KisCalendarProvider(
        mode="live",
        client=_StubClient(
            [
                {
                    "date": "2026-03-09",
                    "open_flag": "true",
                    "market": "KRX",
                    "market_open": "09:00",
                    "market_close": "15:30",
                    "half_day": "false",
                    "prev_open_date": "2026-03-06",
                }
            ]
        ),
    )
    rows = provider.fetch_calendar_range(start_date=date(2026, 3, 9), end_date=date(2026, 3, 9))
    assert len(rows) == 1
    assert rows[0].trade_date == date(2026, 3, 9)
    assert rows[0].is_open is True
    assert rows[0].open_time == time(9, 0)
    assert rows[0].close_time == time(15, 30)
    assert rows[0].prev_trade_date == date(2026, 3, 6)
    assert rows[0].next_trade_date is None


def test_dart_live_mode_normalizes_alias_payload() -> None:
    provider = DartCorporateActionProvider(
        mode="live",
        client=_StubClient(
            [
                {
                    "stock_code": "005930",
                    "report_nm": "cash_dividend",
                    "rcept_dt": "20260301",
                    "bas_dt": "2026-03-10",
                    "stlm_dt": "2026-04-01",
                    "cash_amt": "123.45",
                }
            ]
        ),
    )
    rows = provider.fetch_corporate_actions(start_date="2026-03-01", end_date="2026-03-31")
    assert len(rows) == 1
    assert rows[0].symbol == "005930"
    assert rows[0].event_type == "cash_dividend"
    assert rows[0].announce_date == date(2026, 3, 1)
    assert rows[0].ex_date == date(2026, 3, 10)
    assert rows[0].effective_date == date(2026, 4, 1)
    assert rows[0].cash_value == 123.45


def test_dart_live_mode_uses_configured_request_param_names_and_date_format() -> None:
    client = _CaptureClient(payload=[])
    provider = DartCorporateActionProvider(
        mode="live",
        client=client,
        start_date_param_name="bgn_de",
        end_date_param_name="end_de",
        request_date_format="YYYYMMDD",
    )
    rows = provider.fetch_corporate_actions(start_date="2026-03-01", end_date="2026-03-31")
    assert rows == []
    assert client.last_request is not None
    params = getattr(client.last_request, "params")
    assert params == {"bgn_de": "20260301", "end_de": "20260331"}
