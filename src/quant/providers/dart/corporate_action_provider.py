from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

from quant.providers.base import ProviderAdapter, ProviderMode
from quant.providers.dart.client import DartClient, DartRequest
from quant.providers.normalize import parse_date, parse_float, parse_str, pick_value


@dataclass(slots=True)
class CorporateActionDisclosureRow:
    symbol: str
    event_type: str
    announce_date: date | None
    ex_date: date | None
    effective_date: date | None
    ratio_value: float | None = None
    cash_value: float | None = None
    raw_payload: dict | None = None


class DartCorporateActionProvider(ProviderAdapter):
    provider_name = "DART"

    def __init__(
        self,
        enabled: bool = True,
        mode: str | ProviderMode = ProviderMode.PLACEHOLDER,
        note: str | None = None,
        base_url: str | None = None,
        timeout_sec: int = 10,
        endpoints: dict[str, str] | None = None,
        api_key: str | None = None,
        api_key_param_name: str | None = None,
        api_key_header_name: str | None = None,
        response_list_key: str = "list",
        response_status_key: str = "status",
        response_success_values: list[str] | None = None,
        start_date_param_name: str = "start_date",
        end_date_param_name: str = "end_date",
        request_date_format: str = "ISO",
        client: DartClient | None = None,
    ) -> None:
        super().__init__(enabled=enabled, mode=mode, note=note)
        self.start_date_param_name = start_date_param_name
        self.end_date_param_name = end_date_param_name
        self.request_date_format = request_date_format.upper()
        self.client = client or DartClient(
            base_url=base_url,
            endpoints=endpoints,
            timeout_sec=timeout_sec,
            api_key=api_key,
            api_key_param_name=api_key_param_name,
            api_key_header_name=api_key_header_name,
            response_list_key=response_list_key,
            response_status_key=response_status_key,
            response_success_values=response_success_values,
        )

    def _format_request_date(self, value: str) -> str:
        text = value.strip()
        if self.request_date_format == "YYYYMMDD":
            if len(text) == 8 and text.isdigit():
                return text
            try:
                return datetime.fromisoformat(text).strftime("%Y%m%d")
            except ValueError:
                return text.replace("-", "")
        return text

    def _normalize_row(self, item: dict) -> CorporateActionDisclosureRow:
        symbol = parse_str(
            pick_value(item, ["symbol", "ticker", "stock_code", "stck_cd"], "symbol"),
            "symbol",
        )
        event_type = parse_str(
            pick_value(
                item,
                ["event_type", "action_type", "report_nm", "report_name"],
                "event_type",
            ),
            "event_type",
        )

        return CorporateActionDisclosureRow(
            symbol=cast(str, symbol),
            event_type=cast(str, event_type),
            announce_date=parse_date(
                pick_value(
                    item,
                    ["announce_date", "disclosure_date", "rcept_dt"],
                    "announce_date",
                    required=False,
                ),
                "announce_date",
                required=False,
            ),
            ex_date=parse_date(
                pick_value(
                    item,
                    ["ex_date", "ex_dividend_date", "bas_dt"],
                    "ex_date",
                    required=False,
                ),
                "ex_date",
                required=False,
            ),
            effective_date=parse_date(
                pick_value(
                    item,
                    ["effective_date", "payment_date", "stlm_dt"],
                    "effective_date",
                    required=False,
                ),
                "effective_date",
                required=False,
            ),
            ratio_value=parse_float(
                pick_value(
                    item,
                    ["ratio_value", "ratio", "stock_ratio", "split_ratio"],
                    "ratio_value",
                    required=False,
                ),
                "ratio_value",
                required=False,
            ),
            cash_value=parse_float(
                pick_value(
                    item,
                    ["cash_value", "amount", "cash_amt", "div_amt"],
                    "cash_value",
                    required=False,
                ),
                "cash_value",
                required=False,
            ),
            raw_payload=item if isinstance(item, dict) else None,
        )

    def fetch_corporate_actions(self, start_date: str, end_date: str) -> list[CorporateActionDisclosureRow]:
        # TODO(provider): Wire DART disclosures to normalized corporate action rows.
        if not self.enabled or self.mode == ProviderMode.DISABLED:
            return []
        if self.mode == ProviderMode.PLACEHOLDER:
            return []

        params = {
            self.start_date_param_name: self._format_request_date(start_date),
            self.end_date_param_name: self._format_request_date(end_date),
        }
        payload = self.client.call(
            DartRequest(api_name="corporate_actions", params=params)
        )
        return [self._normalize_row(item) for item in payload]
