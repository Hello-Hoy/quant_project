from __future__ import annotations

import json
from pathlib import Path

from quant.providers.dart.corporate_action_provider import DartCorporateActionProvider


class _StubClient:
    def __init__(self, payload: list[dict]) -> None:
        self.payload = payload

    def call(self, _request: object) -> list[dict]:
        return self.payload


def _fixture_path(name: str) -> Path:
    return Path(__file__).parent / "fixtures" / "providers" / "dart" / name


def test_dart_corporate_action_payload_contract_alias_fixture() -> None:
    payload = json.loads(
        _fixture_path("corporate_actions_alias_payload.json").read_text(encoding="utf-8")
    )
    provider = DartCorporateActionProvider(mode="live", client=_StubClient(payload))

    rows = provider.fetch_corporate_actions(start_date="2026-03-01", end_date="2026-03-31")

    assert len(rows) == 2
    assert rows[0].symbol == "005930"
    assert rows[0].event_type == "cash_dividend"
    assert rows[0].cash_value == 123.45
    assert rows[1].symbol == "000660"
    assert rows[1].event_type == "stock_split"
    assert rows[1].ratio_value == 2.0


def test_dart_corporate_action_payload_contract_dart_like_fixture() -> None:
    payload = json.loads(
        _fixture_path("corporate_actions_dart_like_list_payload.json").read_text(encoding="utf-8")
    )
    provider = DartCorporateActionProvider(mode="live", client=_StubClient(payload))

    rows = provider.fetch_corporate_actions(start_date="2026-03-01", end_date="2026-03-31")

    assert len(rows) == 2
    assert rows[0].symbol == "005930"
    assert rows[0].event_type == "cash_dividend"
    assert rows[0].announce_date is not None
    assert rows[0].cash_value == 123.45
    assert rows[1].symbol == "000660"
    assert rows[1].event_type == "stock_split"
    assert rows[1].ratio_value == 2.0
