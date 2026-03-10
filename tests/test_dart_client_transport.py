from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from quant.core.exceptions import ProviderNotImplementedError
import quant.providers.dart.client as dart_client_module
from quant.providers.dart.client import DartClient, DartRequest


class _FakeHttpResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _ = exc_type
        _ = exc
        _ = tb
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def _fixture_payload(name: str) -> object:
    path = Path(__file__).parent / "fixtures" / "providers" / "dart" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_dart_client_requires_api_key_when_query_auth_configured() -> None:
    client = DartClient(
        base_url="https://example.com/api",
        endpoints={"corporate_actions": "/list.json"},
        api_key=None,
        api_key_param_name="crtfc_key",
    )
    with pytest.raises(ProviderNotImplementedError):
        client.call(DartRequest(api_name="corporate_actions", params={"bgn_de": "20260301"}))


def test_dart_client_injects_query_api_key_and_parses_status_list(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_urlopen(request, timeout: int):  # noqa: ANN001
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return _FakeHttpResponse(
            {
                "status": "000",
                "message": "OK",
                "list": [{"stock_code": "005930", "action_type": "stock_split"}],
            }
        )

    monkeypatch.setattr(dart_client_module, "urlopen", _fake_urlopen)

    client = DartClient(
        base_url="https://example.com/api",
        endpoints={"corporate_actions": "/list.json"},
        api_key="DART-KEY",
        api_key_param_name="crtfc_key",
        timeout_sec=7,
    )
    rows = client.call(DartRequest(api_name="corporate_actions", params={"bgn_de": "20260301"}))

    assert len(rows) == 1
    assert rows[0]["stock_code"] == "005930"
    parsed = urlparse(str(captured["url"]))
    query = parse_qs(parsed.query)
    assert query["crtfc_key"] == ["DART-KEY"]
    assert query["bgn_de"] == ["20260301"]
    assert captured["timeout"] == 7


def test_dart_client_injects_header_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_urlopen(request, timeout: int):  # noqa: ANN001
        captured["header_items"] = dict(request.header_items())
        captured["timeout"] = timeout
        return _FakeHttpResponse({"status": "000", "list": []})

    monkeypatch.setattr(dart_client_module, "urlopen", _fake_urlopen)

    client = DartClient(
        base_url="https://example.com/api",
        endpoints={"corporate_actions": "/list.json"},
        api_key="DART-KEY",
        api_key_header_name="X-DART-KEY",
    )
    _ = client.call(DartRequest(api_name="corporate_actions", params={}))

    headers = captured["header_items"]
    assert isinstance(headers, dict)
    assert headers["X-dart-key"] == "DART-KEY"
    assert captured["timeout"] == 10


def test_dart_client_raises_on_failure_status(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_urlopen(request, timeout: int):  # noqa: ANN001
        _ = request
        _ = timeout
        return _FakeHttpResponse({"status": "013", "message": "No data"})

    monkeypatch.setattr(dart_client_module, "urlopen", _fake_urlopen)

    client = DartClient(
        base_url="https://example.com/api",
        endpoints={"corporate_actions": "/list.json"},
    )
    with pytest.raises(ProviderNotImplementedError):
        client.call(DartRequest(api_name="corporate_actions", params={}))


def test_dart_client_parses_envelope_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _fixture_payload("corporate_actions_envelope_payload.json")

    def _fake_urlopen(request, timeout: int):  # noqa: ANN001
        _ = request
        _ = timeout
        return _FakeHttpResponse(payload)

    monkeypatch.setattr(dart_client_module, "urlopen", _fake_urlopen)

    client = DartClient(
        base_url="https://example.com/api",
        endpoints={"corporate_actions": "/list.json"},
    )
    rows = client.call(DartRequest(api_name="corporate_actions", params={}))
    assert len(rows) == 2
    assert rows[0]["stock_code"] == "005930"
    assert rows[1]["stock_code"] == "000660"


def test_dart_client_supports_custom_response_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_urlopen(request, timeout: int):  # noqa: ANN001
        _ = request
        _ = timeout
        return _FakeHttpResponse({"rsp_cd": "OK", "items": [{"id": 1}]})

    monkeypatch.setattr(dart_client_module, "urlopen", _fake_urlopen)

    client = DartClient(
        base_url="https://example.com/api",
        endpoints={"corporate_actions": "/list.json"},
        response_status_key="rsp_cd",
        response_success_values=["OK"],
        response_list_key="items",
    )
    rows = client.call(DartRequest(api_name="corporate_actions", params={}))
    assert rows == [{"id": 1}]
