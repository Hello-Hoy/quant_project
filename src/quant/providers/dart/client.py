from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urljoin
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from quant.core.exceptions import ProviderNotImplementedError


@dataclass(slots=True)
class DartRequest:
    api_name: str
    params: dict[str, Any]


class DartClient:
    """DART transport scaffold.

    Authentication and disclosure-specific payload mapping are handled at
    provider adapter level.
    """

    def __init__(
        self,
        base_url: str | None = None,
        endpoints: dict[str, str] | None = None,
        timeout_sec: int = 10,
        api_key: str | None = None,
        api_key_param_name: str | None = None,
        api_key_header_name: str | None = None,
        response_list_key: str = "list",
        response_status_key: str = "status",
        response_success_values: list[str] | None = None,
    ) -> None:
        self.base_url = base_url
        self.endpoints = endpoints or {}
        self.timeout_sec = timeout_sec
        self.api_key = api_key
        self.api_key_param_name = api_key_param_name
        self.api_key_header_name = api_key_header_name
        self.response_list_key = response_list_key
        self.response_status_key = response_status_key
        self.response_success_values = (
            response_success_values[:] if response_success_values is not None else ["000"]
        )

    def _build_url(self, request: DartRequest) -> str:
        endpoint_path = self.endpoints.get(request.api_name)
        if not self.base_url or not endpoint_path:
            raise ProviderNotImplementedError(
                "DART live endpoint is not configured for "
                f"api_name={request.api_name}. Set providers.yaml base_url/endpoints first."
            )
        if self.api_key_param_name and not self.api_key:
            raise ProviderNotImplementedError(
                "DART API key is required for query-param auth but is missing. "
                "Set DART_API_KEY in environment."
            )

        base = self.base_url if self.base_url.endswith("/") else f"{self.base_url}/"
        path = endpoint_path.lstrip("/")
        url = urljoin(base, path)
        query_params = dict(request.params)
        if self.api_key_param_name and self.api_key:
            query_params[self.api_key_param_name] = self.api_key
        if query_params:
            return f"{url}?{urlencode(query_params)}"
        return url

    def _build_headers(self) -> dict[str, str]:
        if self.api_key_header_name and not self.api_key:
            raise ProviderNotImplementedError(
                "DART API key is required for header auth but is missing. "
                "Set DART_API_KEY in environment."
            )
        headers: dict[str, str] = {}
        if self.api_key_header_name and self.api_key:
            headers[self.api_key_header_name] = self.api_key
        return headers

    def _extract_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]

        if not isinstance(payload, dict):
            raise ProviderNotImplementedError(
                "DART response format is unsupported. Expected JSON object or list."
            )

        status_value = payload.get(self.response_status_key)
        if status_value is not None and self.response_success_values:
            status_text = str(status_value)
            allowed = {str(item) for item in self.response_success_values}
            if status_text not in allowed:
                message = payload.get("message") or payload.get("msg") or "unknown error"
                raise ProviderNotImplementedError(
                    f"DART response indicates failure: {self.response_status_key}={status_text}, message={message}"
                )

        list_payload = payload.get(self.response_list_key)
        if isinstance(list_payload, list):
            return [row for row in list_payload if isinstance(row, dict)]

        data_payload = payload.get("data")
        if isinstance(data_payload, list):
            return [row for row in data_payload if isinstance(row, dict)]

        raise ProviderNotImplementedError(
            "DART response format is unsupported. Expected "
            f"{{'{self.response_list_key}': list[dict]}} or {{'data': list[dict]}}."
        )

    def call(self, request: DartRequest) -> list[dict[str, Any]]:
        url = self._build_url(request)
        headers = self._build_headers()
        http_request = Request(url=url, headers=headers)
        try:
            with urlopen(http_request, timeout=self.timeout_sec) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise ProviderNotImplementedError(
                f"DART request failed with HTTP {exc.code} for api_name={request.api_name}"
            ) from exc
        except URLError as exc:
            raise ProviderNotImplementedError(
                f"DART request failed for api_name={request.api_name}: {exc.reason}"
            ) from exc
        return self._extract_rows(payload)
