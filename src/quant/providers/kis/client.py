from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urljoin
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from quant.core.exceptions import ProviderNotImplementedError


@dataclass(slots=True)
class KisRequest:
    api_name: str
    params: dict[str, Any]


class KisClient:
    """KIS transport scaffold.

    OAuth/signing and endpoint-specific mapping are intentionally left for
    provider-level implementation.
    """

    def __init__(
        self,
        base_url: str | None = None,
        endpoints: dict[str, str] | None = None,
        timeout_sec: int = 10,
    ) -> None:
        self.base_url = base_url
        self.endpoints = endpoints or {}
        self.timeout_sec = timeout_sec

    def _build_url(self, request: KisRequest) -> str:
        endpoint_path = self.endpoints.get(request.api_name)
        if not self.base_url or not endpoint_path:
            raise ProviderNotImplementedError(
                "KIS live endpoint is not configured for "
                f"api_name={request.api_name}. Set providers.yaml base_url/endpoints first."
            )
        base = self.base_url if self.base_url.endswith("/") else f"{self.base_url}/"
        path = endpoint_path.lstrip("/")
        url = urljoin(base, path)
        if request.params:
            return f"{url}?{urlencode(request.params)}"
        return url

    def call(self, request: KisRequest) -> list[dict[str, Any]]:
        url = self._build_url(request)
        try:
            with urlopen(url, timeout=self.timeout_sec) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise ProviderNotImplementedError(
                f"KIS request failed with HTTP {exc.code} for api_name={request.api_name}"
            ) from exc
        except URLError as exc:
            raise ProviderNotImplementedError(
                f"KIS request failed for api_name={request.api_name}: {exc.reason}"
            ) from exc

        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)]
        raise ProviderNotImplementedError(
            "KIS response format is unsupported. Expected list[dict] or {'data': list[dict]}."
        )
