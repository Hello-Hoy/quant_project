from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class KrxRequest:
    api_name: str
    params: dict[str, Any]


class KrxClient:
    """KRX transport placeholder.

    TODO:
    - wire authenticated HTTP transport for KRX data source(s)
    - add response decoding + retry policy
    - enforce request throttling limits
    """

    def call(self, request: KrxRequest) -> list[dict[str, Any]]:
        # Intentionally returns empty until endpoint contract is implemented.
        return []
