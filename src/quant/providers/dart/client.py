from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class DartRequest:
    endpoint: str
    params: dict[str, Any]


class DartClient:
    """DART transport placeholder.

    TODO:
    - integrate official DART OpenAPI authentication
    - normalize corporate-action relevant disclosures
    """

    def call(self, request: DartRequest) -> list[dict[str, Any]]:
        return []
