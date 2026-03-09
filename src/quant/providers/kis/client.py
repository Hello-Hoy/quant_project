from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class KisRequest:
    path: str
    params: dict[str, Any]


class KisClient:
    """KIS transport placeholder.

    TODO:
    - add OAuth token management
    - add production/sandbox base URL switch
    - add resilient request/retry handling
    """

    def call(self, request: KisRequest) -> dict[str, Any]:
        # Intentionally returns empty payload in scaffold stage.
        return {}
