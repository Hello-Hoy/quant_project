from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ProviderRequest:
    endpoint: str
    params: dict[str, Any]


@dataclass(slots=True)
class ProviderResponse:
    status_code: int
    payload: dict[str, Any] | list[dict[str, Any]]


class ProviderAdapter:
    """Base adapter contract for external market-data providers."""

    provider_name: str = "UNKNOWN"

    def healthcheck(self) -> bool:
        # Placeholder until real endpoint checks are integrated.
        return True
