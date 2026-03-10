from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


@dataclass(slots=True)
class ProviderRequest:
    endpoint: str
    params: dict[str, Any]


@dataclass(slots=True)
class ProviderResponse:
    status_code: int
    payload: dict[str, Any] | list[dict[str, Any]]


class ProviderMode(StrEnum):
    PLACEHOLDER = "placeholder"
    LIVE = "live"
    DISABLED = "disabled"


class ProviderAdapter:
    """Base adapter contract for external market-data providers."""

    provider_name: str = "UNKNOWN"

    def __init__(
        self,
        enabled: bool = True,
        mode: str | ProviderMode = ProviderMode.PLACEHOLDER,
        note: str | None = None,
    ) -> None:
        self.enabled = enabled
        self.mode = ProviderMode(mode)
        self.note = note

    @property
    def is_placeholder(self) -> bool:
        return self.mode == ProviderMode.PLACEHOLDER

    def unavailable_message(self, capability: str, context: str | None = None) -> str:
        state = (
            "disabled by config"
            if not self.enabled or self.mode == ProviderMode.DISABLED
            else f"provider mode={self.mode.value}"
        )
        suffix = f" ({context})" if context else ""
        detail = f" - {self.note}" if self.note else ""
        return f"{self.provider_name} unavailable for {capability}{suffix}: {state}{detail}"

    def healthcheck(self) -> bool:
        # Placeholder until real endpoint checks are integrated.
        return self.enabled
