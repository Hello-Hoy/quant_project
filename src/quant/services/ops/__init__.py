from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "CatchupDateDiscoveryResult",
    "CatchupDateDiscoveryService",
    "CatchupInclusionPolicy",
    "IngestionLoggingService",
    "PlatformPreflightService",
    "PreflightCheck",
    "PreflightReport",
]


def __getattr__(name: str) -> Any:
    if name in {"CatchupDateDiscoveryResult", "CatchupDateDiscoveryService"}:
        module = import_module("quant.services.ops.catchup_date_discovery_service")
        return getattr(module, name)
    if name == "CatchupInclusionPolicy":
        module = import_module("quant.services.ops.catchup_inclusion_policy")
        return getattr(module, name)
    if name == "IngestionLoggingService":
        module = import_module("quant.services.ops.ingestion_logging_service")
        return getattr(module, name)
    if name in {"PlatformPreflightService", "PreflightCheck", "PreflightReport"}:
        module = import_module("quant.services.ops.platform_preflight_service")
        return getattr(module, name)
    raise AttributeError(name)
