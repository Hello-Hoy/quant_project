from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["EodCatchupPipeline", "SingleDateEodPipeline"]


def __getattr__(name: str) -> Any:
    if name == "EodCatchupPipeline":
        return import_module("quant.pipelines.eod_catchup_pipeline").EodCatchupPipeline
    if name == "SingleDateEodPipeline":
        return import_module("quant.pipelines.single_date_eod_pipeline").SingleDateEodPipeline
    raise AttributeError(name)
