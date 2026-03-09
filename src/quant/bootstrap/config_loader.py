from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from quant.bootstrap.settings import settings


class ConfigLoader:
    def __init__(self, config_root: Path | None = None) -> None:
        root = config_root or settings.configs_root
        self.config_root = settings.resolve_path(root).resolve()

    @lru_cache(maxsize=64)
    def load_yaml(self, relative_path: str) -> dict[str, Any]:
        path = (self.config_root / relative_path).resolve()
        if self.config_root not in path.parents and path != self.config_root:
            raise ValueError(f"Config path is outside config root: {relative_path}")
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError(f"Config file must contain a mapping object: {path}")
        return data

    def load_app_config(self) -> dict[str, Any]:
        return self.load_yaml("app.yaml")

    def load_provider_config(self) -> dict[str, Any]:
        return self.load_yaml("providers.yaml")

    def load_storage_config(self) -> dict[str, Any]:
        return self.load_yaml("storage.yaml")

    def load_universe_config(self, universe_name: str) -> dict[str, Any]:
        return self.load_yaml(f"universe/{universe_name}.yaml")

    def load_feature_set_config(self, feature_set_name: str) -> dict[str, Any]:
        return self.load_yaml(f"feature_sets/{feature_set_name}.yaml")
