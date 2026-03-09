from __future__ import annotations

from quant.bootstrap.config_loader import ConfigLoader
from quant.bootstrap.logging_setup import setup_logging
from quant.bootstrap.settings import settings


class Container:
    def bootstrap(self) -> None:
        config_loader = ConfigLoader()
        app_cfg = config_loader.load_app_config()
        config_loader.load_provider_config()
        config_loader.load_storage_config()

        app_section = app_cfg.get("app", {})
        defaults_section = app_cfg.get("defaults", {})

        settings.app_name = str(app_section.get("name", settings.app_name))
        settings.env = str(app_section.get("env", settings.env))
        settings.timezone = str(app_section.get("timezone", settings.timezone))
        settings.log_level = str(app_section.get("log_level", settings.log_level))
        settings.default_universe_name = str(
            defaults_section.get("universe_name", settings.default_universe_name)
        )
        settings.default_feature_set_name = str(
            defaults_section.get("feature_set_name", settings.default_feature_set_name)
        )

        settings.ensure_directories()
        setup_logging()
