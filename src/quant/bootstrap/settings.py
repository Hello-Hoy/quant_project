from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # .env loading is optional in the scaffold stage.
    pass


@dataclass(slots=True)
class Settings:
    app_name: str = field(default_factory=lambda: os.getenv("APP_NAME", "quant-project"))
    env: str = field(default_factory=lambda: os.getenv("ENV", "dev"))
    timezone: str = field(default_factory=lambda: os.getenv("TIMEZONE", "Asia/Seoul"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    project_root: Path = field(default_factory=lambda: Path(os.getenv("PROJECT_ROOT", Path.cwd())).resolve())
    data_root: Path = field(default_factory=lambda: Path(os.getenv("DATA_ROOT", "data")))
    raw_data_root: Path = field(default_factory=lambda: Path(os.getenv("RAW_DATA_ROOT", "data/raw")))
    curated_data_root: Path = field(default_factory=lambda: Path(os.getenv("CURATED_DATA_ROOT", "data/curated")))
    feature_data_root: Path = field(default_factory=lambda: Path(os.getenv("FEATURE_DATA_ROOT", "data/features")))
    log_root: Path = field(default_factory=lambda: Path(os.getenv("LOG_ROOT", "logs")))
    configs_root: Path = field(default_factory=lambda: Path(os.getenv("CONFIGS_ROOT", "configs")))

    postgres_url: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/quant",
        )
    )
    duckdb_path: str = field(default_factory=lambda: os.getenv("DUCKDB_PATH", "data/tmp/quant.duckdb"))

    krx_api_key: str | None = field(default_factory=lambda: os.getenv("KRX_API_KEY") or None)
    kis_app_key: str | None = field(default_factory=lambda: os.getenv("KIS_APP_KEY") or None)
    kis_app_secret: str | None = field(default_factory=lambda: os.getenv("KIS_APP_SECRET") or None)
    dart_api_key: str | None = field(default_factory=lambda: os.getenv("DART_API_KEY") or None)

    default_universe_name: str = field(
        default_factory=lambda: os.getenv("DEFAULT_UNIVERSE_NAME", "core_equity_etf")
    )
    default_feature_set_name: str = field(default_factory=lambda: os.getenv("DEFAULT_FEATURE_SET_NAME", "core_v1"))

    def resolve_path(self, value: Path | str) -> Path:
        path = value if isinstance(value, Path) else Path(value)
        return path if path.is_absolute() else (self.project_root / path).resolve()

    def ensure_directories(self) -> None:
        for path in [
            self.resolve_path(self.data_root),
            self.resolve_path(self.raw_data_root),
            self.resolve_path(self.curated_data_root),
            self.resolve_path(self.feature_data_root),
            self.resolve_path(self.log_root),
            self.resolve_path(Path(self.duckdb_path)).parent,
            self.resolve_path(self.configs_root),
        ]:
            path.mkdir(parents=True, exist_ok=True)


settings = Settings()
