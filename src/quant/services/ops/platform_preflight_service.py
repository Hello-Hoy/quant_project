from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from quant.bootstrap.config_loader import ConfigLoader
from quant.bootstrap.settings import Settings, settings
from quant.core.enums import RunStatus
from quant.providers.base import ProviderMode
from quant.storage.db.base import Base
import quant.storage.db.models as _models  # noqa: F401
from quant.storage.db.session import engine


@dataclass
class PreflightCheck:
    name: str
    status: RunStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class PreflightReport:
    status: RunStatus
    checks: list[PreflightCheck]
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "message": self.message,
            "checks": [check.to_dict() for check in self.checks],
        }


class PlatformPreflightService:
    def __init__(
        self,
        config_loader: ConfigLoader | None = None,
        db_engine: Engine | None = None,
        settings_obj: Settings | None = None,
    ) -> None:
        self.config_loader = config_loader or ConfigLoader()
        self.db_engine = db_engine or engine
        self.settings = settings_obj or settings

    def run(
        self,
        require_db_schema: bool = True,
        check_database: bool = True,
    ) -> PreflightReport:
        checks = [self._check_configs(), self._check_storage_paths()]
        if check_database:
            checks.append(self._check_db(require_db_schema=require_db_schema))
        else:
            checks.append(
                PreflightCheck(
                    name="database",
                    status=RunStatus.SUCCESS,
                    message="Database checks skipped by option",
                )
            )
        has_failed = any(check.status == RunStatus.FAILED for check in checks)
        has_warning = any(check.status == RunStatus.WARNING for check in checks)
        final_status = (
            RunStatus.FAILED
            if has_failed
            else (RunStatus.WARNING if has_warning else RunStatus.SUCCESS)
        )
        message = (
            "Preflight checks failed"
            if has_failed
            else ("Preflight checks completed with warnings" if has_warning else "Preflight checks passed")
        )
        return PreflightReport(status=final_status, checks=checks, message=message)

    def _check_configs(self) -> PreflightCheck:
        try:
            app_cfg = self.config_loader.load_app_config()
            provider_cfg = self.config_loader.load_provider_config()
            storage_cfg = self.config_loader.load_storage_config()
        except Exception as exc:
            return PreflightCheck(
                name="configs",
                status=RunStatus.FAILED,
                message=f"Failed to load config files: {exc}",
            )

        invalid_provider_modes: dict[str, str] = {}
        for provider_name, cfg in provider_cfg.items():
            mode_raw = "placeholder"
            if isinstance(cfg, dict):
                mode_raw = str(cfg.get("mode", "placeholder"))
            try:
                ProviderMode(mode_raw)
            except Exception:
                invalid_provider_modes[str(provider_name)] = mode_raw

        if invalid_provider_modes:
            return PreflightCheck(
                name="configs",
                status=RunStatus.FAILED,
                message="Invalid provider mode values found",
                details={"invalid_provider_modes": invalid_provider_modes},
            )

        return PreflightCheck(
            name="configs",
            status=RunStatus.SUCCESS,
            message="Config files loaded and provider modes validated",
            details={
                "app_keys": sorted(app_cfg.keys()),
                "provider_keys": sorted(provider_cfg.keys()),
                "storage_keys": sorted(storage_cfg.keys()),
            },
        )

    def _check_storage_paths(self) -> PreflightCheck:
        try:
            storage_cfg = self.config_loader.load_storage_config()
            parquet_cfg = storage_cfg.get("parquet", {})
            if not isinstance(parquet_cfg, dict):
                parquet_cfg = {}

            raw_root = self.settings.resolve_path(parquet_cfg.get("raw_root", self.settings.raw_data_root))
            curated_root = self.settings.resolve_path(
                parquet_cfg.get("curated_root", self.settings.curated_data_root)
            )
            feature_root = self.settings.resolve_path(
                parquet_cfg.get("features_root", self.settings.feature_data_root)
            )
        except Exception as exc:
            return PreflightCheck(
                name="storage_paths",
                status=RunStatus.FAILED,
                message=f"Failed to resolve storage paths: {exc}",
            )

        problems: list[str] = []
        checked_paths: list[str] = []
        for root in [raw_root, curated_root, feature_root]:
            checked_paths.append(str(root))
            try:
                root.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                problems.append(f"mkdir_failed:{root}:{exc}")
                continue
            if not root.exists() or not root.is_dir():
                problems.append(f"not_directory:{root}")
                continue
            if not os.access(root, os.W_OK | os.X_OK):
                problems.append(f"not_writable:{root}")

        if problems:
            return PreflightCheck(
                name="storage_paths",
                status=RunStatus.FAILED,
                message="Storage path check failed",
                details={"paths": checked_paths, "problems": problems},
            )

        return PreflightCheck(
            name="storage_paths",
            status=RunStatus.SUCCESS,
            message="Storage paths are writable",
            details={"paths": checked_paths},
        )

    def _check_db(self, require_db_schema: bool) -> PreflightCheck:
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            return PreflightCheck(
                name="database",
                status=RunStatus.FAILED,
                message=f"Database connectivity check failed: {exc}",
            )

        if not require_db_schema:
            return PreflightCheck(
                name="database",
                status=RunStatus.SUCCESS,
                message="Database connectivity check passed (schema check skipped)",
            )

        try:
            inspector = inspect(self.db_engine)
            existing_tables = set(inspector.get_table_names())
            required_tables = set(Base.metadata.tables.keys())
            missing_tables = sorted(required_tables - existing_tables)
        except Exception as exc:
            return PreflightCheck(
                name="database",
                status=RunStatus.FAILED,
                message=f"Database schema inspection failed: {exc}",
            )

        if missing_tables:
            return PreflightCheck(
                name="database",
                status=RunStatus.WARNING,
                message="Database connected but schema is incomplete. Run scripts/init_db.py",
                details={"missing_tables": missing_tables},
            )

        return PreflightCheck(
            name="database",
            status=RunStatus.SUCCESS,
            message="Database connectivity and schema checks passed",
        )
