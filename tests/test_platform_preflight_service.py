from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine

from quant.core.enums import RunStatus
from quant.services.ops.platform_preflight_service import PlatformPreflightService
from quant.storage.db.base import Base


class _FakeConfigLoader:
    def __init__(self, provider_mode: str = "placeholder") -> None:
        self.provider_mode = provider_mode

    def load_app_config(self) -> dict[str, object]:
        return {"app": {"name": "quant-project"}}

    def load_provider_config(self) -> dict[str, object]:
        return {
            "krx": {
                "mode": self.provider_mode,
            }
        }

    def load_storage_config(self) -> dict[str, object]:
        return {
            "parquet": {
                "raw_root": "data/raw",
                "curated_root": "data/curated",
                "features_root": "data/features",
            }
        }


@dataclass
class _FakeSettings:
    project_root: Path

    raw_data_root: str = "data/raw"
    curated_data_root: str = "data/curated"
    feature_data_root: str = "data/features"

    def resolve_path(self, value):  # noqa: ANN001, ANN201
        path = value if isinstance(value, Path) else Path(value)
        return path if path.is_absolute() else (self.project_root / path)


def _build_service(tmp_path: Path, provider_mode: str = "placeholder") -> PlatformPreflightService:
    db_path = tmp_path / "preflight.db"
    db_engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    return PlatformPreflightService(
        config_loader=_FakeConfigLoader(provider_mode=provider_mode),
        db_engine=db_engine,
        settings_obj=_FakeSettings(project_root=tmp_path),
    )


def test_preflight_warns_when_schema_missing(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    report = service.run(require_db_schema=True)

    assert report.status == RunStatus.WARNING
    db_check = next(check for check in report.checks if check.name == "database")
    assert db_check.status == RunStatus.WARNING
    assert "missing_tables" in db_check.details


def test_preflight_succeeds_when_schema_present(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    Base.metadata.create_all(bind=service.db_engine)

    report = service.run(require_db_schema=True)

    assert report.status == RunStatus.SUCCESS
    db_check = next(check for check in report.checks if check.name == "database")
    assert db_check.status == RunStatus.SUCCESS


def test_preflight_fails_on_invalid_provider_mode(tmp_path: Path) -> None:
    service = _build_service(tmp_path, provider_mode="broken-mode")

    report = service.run(require_db_schema=False)

    assert report.status == RunStatus.FAILED
    config_check = next(check for check in report.checks if check.name == "configs")
    assert config_check.status == RunStatus.FAILED


def test_preflight_can_skip_database_checks(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    report = service.run(require_db_schema=True, check_database=False)

    assert report.status == RunStatus.SUCCESS
    db_check = next(check for check in report.checks if check.name == "database")
    assert db_check.status == RunStatus.SUCCESS
    assert "skipped" in db_check.message.lower()
