from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from quant.bootstrap.config_loader import ConfigLoader
from quant.bootstrap.settings import settings
from quant.providers.dart.corporate_action_provider import DartCorporateActionProvider
from quant.services.ingestion.corporate_action_event_mapper import canonicalize_event_type
from quant.storage.db.repositories.corporate_action_event_repository import (
    CorporateActionEventRepository,
)
from quant.storage.db.repositories.instrument_repository import InstrumentRepository


@dataclass
class CorporateActionIngestionResult:
    row_count: int
    mapped_count: int
    inserted_count: int
    updated_count: int
    skipped_unmapped_count: int
    event_type_counts: dict[str, int]
    message: str | None = None


class CorporateActionIngestionService:
    def __init__(
        self,
        session: Session,
        provider: DartCorporateActionProvider | None = None,
        event_repository: CorporateActionEventRepository | None = None,
        instrument_repository: InstrumentRepository | None = None,
        config_loader: ConfigLoader | None = None,
    ) -> None:
        self.config_loader = config_loader or ConfigLoader()
        if provider is None:
            runtime = self.config_loader.get_provider_runtime_config("dart")
            provider_cfg = self.config_loader.load_provider_config().get("dart", {})
            if not isinstance(provider_cfg, dict):
                provider_cfg = {}
            auth_cfg = provider_cfg.get("auth", {})
            if not isinstance(auth_cfg, dict):
                auth_cfg = {}
            response_cfg = provider_cfg.get("response", {})
            if not isinstance(response_cfg, dict):
                response_cfg = {}
            request_cfg = provider_cfg.get("request", {})
            if not isinstance(request_cfg, dict):
                request_cfg = {}

            response_success_values = response_cfg.get("success_values")
            if isinstance(response_success_values, list):
                success_values = [str(item) for item in response_success_values]
            else:
                success_values = None

            provider = DartCorporateActionProvider(
                **runtime,
                api_key=settings.dart_api_key,
                api_key_param_name=(
                    str(auth_cfg["api_key_param_name"])
                    if auth_cfg.get("api_key_param_name")
                    else None
                ),
                api_key_header_name=(
                    str(auth_cfg["api_key_header_name"])
                    if auth_cfg.get("api_key_header_name")
                    else None
                ),
                response_list_key=str(response_cfg.get("list_key", "list")),
                response_status_key=str(response_cfg.get("status_key", "status")),
                response_success_values=success_values,
                start_date_param_name=str(request_cfg.get("start_date_param_name", "start_date")),
                end_date_param_name=str(request_cfg.get("end_date_param_name", "end_date")),
                request_date_format=str(request_cfg.get("date_format", "ISO")),
            )
        self.provider = provider
        self.event_repository = event_repository or CorporateActionEventRepository(session)
        self.instrument_repository = instrument_repository or InstrumentRepository(session)

    def sync(self, start_date: str, end_date: str, force: bool = False) -> CorporateActionIngestionResult:
        _ = force
        rows = self.provider.fetch_corporate_actions(start_date=start_date, end_date=end_date)
        if not rows:
            return CorporateActionIngestionResult(
                row_count=0,
                mapped_count=0,
                inserted_count=0,
                updated_count=0,
                skipped_unmapped_count=0,
                event_type_counts={},
                message=self.provider.unavailable_message(
                    capability="corporate action sync",
                    context=f"range={start_date}~{end_date}",
                ),
            )

        mapped_count = 0
        inserted_count = 0
        updated_count = 0
        skipped_unmapped_symbols: list[str] = []
        event_type_counts: dict[str, int] = {}

        for row in rows:
            instrument = self.instrument_repository.get_preferred_by_symbol(row.symbol)
            if instrument is None:
                skipped_unmapped_symbols.append(row.symbol)
                continue

            canonical_event_type = canonicalize_event_type(row.event_type)
            _, is_inserted = self.event_repository.upsert_event(
                instrument_id=int(instrument.instrument_id),
                event_type=canonical_event_type,
                source="DART",
                announce_date=row.announce_date,
                ex_date=row.ex_date,
                effective_date=row.effective_date,
                ratio_value=row.ratio_value,
                cash_value=row.cash_value,
                raw_payload=row.raw_payload,
            )
            mapped_count += 1
            if is_inserted:
                inserted_count += 1
            else:
                updated_count += 1
            event_type_counts[canonical_event_type] = event_type_counts.get(canonical_event_type, 0) + 1

        message: str | None = None
        if skipped_unmapped_symbols:
            sample = sorted(set(skipped_unmapped_symbols))[:10]
            message = (
                "Some corporate action rows were skipped because symbol mapping failed: "
                f"{sample}"
            )

        return CorporateActionIngestionResult(
            row_count=mapped_count,
            mapped_count=mapped_count,
            inserted_count=inserted_count,
            updated_count=updated_count,
            skipped_unmapped_count=len(skipped_unmapped_symbols),
            event_type_counts=event_type_counts,
            message=message,
        )
