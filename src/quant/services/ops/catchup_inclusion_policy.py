from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CatchupInclusionPolicy:
    include_research_ready: bool = False
    include_unsynced_corporate_action_dates: bool = False

    @property
    def requires_sync_map(self) -> bool:
        return self.include_unsynced_corporate_action_dates

    def should_include(self, ready_row: object | None, sync_row: object | None = None) -> bool:
        if self.include_research_ready:
            return True

        if not self._is_research_ready(ready_row):
            return True

        if not self.include_unsynced_corporate_action_dates:
            return False

        return not self._is_sync_ready(sync_row)

    @staticmethod
    def _is_research_ready(ready_row: object | None) -> bool:
        if ready_row is None:
            return False
        return bool(getattr(ready_row, "research_ready", False))

    @staticmethod
    def _is_sync_ready(sync_row: object | None) -> bool:
        if sync_row is None:
            return False
        status = str(getattr(sync_row, "status", "")).upper()
        return status in {"SUCCESS", "WARNING"}

