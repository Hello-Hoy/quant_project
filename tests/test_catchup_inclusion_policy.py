from __future__ import annotations

from dataclasses import dataclass

from quant.services.ops.catchup_inclusion_policy import CatchupInclusionPolicy


@dataclass
class _Ready:
    research_ready: bool


@dataclass
class _Run:
    status: str


def test_policy_includes_not_ready_dates() -> None:
    policy = CatchupInclusionPolicy(
        include_research_ready=False,
        include_unsynced_corporate_action_dates=False,
    )

    assert policy.should_include(ready_row=None) is True
    assert policy.should_include(ready_row=_Ready(research_ready=False)) is True


def test_policy_excludes_ready_dates_by_default() -> None:
    policy = CatchupInclusionPolicy(
        include_research_ready=False,
        include_unsynced_corporate_action_dates=False,
    )

    assert policy.should_include(ready_row=_Ready(research_ready=True)) is False


def test_policy_include_research_ready_short_circuits() -> None:
    policy = CatchupInclusionPolicy(
        include_research_ready=True,
        include_unsynced_corporate_action_dates=False,
    )

    assert policy.should_include(ready_row=_Ready(research_ready=True)) is True


def test_policy_can_include_ready_dates_when_sync_missing() -> None:
    policy = CatchupInclusionPolicy(
        include_research_ready=False,
        include_unsynced_corporate_action_dates=True,
    )

    assert policy.requires_sync_map is True
    assert policy.should_include(ready_row=_Ready(research_ready=True), sync_row=None) is True


def test_policy_treats_success_warning_as_synced() -> None:
    policy = CatchupInclusionPolicy(
        include_research_ready=False,
        include_unsynced_corporate_action_dates=True,
    )

    assert policy.should_include(ready_row=_Ready(research_ready=True), sync_row=_Run(status="SUCCESS")) is False
    assert policy.should_include(ready_row=_Ready(research_ready=True), sync_row=_Run(status="warning")) is False
    assert policy.should_include(ready_row=_Ready(research_ready=True), sync_row=_Run(status="FAILED")) is True
