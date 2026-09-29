"""Tests for run-detail ingestion."""

import asyncio
import datetime
from typing import cast

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from sds_utils.dashboard.backend.db.models import (
    CachedDagsterRun,
    CachedRunEvent,
    DagsterCacheNamespace,
    DerivedJobRun,
)
from sds_utils.dashboard.backend.query.graphql_api import DagsterGraphQLClient
from sds_utils.dashboard.backend.query.graphql_api.run_details import RunDetails
from sds_utils.dashboard.backend.query.run_details import ingest_run_details


class FakeDetailsClient:
    """Return materialization or skip observations for requested run IDs."""

    def __init__(self) -> None:
        self.requested_run_ids: list[list[str]] = []

    async def run_details(
        self,
        *,
        run_ids: list[str],
        event_limit: int,
    ) -> RunDetails:
        self.requested_run_ids.append(run_ids)
        return RunDetails.model_validate(
            {
                "runsOrError": {
                    "__typename": "Runs",
                    "results": [
                        {
                            "runId": run_id,
                            "eventConnection": {
                                "events": self._events(run_id),
                                "cursor": f"{run_id}-cursor",
                                "hasMore": False,
                            },
                        }
                        for run_id in run_ids
                    ],
                }
            }
        )

    @staticmethod
    def _events(run_id: str) -> list[dict[str, object]]:
        if run_id in {
            "planned-only-run",
            "planned-overrides-selection-run",
            "latest-ineligible-run",
        }:
            events: list[dict[str, object]] = [
                {
                    "__typename": "AssetMaterializationPlannedEvent",
                    "runId": run_id,
                    "stepKey": "step-planned-asset",
                    "timestamp": "1788307199000",
                    "assetKey": {"path": ["planned-asset"]},
                }
            ]
            if run_id == "latest-ineligible-run":
                events.append(
                    {
                        "__typename": "LogMessageEvent",
                        "runId": run_id,
                        "stepKey": "step-planned-asset",
                        "timestamp": "1788307200000",
                        "message": (
                            "Submit response: skipped\n"
                            "  - Job already completed or in progress.,\n"
                            "  {'status': 'INPROGRESS'}"
                        ),
                    }
                )
            return events
        if run_id in {"materialized-run", "partial-run"}:
            assets = (
                ("asset-a", "asset-b") if run_id == "materialized-run" else ("asset-a",)
            )
            events = [
                {
                    "__typename": "MaterializationEvent",
                    "runId": run_id,
                    "stepKey": f"step-{asset}",
                    "timestamp": "1788307200000",
                    "partition": "2026-09-01",
                    "assetKey": {"path": [asset]},
                    "metadataEntries": [],
                }
                for asset in assets
            ]
            if run_id == "materialized-run":
                events.append(
                    {
                        "__typename": "LogMessageEvent",
                        "runId": run_id,
                        "stepKey": "step-asset-a",
                        "timestamp": "1788307199000",
                        "message": (
                            "Submit response: submitted\n"
                            "  - Job submitted successfully.,\n"
                            "  {'status': 'INPROGRESS'}"
                        ),
                    }
                )
            return events
        if run_id == "legacy-skipped-run":
            return [
                {
                    "__typename": "ExecutionStepSkippedEvent",
                    "runId": run_id,
                    "stepKey": "legacy-skipped-step",
                    "timestamp": "1788307200000",
                    "message": "Skipped because a legacy SkipReason was returned",
                }
            ]

        return [
            {
                "__typename": "ObservationEvent",
                "runId": run_id,
                "stepKey": f"step-{asset}",
                "timestamp": "1788307200000",
                "partition": "2026-09-01",
                "assetKey": {"path": [asset]},
                "metadataEntries": [
                    {
                        "__typename": "TextMetadataEntry",
                        "label": "status",
                        "text": "Skipped - Missing dependencies",
                    },
                    {
                        "__typename": "TextMetadataEntry",
                        "label": "missing_files",
                        "text": (
                            f"missing-{asset}"
                            if run_id == "different-skip-info-run"
                            else "shared missing-file details"
                        ),
                    },
                ],
            }
            for asset in ("asset-a", "asset-b")
        ]


def _cached_run(
    namespace_id: int,
    run_id: str,
    *,
    status: str = "SUCCESS",
) -> CachedDagsterRun:
    return CachedDagsterRun(
        namespace_id=namespace_id,
        run_id=run_id,
        job_name="example_job",
        partition="2026-09-01",
        dagster_status=status,
        creation_time=datetime.datetime(2026, 9, 1, tzinfo=datetime.UTC),
        selected_assets=[["asset-a"], ["asset-b"]],
    )


def test_ingest_run_details_derives_successful_runs_and_caches_events() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    with Session(db_engine) as session:
        namespace = DagsterCacheNamespace(
            name="default",
            graphql_url="https://dagster.example/graphql",
        )
        session.add(namespace)
        session.flush()
        assert namespace.id is not None
        materialized_run = _cached_run(namespace.id, "materialized-run")
        partial_run = _cached_run(namespace.id, "partial-run")
        skipped_run = _cached_run(namespace.id, "skipped-run")
        legacy_skipped_run = _cached_run(namespace.id, "legacy-skipped-run")
        different_skip_info_run = _cached_run(
            namespace.id,
            "different-skip-info-run",
        )
        planned_only_run = _cached_run(namespace.id, "planned-only-run")
        planned_only_run.selected_assets = []
        planned_overrides_selection_run = _cached_run(
            namespace.id, "planned-overrides-selection-run"
        )
        latest_ineligible_run = _cached_run(namespace.id, "latest-ineligible-run")
        failed_run = _cached_run(namespace.id, "failed-run", status="FAILURE")
        active_run = _cached_run(namespace.id, "active-run", status="STARTED")
        previously_derived_run = _cached_run(namespace.id, "already-derived-run")
        session.add_all(
            [
                materialized_run,
                partial_run,
                skipped_run,
                legacy_skipped_run,
                different_skip_info_run,
                planned_only_run,
                planned_overrides_selection_run,
                latest_ineligible_run,
                failed_run,
                active_run,
                previously_derived_run,
            ]
        )
        session.flush()
        assert previously_derived_run.id is not None
        session.add(
            DerivedJobRun(
                cached_run_id=previously_derived_run.id,
                dashboard_status="materialized",
                run_completed=True,
            )
        )
        session.commit()

    client = FakeDetailsClient()
    processed = asyncio.run(
        ingest_run_details(
            db_engine=db_engine,
            client=cast(DagsterGraphQLClient, client),
        )
    )

    assert processed == 10
    assert len(client.requested_run_ids) == 1
    assert set(client.requested_run_ids[0]) == {
        "materialized-run",
        "partial-run",
        "skipped-run",
        "legacy-skipped-run",
        "different-skip-info-run",
        "planned-only-run",
        "planned-overrides-selection-run",
        "latest-ineligible-run",
        "failed-run",
        "active-run",
    }
    with Session(db_engine) as session:
        cached_runs = {
            run.run_id: run for run in session.exec(select(CachedDagsterRun)).all()
        }
        derived_runs = {
            derived.cached_run_id: derived
            for derived in session.exec(select(DerivedJobRun)).all()
        }
        events = session.exec(select(CachedRunEvent)).all()

    materialized = derived_runs[cached_runs["materialized-run"].id]
    assert materialized.dashboard_status == "materialized"
    assert materialized.n_expected == 2
    assert materialized.n_materialized == 2
    assert materialized.n_skipped == 0
    assert materialized.n_missing == 0
    assert materialized.skip_info is None

    partial = derived_runs[cached_runs["partial-run"].id]
    assert partial.dashboard_status == "missing"
    assert partial.n_expected == 2
    assert partial.n_materialized == 1
    assert partial.n_skipped == 0
    assert partial.n_missing == 1
    assert partial.skip_info is None

    skipped = derived_runs[cached_runs["skipped-run"].id]
    assert skipped.dashboard_status == "skipped"
    assert skipped.n_expected == 2
    assert skipped.n_materialized == 0
    assert skipped.n_skipped == 2
    assert skipped.n_missing == 0
    assert skipped.skip_info == {
        "status": "Skipped - Missing dependencies",
        "missing_files": "shared missing-file details",
    }

    legacy_skipped = derived_runs[cached_runs["legacy-skipped-run"].id]
    assert legacy_skipped.dashboard_status == "skipped"
    assert legacy_skipped.n_skipped == 2
    assert legacy_skipped.n_missing == 0
    assert legacy_skipped.skip_info == {
        "skip_reason": "Skipped because a legacy SkipReason was returned"
    }

    different_skip_info = derived_runs[cached_runs["different-skip-info-run"].id]
    assert different_skip_info.n_skipped == 2
    assert different_skip_info.skip_info is None

    planned_only = derived_runs[cached_runs["planned-only-run"].id]
    assert planned_only.dashboard_status == "missing"
    assert planned_only.n_expected == 1
    assert planned_only.n_materialized == 0
    assert planned_only.n_skipped == 0
    assert planned_only.n_missing == 1

    planned_override = derived_runs[cached_runs["planned-overrides-selection-run"].id]
    assert planned_override.dashboard_status == "missing"
    assert planned_override.n_expected == 1
    assert planned_override.n_missing == 1

    latest_ineligible = derived_runs[cached_runs["latest-ineligible-run"].id]
    assert latest_ineligible.dashboard_status == "missing"
    assert latest_ineligible.n_skipped == 0
    assert latest_ineligible.n_missing == 1
    assert latest_ineligible.latest_eligible is False
    submission_events = [
        event
        for event in events
        if event.run_id == "latest-ineligible-run"
        and event.event_type == "LogMessageEvent"
    ]
    assert len(submission_events) == 1
    assert submission_events[0].payload["message"].startswith(
        "Submit response: skipped"
    )
    assert any(
        event.run_id == "materialized-run"
        and event.event_type == "LogMessageEvent"
        and event.payload["message"].startswith("Submit response: submitted")
        for event in events
    )

    failed = derived_runs[cached_runs["failed-run"].id]
    assert failed.dashboard_status == "failed"
    assert failed.run_completed is True
    assert failed.expected_assets == [["asset-a"], ["asset-b"]]

    active = derived_runs[cached_runs["active-run"].id]
    assert active.dashboard_status == "materializing"
    assert active.run_completed is False
    assert active.planned_assets == []
    assert active.expected_assets == [["asset-a"], ["asset-b"]]

    planned_only = derived_runs[cached_runs["planned-only-run"].id]
    assert planned_only.planned_assets == [["planned-asset"]]
    assert planned_only.expected_assets == [["planned-asset"]]

    for derived in derived_runs.values():
        if derived.n_expected is None:
            continue
        assert derived.n_materialized is not None
        assert derived.n_skipped is not None
        assert derived.n_missing is not None
        assert derived.n_expected == (
            derived.n_materialized + derived.n_skipped + derived.n_missing
        )

    assert len(events) == 17
    planned_event = next(
        event
        for event in events
        if event.event_type == "AssetMaterializationPlannedEvent"
    )
    assert planned_event.asset_key == '["planned-asset"]'
    skipped_events = [
        event
        for event in events
        if event.run_id == "skipped-run" and event.event_type == "ObservationEvent"
    ]
    assert all(
        event.event_metadata["status"] == "Skipped - Missing dependencies"
        for event in skipped_events
    )
    legacy_skip = next(
        event for event in events if event.event_type == "ExecutionStepSkippedEvent"
    )
    assert legacy_skip.payload["skip_reason"] == (
        "Skipped because a legacy SkipReason was returned"
    )
    assert legacy_skip.event_metadata["skip_reason"] == (
        "Skipped because a legacy SkipReason was returned"
    )
    assert all(
        event.payload["skip_reason"] == "Skipped - Missing dependencies"
        for event in skipped_events
    )

    processed_again = asyncio.run(
        ingest_run_details(
            db_engine=db_engine,
            client=cast(DagsterGraphQLClient, client),
        )
    )

    assert processed_again == 1
    assert client.requested_run_ids[-1] == ["active-run"]
    with Session(db_engine) as session:
        assert len(session.exec(select(CachedRunEvent)).all()) == 17
