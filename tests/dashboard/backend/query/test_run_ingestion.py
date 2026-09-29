"""Tests for Dagster run ingestion."""

import asyncio
import datetime
from typing import cast

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select
import pytest

from sds_utils.dashboard.backend.db.models import (
    CacheIngestionState,
    CachedDagsterRun,
    DagsterCacheNamespace,
)
from sds_utils.dashboard.backend.query.graphql_api import (
    DagsterGraphQLClient,
    RunsFilter,
)
from sds_utils.dashboard.backend.query.graphql_api.runs_for_ingestion import (
    RunsForIngestion,
)
from sds_utils.dashboard.backend.query.graphql_api.run_count import RunCount
from sds_utils.dashboard.backend.query.run_ingestion import (
    RUN_INGESTION_STREAM,
    RunIngestionError,
    _plan_ingestion_ranges,
    ingest_runs,
)


class FakeClient:
    """Return predefined GraphQL responses while recording pagination calls."""

    def __init__(
        self,
        responses: list[RunsForIngestion],
        *,
        run_count: int = 0,
    ) -> None:
        self.responses = iter(responses)
        self.run_count_value = run_count
        self.cursors: list[str | None] = []
        self.filters: list[RunsFilter] = []
        self.count_filters: list[RunsFilter] = []

    async def run_count(self, filter_: RunsFilter) -> RunCount:
        self.count_filters.append(filter_)
        return RunCount.model_validate(
            {"runsOrError": {"__typename": "Runs", "count": self.run_count_value}}
        )

    async def runs_for_ingestion(
        self,
        *,
        filter_: RunsFilter,
        cursor: str | None,
        limit: int,
    ) -> RunsForIngestion:
        self.cursors.append(cursor)
        self.filters.append(filter_)
        return next(self.responses)


def _response(*run_ids: str) -> RunsForIngestion:
    return RunsForIngestion.model_validate(
        {
            "runsOrError": {
                "__typename": "Runs",
                "results": [
                    {
                        "runId": run_id,
                        "jobName": "imap-hi_l1b_45-sensor-hk_processing_job",
                        "status": "SUCCESS",
                        "creationTime": 1_700_000_000.0,
                        "updateTime": 1_700_000_100.0,
                        "startTime": 1_700_000_010.0,
                        "endTime": 1_700_000_090.0,
                        "parentRunId": None,
                        "rootRunId": None,
                        "assetSelection": [{"path": ["example", "asset"]}],
                        "tags": [
                            {
                                "key": "dagster/partition",
                                "value": (
                                    "science_2026-09-19T00:00:00+00:00_to_"
                                    "2026-09-20T00:00:00+00:00"
                                ),
                            }
                        ],
                    }
                    for run_id in run_ids
                ],
            }
        }
    )


def test_ingest_runs_pages_upserts_and_sets_watermarks() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    client = FakeClient(
        [_response("run-2"), _response("run-1"), _response()],
        run_count=2,
    )
    start = datetime.datetime(2026, 8, 1, tzinfo=datetime.UTC)
    end = datetime.datetime(2026, 8, 29, 23, 59, tzinfo=datetime.UTC)

    count = asyncio.run(
        ingest_runs(
            start,
            end,
            graphql_url="https://dagster.example/graphql",
            page_size=1,
            show_progress=True,
            db_engine=db_engine,
            client=cast(DagsterGraphQLClient, client),
        )
    )

    assert count == 2
    assert len(client.count_filters) == 1
    assert client.cursors == [None, "run-2", "run-1"]
    with Session(db_engine) as session:
        state = session.exec(select(CacheIngestionState)).one()
        runs = session.exec(select(CachedDagsterRun)).all()

    assert state.stream == RUN_INGESTION_STREAM
    assert state.watermark_start == start.replace(tzinfo=None)
    assert state.watermark_end == end.replace(tzinfo=None)
    assert {run.run_id for run in runs} == {"run-1", "run-2"}
    assert all(run.partition_start_time == datetime.datetime(2026, 9, 19) for run in runs)
    assert all(run.partition_end_time == datetime.datetime(2026, 9, 20) for run in runs)
    assert all(run.partition_prefix == "science" for run in runs)
    assert all(run.partition_label == "science" for run in runs)
    assert all(run.repoint is None for run in runs)
    assert all(run.job_key == "imap-hi_l1b_45-sensor-hk" for run in runs)
    assert all(run.selected_assets == [["example", "asset"]] for run in runs)


def test_ingest_runs_extends_both_sides_without_querying_cached_middle() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    current_start = datetime.datetime(2026, 8, 10, tzinfo=datetime.UTC)
    current_end = datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC)
    with Session(db_engine) as session:
        namespace = DagsterCacheNamespace(
            name="default",
            graphql_url="https://dagster.example/graphql",
        )
        session.add(namespace)
        session.flush()
        assert namespace.id is not None
        session.add(
            CacheIngestionState(
                namespace_id=namespace.id,
                stream=RUN_INGESTION_STREAM,
                watermark_start=current_start,
                watermark_end=current_end,
            )
        )
        session.commit()

    requested_start = datetime.datetime(2026, 8, 1, tzinfo=datetime.UTC)
    requested_end = datetime.datetime(2026, 8, 29, tzinfo=datetime.UTC)
    buffer = datetime.timedelta(hours=1)
    client = FakeClient([_response(), _response()])

    asyncio.run(
        ingest_runs(
            requested_start,
            requested_end,
            graphql_url="https://dagster.example/graphql",
            overlap_buffer=buffer,
            db_engine=db_engine,
            client=cast(DagsterGraphQLClient, client),
        )
    )

    assert len(client.filters) == 2
    first_filter, second_filter = client.filters
    assert first_filter.updated_after < requested_start.timestamp()
    assert first_filter.updated_before > (current_start + buffer).timestamp()
    assert second_filter.updated_after < (current_end - buffer).timestamp()
    assert second_filter.updated_before > requested_end.timestamp()
    with Session(db_engine) as session:
        state = session.exec(select(CacheIngestionState)).one()
    assert state.watermark_start == requested_start.replace(tzinfo=None)
    assert state.watermark_end == requested_end.replace(tzinfo=None)


def test_ingest_runs_can_extend_only_start() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    current_start = datetime.datetime(2026, 8, 10, tzinfo=datetime.UTC)
    current_end = datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC)
    with Session(db_engine) as session:
        namespace = DagsterCacheNamespace(
            name="default",
            graphql_url="https://dagster.example/graphql",
        )
        session.add(namespace)
        session.flush()
        assert namespace.id is not None
        session.add(
            CacheIngestionState(
                namespace_id=namespace.id,
                stream=RUN_INGESTION_STREAM,
                watermark_start=current_start,
                watermark_end=current_end,
            )
        )
        session.commit()
    requested_start = datetime.datetime(2026, 8, 1, tzinfo=datetime.UTC)
    client = FakeClient([_response()])

    asyncio.run(
        ingest_runs(
            requested_start,
            None,
            graphql_url="https://dagster.example/graphql",
            db_engine=db_engine,
            client=cast(DagsterGraphQLClient, client),
        )
    )

    assert len(client.filters) == 1
    with Session(db_engine) as session:
        state = session.exec(select(CacheIngestionState)).one()
    assert state.watermark_start == requested_start.replace(tzinfo=None)
    assert state.watermark_end == current_end.replace(tzinfo=None)


def test_ingest_runs_rejects_disjoint_range() -> None:
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
        session.add(
            CacheIngestionState(
                namespace_id=namespace.id,
                stream=RUN_INGESTION_STREAM,
                watermark_start=datetime.datetime(2026, 8, 10),
                watermark_end=datetime.datetime(2026, 8, 20),
            )
        )
        session.commit()
    client = FakeClient([])

    with pytest.raises(RunIngestionError, match="does not overlap"):
        asyncio.run(
            ingest_runs(
                datetime.datetime(2026, 9, 1, tzinfo=datetime.UTC),
                datetime.datetime(2026, 9, 10, tzinfo=datetime.UTC),
                graphql_url="https://dagster.example/graphql",
                db_engine=db_engine,
                client=cast(DagsterGraphQLClient, client),
            )
        )

    assert client.filters == []


def test_plan_ingestion_ranges_limits_end_watermark_to_now() -> None:
    current_start = datetime.datetime(2026, 8, 10, tzinfo=datetime.UTC)
    current_end = datetime.datetime(2026, 8, 20, tzinfo=datetime.UTC)
    now = datetime.datetime(2026, 8, 25, tzinfo=datetime.UTC)
    state = CacheIngestionState(
        namespace_id=1,
        stream=RUN_INGESTION_STREAM,
        watermark_start=current_start,
        watermark_end=current_end,
    )

    ranges, new_start, new_end = _plan_ingestion_ranges(
        state,
        requested_start=None,
        requested_end=datetime.datetime(2026, 9, 1, tzinfo=datetime.UTC),
        overlap_buffer=datetime.timedelta(minutes=5),
        now=now,
    )

    assert len(ranges) == 1
    assert ranges[0].end == now
    assert new_start == current_start
    assert new_end == now
