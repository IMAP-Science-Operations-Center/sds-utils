"""Tests for the dashboard database data source."""

import datetime
import logging

import pandas as pd
import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from sds_utils.dashboard.backend.data import QuerySpec
from sds_utils.dashboard.backend.db.models import (
    CachedAssetMaterialization,
    CachedDagsterRun,
    DagsterCacheNamespace,
    DerivedJobRun,
)
from sds_utils.dashboard.backend.dbdata import (
    _DAGSTER_STATUS_MAP,
    DBDataSource,
    _status,
)
from sds_utils.dashboard.backend.query.graphql_api import RunStatus


def test_all_dagster_statuses_have_dashboard_mappings() -> None:
    assert set(_DAGSTER_STATUS_MAP) == {status.value for status in RunStatus}
    assert _DAGSTER_STATUS_MAP[RunStatus.CANCELING.value] == "canceling"
    assert _DAGSTER_STATUS_MAP[RunStatus.CANCELED.value] == "canceled"


def _run(
    namespace_id: int,
    run_id: str,
    status: str,
    update_time: datetime.datetime,
) -> CachedDagsterRun:
    creation_time = update_time - datetime.timedelta(days=30)
    partition_start_time = datetime.datetime(2026, 9, 5, tzinfo=datetime.UTC)
    partition_end_time = datetime.datetime(
        2026, 9, 6, 23, 59, 59, tzinfo=datetime.UTC
    )
    return CachedDagsterRun(
        namespace_id=namespace_id,
        run_id=run_id,
        job_name="imap-hi_l1b_45-sensor-hk_processing_job",
        job_key="imap-hi_l1b_45-sensor-hk",
        partition=("repoint343_2026-09-05T00:00:00+00:00_to_2026-09-06T23:59:59+00:00"),
        partition_prefix="repoint343",
        partition_label="repoint",
        repoint=343,
        partition_start_time=partition_start_time,
        partition_end_time=partition_end_time,
        dagster_status=status,
        creation_time=creation_time,
        update_time=update_time,
        start_time=creation_time + datetime.timedelta(minutes=1),
        end_time=creation_time + datetime.timedelta(minutes=3),
        selected_assets=[["asset-a"], ["asset-b"]],
    )


def test_unknown_dagster_status_is_unknown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    run = _run(1, "future-status-run", "FUTURE_STATUS", datetime.datetime(2026, 9, 10))

    with caplog.at_level(logging.WARNING):
        status = _status(run, None)

    assert status == "unknown"
    assert "Unknown Dagster run status 'FUTURE_STATUS'" in caplog.text
    assert "future-status-run" in caplog.text


def test_query_builds_synthetic_l0_rows_from_asset_materializations() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    timestamp = datetime.datetime(2026, 9, 12, 12, tzinfo=datetime.UTC)
    with Session(db_engine) as session:
        namespace = DagsterCacheNamespace(
            name="default",
            graphql_url="https://dagster.example/graphql",
        )
        session.add(namespace)
        session.flush()
        assert namespace.id is not None
        session.add(
            CachedAssetMaterialization(
                namespace_id=namespace.id,
                event_key="event-key",
                asset_key="glows_l0_raw",
                partition=(
                    "repoint369_2026-09-12T10:03:12_to_2026-09-13T10:03:10"
                ),
                partition_prefix="repoint369",
                partition_label="repoint",
                repoint=369,
                partition_start_time=datetime.datetime(
                    2026, 9, 12, 10, 3, 12, tzinfo=datetime.UTC
                ),
                partition_end_time=datetime.datetime(
                    2026, 9, 13, 10, 3, 10, tzinfo=datetime.UTC
                ),
                run_id="sensor-run",
                timestamp=timestamp,
            )
        )
        session.commit()

    data_df = DBDataSource(db_engine, "default").query(
        QuerySpec(
            start_time=datetime.datetime(2026, 9, 12, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 9, 13, tzinfo=datetime.UTC),
        )
    )

    row = data_df.iloc[0]
    assert row["job_key"] == "glows_l0_none"
    assert row["status"] == "materialized"
    assert row["n_expected"] == 1
    assert row["n_materialized"] == 1
    assert row["expected_assets"] == [["glows_l0_raw"]]
    assert row["source_kind"] == "l0_asset_materialization"


def test_latest_query_ignores_latest_ineligible_runs() -> None:
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
        eligible = _run(
            namespace.id,
            "eligible",
            "SUCCESS",
            datetime.datetime(2026, 9, 10, 12),
        )
        ineligible = _run(
            namespace.id,
            "ineligible",
            "SUCCESS",
            datetime.datetime(2026, 9, 10, 13),
        )
        session.add_all([eligible, ineligible])
        session.flush()
        assert eligible.id is not None
        assert ineligible.id is not None
        session.add_all(
            [
                DerivedJobRun(
                    cached_run_id=eligible.id,
                    dashboard_status="materialized",
                    latest_eligible=True,
                ),
                DerivedJobRun(
                    cached_run_id=ineligible.id,
                    dashboard_status="missing",
                    latest_eligible=False,
                ),
            ]
        )
        session.commit()

    data_df = DBDataSource(db_engine, "default").query(
        QuerySpec(
            start_time=datetime.datetime(2026, 9, 10, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
            version_mode="latest",
        )
    )

    assert data_df["run_id"].tolist() == ["eligible"]


def test_query_builds_dashboard_dataframe_from_relevant_runs(
) -> None:
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
        other_namespace = DagsterCacheNamespace(
            name="other",
            graphql_url="https://other-dagster.example/graphql",
        )
        session.add_all([namespace, other_namespace])
        session.flush()
        assert namespace.id is not None
        assert other_namespace.id is not None

        successful = _run(
            namespace.id,
            "successful",
            "SUCCESS",
            datetime.datetime(2026, 9, 10, 12),
        )
        failed = _run(
            namespace.id,
            "failed",
            "FAILURE",
            datetime.datetime(2026, 9, 10, 13),
        )
        failed.partition = (
            "long-label_2026-09-07T00:00:00+00:00_to_2026-09-08T00:00:00+00:00"
        )
        failed.partition_prefix = "long-label"
        failed.partition_label = "long-label"
        failed.repoint = None
        failed.partition_start_time = datetime.datetime(
            2026, 9, 7, tzinfo=datetime.UTC
        )
        failed.partition_end_time = datetime.datetime(2026, 9, 8, tzinfo=datetime.UTC)
        running = _run(
            namespace.id,
            "running",
            "STARTED",
            datetime.datetime(2026, 9, 10, 14),
        )
        canceled = _run(
            namespace.id,
            "canceled",
            "CANCELED",
            datetime.datetime(2026, 9, 10, 13, 30),
        )
        canceling = _run(
            namespace.id,
            "canceling",
            "CANCELING",
            datetime.datetime(2026, 9, 10, 13, 45),
        )
        pending_details = _run(
            namespace.id,
            "pending-details",
            "SUCCESS",
            datetime.datetime(2026, 9, 10, 15),
        )
        pending_details.job_name = "unparsable"
        pending_details.job_key = None
        pending_details.partition = None
        pending_details.partition_prefix = None
        pending_details.partition_label = None
        pending_details.repoint = None
        pending_details.partition_start_time = None
        pending_details.partition_end_time = None
        reprocessed = _run(
            namespace.id,
            "reprocessed",
            "SUCCESS",
            datetime.datetime(2026, 9, 10, 14, 30),
        )
        reprocessed.job_name = "__ASSET_JOB"
        reprocessed.job_key = "hit_l2_summedintensity"
        reprocessed.selected_assets = [["hit_l2_summedintensity"]]
        idex_raw = _run(
            namespace.id,
            "idex-raw",
            "SUCCESS",
            datetime.datetime(2026, 9, 10, 14, 15),
        )
        idex_raw.job_name = "__ASSET_JOB"
        idex_raw.job_key = "idex_l0"
        idex_raw.selected_assets = [["idex_l0_raw"]]
        outside = _run(
            namespace.id,
            "outside",
            "SUCCESS",
            datetime.datetime(2026, 9, 12),
        )
        other_namespace_run = _run(
            other_namespace.id,
            "other-namespace-run",
            "FAILURE",
            datetime.datetime(2026, 9, 10, 16),
        )
        session.add_all(
            [
                successful,
                failed,
                running,
                canceled,
                canceling,
                pending_details,
                reprocessed,
                idex_raw,
                outside,
                other_namespace_run,
            ]
        )
        session.flush()
        assert successful.id is not None
        assert outside.id is not None
        session.add_all(
            [
                    DerivedJobRun(
                        cached_run_id=successful.id,
                        dashboard_status="skipped",
                        n_expected=2,
                        n_materialized=1,
                        n_skipped=1,
                        n_missing=0,
                    skip_info={
                        "status": "Skipped - Missing dependencies",
                        "missing_files": "imap_test_missing.cdf",
                    },
                ),
                DerivedJobRun(
                    cached_run_id=outside.id,
                    dashboard_status="materialized",
                ),
            ]
        )
        session.commit()

    data_df = DBDataSource(db_engine, "default").query(
        QuerySpec(
            start_time=datetime.datetime(2026, 9, 10, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
        )
    )

    assert data_df["run_id"].tolist() == [
        "pending-details",
        "reprocessed",
        "idex-raw",
        "running",
        "canceling",
        "canceled",
        "failed",
        "successful",
    ]
    assert data_df.set_index("run_id")["status"].to_dict() == {
        "pending-details": "unknown",
        "reprocessed": "unknown",
        "idex-raw": "unknown",
        "running": "materializing",
        "canceling": "canceling",
        "canceled": "canceled",
        "failed": "failed",
        "successful": "skipped",
    }
    successful_row = data_df.set_index("run_id").loc["successful"]
    assert successful_row["instrument"] == "imap-hi"
    assert successful_row["data_level"] == "l1b"
    assert successful_row["descriptor"] == "45-sensor-hk"
    assert successful_row["job_key"] == "imap-hi_l1b_45-sensor-hk"
    reprocessed_row = data_df.set_index("run_id").loc["reprocessed"]
    assert reprocessed_row["job_name"] == "__ASSET_JOB"
    assert reprocessed_row["instrument"] == "hit"
    assert reprocessed_row["data_level"] == "l2"
    assert reprocessed_row["descriptor"] == "summedintensity"
    assert reprocessed_row["job_key"] == "hit_l2_summedintensity"
    idex_raw_row = data_df.set_index("run_id").loc["idex-raw"]
    assert idex_raw_row["instrument"] == "idex"
    assert idex_raw_row["data_level"] == "l0"
    assert pd.isna(idex_raw_row["descriptor"])
    assert idex_raw_row["job_key"] == "idex_l0"
    assert successful_row["partition_label"] == "repoint"
    assert successful_row["repoint"] == 343
    assert successful_row["n_expected"] == 2
    assert successful_row["n_skipped"] == 1
    assert successful_row["n_missing"] == 0
    assert successful_row["skip_reason"] == "Skipped - Missing dependencies"
    assert successful_row["missing_files"] == "imap_test_missing.cdf"
    assert successful_row["duration_seconds"] == 120
    assert successful_row["start_time"] == pd.Timestamp("2026-09-05T00:00:00Z")
    assert successful_row["end_time"] == pd.Timestamp("2026-09-06T23:59:59Z")
    assert successful_row["start_date"] == pd.Timestamp(
        "2026-09-05",
        tz="UTC",
    )
    assert successful_row["end_date"] == pd.Timestamp(
        "2026-09-06",
        tz="UTC",
    )
    failed_row = data_df.set_index("run_id").loc["failed"]
    assert failed_row["partition_label"] == "long-label"
    assert pd.isna(failed_row["repoint"])
    unparseable_row = data_df.set_index("run_id").loc["pending-details"]
    assert pd.isna(unparseable_row["instrument"])
    assert pd.isna(unparseable_row["data_level"])
    assert pd.isna(unparseable_row["descriptor"])
    assert pd.isna(unparseable_row["partition_label"])
    assert pd.isna(unparseable_row["repoint"])
    assert pd.isna(unparseable_row["start_time"])
    assert pd.isna(unparseable_row["end_time"])
    assert data_df["n_expected"].dtype == pd.Int64Dtype()
    assert str(data_df["creation_time"].dtype) == "datetime64[ns, UTC]"
    assert str(data_df["start_date"].dtype) == "datetime64[ns, UTC]"

    latest_df = DBDataSource(db_engine, "default").query(
        QuerySpec(
            start_time=datetime.datetime(2026, 9, 10, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
            version_mode="latest",
        )
    )
    assert latest_df["run_id"].tolist() == [
        "pending-details",
        "reprocessed",
        "idex-raw",
        "running",
        "failed",
    ]

    partition_df = DBDataSource(db_engine, "default").query(
        QuerySpec(
            start_time=datetime.datetime(2026, 9, 5, 12, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 9, 5, 12, tzinfo=datetime.UTC),
            date_mode="partition",
        )
    )
    assert set(partition_df["run_id"]) == {
        "successful",
        "running",
        "canceled",
        "canceling",
        "reprocessed",
        "idex-raw",
        "outside",
    }

    missing_namespace_df = DBDataSource(db_engine, "missing").query(
        QuerySpec(
            start_time=datetime.datetime(2026, 9, 10, tzinfo=datetime.UTC),
            end_time=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
        )
    )
    assert missing_namespace_df.empty
