"""Tests for logical job projection and phantom not-run rows."""

import pandas as pd

from sds_utils.dashboard.backend.jobkey import CurrentJobDefinition
from sds_utils.dashboard.backend.projectionbase import (
    JobProjectionMode,
    JobProjectionReason,
    JobProjectionType,
    ProjectionSpec,
    Projector,
)


def _definition(job_key: str, asset: str) -> CurrentJobDefinition:
    instrument, data_level, descriptor = job_key.split("_", 2)
    return CurrentJobDefinition(
        job_key,
        instrument,
        data_level,
        descriptor,
        frozenset({(asset,)}),
    )


def _dataframe() -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                "run_id": "current",
                "instrument": "hit",
                "data_level": "l1",
                "descriptor": "a",
                "job_key": "hit_l1_a",
                "partition": "daily_1",
                "planned_assets": [["asset-a"]],
                "selected_assets": [],
                "expected_assets": [["asset-a"]],
                "status": "materialized",
                "start_time": pd.Timestamp("2026-09-01", tz="UTC"),
                "end_time": pd.Timestamp("2026-09-02", tz="UTC"),
                "start_date": pd.Timestamp("2026-09-01", tz="UTC"),
                "end_date": pd.Timestamp("2026-09-02", tz="UTC"),
            },
            {
                "run_id": "assumed",
                "instrument": "hit",
                "data_level": "l1",
                "descriptor": "b",
                "job_key": "hit_l1_b",
                "partition": "daily_1",
                "planned_assets": [],
                "selected_assets": [],
                "expected_assets": [],
                "status": "failed",
                "start_time": pd.Timestamp("2026-09-01", tz="UTC"),
                "end_time": pd.Timestamp("2026-09-02", tz="UTC"),
                "start_date": pd.Timestamp("2026-09-01", tz="UTC"),
                "end_date": pd.Timestamp("2026-09-02", tz="UTC"),
            },
            {
                "run_id": "obsolete",
                "instrument": "hit",
                "data_level": "l1",
                "descriptor": "old",
                "job_key": "hit_l1_old",
                "partition": "daily_1",
                "planned_assets": [["old-asset"]],
                "selected_assets": [],
                "expected_assets": [["old-asset"]],
                "status": "materialized",
                "start_time": pd.Timestamp("2026-09-01", tz="UTC"),
                "end_time": pd.Timestamp("2026-09-02", tz="UTC"),
                "start_date": pd.Timestamp("2026-09-01", tz="UTC"),
                "end_date": pd.Timestamp("2026-09-02", tz="UTC"),
            },
        ]
    )


def _projector() -> Projector:
    definitions = {
        definition.job_key: definition
        for definition in (
            _definition("hit_l1_a", "asset-a"),
            _definition("hit_l1_b", "asset-b"),
            _definition("hit_l1_c", "asset-c"),
        )
    }
    return Projector(lambda _instruments: definitions)


def test_all_runs_are_classified_without_changing_row_population() -> None:
    result = _projector().apply(
        _dataframe(),
        ProjectionSpec(job_projection_mode=JobProjectionMode.ALL_RUNS),
    )

    assert result["run_id"].tolist() == ["current", "assumed", "obsolete"]
    assert result["job_projection_type"].tolist() == [
        JobProjectionType.CURRENT_RUN,
        JobProjectionType.ASSUMED_CURRENT_RUN,
        JobProjectionType.LEGACY_RUN,
    ]
    assert result["job_projection_reason"].tolist() == [
        JobProjectionReason.PLANNED_ASSETS_MATCH,
        JobProjectionReason.INSUFFICIENT_EVIDENCE,
        JobProjectionReason.JOB_KEY_NOT_DEFINED,
    ]


def test_latest_jobs_add_not_run_rows_and_remove_legacy_runs() -> None:
    result = _projector().apply(
        _dataframe(),
        ProjectionSpec(job_projection_mode=JobProjectionMode.LATEST_JOBS_ONLY),
    )

    assert set(result["run_id"].dropna()) == {"current", "assumed"}
    phantom = result[result["job_projection_type"] == JobProjectionType.NOT_RUN]
    assert phantom["job_key"].tolist() == ["hit_l1_c"]
    assert phantom["status"].tolist() == ["not-run"]
    assert phantom["n_expected"].tolist() == [1]
    assert phantom["source_kind"].tolist() == ["expectation"]


def test_legacy_runs_are_actual_row_complement_of_latest_jobs() -> None:
    result = _projector().apply(
        _dataframe(),
        ProjectionSpec(job_projection_mode=JobProjectionMode.LEGACY_RUNS_ONLY),
    )

    assert result["run_id"].tolist() == ["obsolete"]
    assert result["job_projection"].tolist() == ["legacy"]
