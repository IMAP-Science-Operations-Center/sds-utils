"""Database-backed dataframe source for the dashboard."""

import datetime
import logging
import re

import pandas as pd
from sqlalchemy import Engine, func, or_
from sqlmodel import Session, col, select

from .data import DataSourceBase, QuerySpec
from .db.models import (
    CachedAssetMaterialization,
    CachedDagsterRun,
    DagsterCacheNamespace,
    DerivedJobRun,
)
from .jobkey import parse_job_key

logger = logging.getLogger(__name__)

_DATETIME_COLUMNS = (
    "creation_time",
    "update_time",
    "start_time",
    "end_time",
    "run_start_time",
    "run_end_time",
    "start_date",
    "end_date",
)
_COUNT_COLUMNS = (
    "n_expected",
    "n_materialized",
    "n_skipped",
    "n_missing",
)
_DAGSTER_STATUS_MAP = {
    "QUEUED": "materializing",
    "NOT_STARTED": "materializing",
    "MANAGED": "materializing",
    "STARTING": "materializing",
    "STARTED": "materializing",
    "SUCCESS": "unknown",
    "FAILURE": "failed",
    "CANCELING": "canceling",
    "CANCELED": "canceled",
}
_COLUMNS = (
    "run_id",
    "instrument",
    "data_level",
    "descriptor",
    "job_key",
    "partition",
    "partition_prefix",
    "partition_label",
    "repoint",
    "job_name",
    "status",
    "dagster_status",
    "creation_time",
    "update_time",
    "start_time",
    "end_time",
    "run_start_time",
    "run_end_time",
    "start_date",
    "end_date",
    "duration_seconds",
    "parent_run_id",
    "root_run_id",
    "selected_assets",
    "planned_assets",
    "expected_assets",
    "source_kind",
    "latest_eligible",
    "tags",
    *_COUNT_COLUMNS,
    "skip_info",
    "skip_reason",
    "missing_files",
)


def _utc_naive(value: datetime.datetime) -> datetime.datetime:
    """Represent an input timestamp as naive UTC for SQLite comparisons."""
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(datetime.UTC).replace(tzinfo=None)


def _status(run: CachedDagsterRun, derived: DerivedJobRun | None) -> str:
    if derived is not None:
        return derived.dashboard_status
    status = _DAGSTER_STATUS_MAP.get(run.dagster_status)
    if status is None:
        logger.warning(
            "Unknown Dagster run status %r for run %s; marking it unknown",
            run.dagster_status,
            run.run_id,
        )
        return "unknown"
    return status


def _parse_skipped_reason(missing_files: str | None) -> str | None:
    """Determine skipped_reason from the more verbose missing_files."""
    if missing_files is None:
        return None
    lines = [line.strip() for line in missing_files.splitlines()]

    prefix = "Missing "
    if (
        len(lines) == 2  # noqa: PLR2004
        and lines[0] == "Not enough information to process."
        and lines[1].startswith(prefix)
    ):
        return lines[1].removeprefix(prefix).split()[0]

    prefix = "Missing SPICE files"
    if len(lines) == 1 and lines[0].startswith(prefix):
        return "SPICE"

    regex = (
        r"Hi Goodtimes: skipping repoint \d+ - pointing (\d+) does not exist yet, "
        r"waiting for more data to fill in."
    )
    if len(lines) == 1 and (match := re.fullmatch(regex, lines[0])):
        return f"pointing{match.group(1)}"

    first_line = "Missing dependency for"
    if len(lines) == 3 and lines[0] == first_line:  # noqa: PLR2004
        return lines[1]

    return missing_files


def _skip_columns(
    derived: DerivedJobRun | None,
) -> tuple[dict[str, str] | None, str | None, str | None]:
    skip_info = derived.skip_info if derived is not None else None
    if skip_info is None:
        return None, None, None
    return (
        skip_info,
        skip_info.get("status") or skip_info.get("skip_reason"),
        skip_info.get("missing_files"),
    )


class DBDataSource(DataSourceBase):
    """Load cached and derived Dagster runs into dashboard dataframes."""

    def __init__(self, engine: Engine, dagster_namespace: str):
        self.engine = engine
        self.namespace = dagster_namespace

    def query(self, query: QuerySpec) -> pd.DataFrame:  # noqa: PLR0912, PLR0915
        """Return runs selected by update time or overlapping partition interval."""
        start_time = _utc_naive(query.start_time)
        end_time = _utc_naive(query.end_time)
        if start_time > end_time:
            raise ValueError("Query start_time must not be later than end_time")

        statement = (
            select(CachedDagsterRun, DerivedJobRun)
            .join(
                DagsterCacheNamespace,
                col(CachedDagsterRun.namespace_id) == DagsterCacheNamespace.id,
            )
            .outerjoin(
                DerivedJobRun,
                col(DerivedJobRun.cached_run_id) == CachedDagsterRun.id,
            )
            .where(
                DagsterCacheNamespace.name == self.namespace,
            )
            .order_by(col(CachedDagsterRun.update_time).desc())
        )
        if query.date_mode == "update_time":
            statement = statement.where(
                col(CachedDagsterRun.update_time) >= start_time,
                col(CachedDagsterRun.update_time) <= end_time,
            )
        else:
            statement = statement.where(
                col(CachedDagsterRun.partition_start_time) <= end_time,
                col(CachedDagsterRun.partition_end_time) >= start_time,
            )
        if query.version_mode == "latest":
            ranked_runs = (
                select(
                    CachedDagsterRun.id.label("cached_run_id"),
                    func.row_number()
                    .over(
                        partition_by=(
                            col(CachedDagsterRun.job_key),
                            col(CachedDagsterRun.partition),
                        ),
                        order_by=col(CachedDagsterRun.update_time).desc(),
                    )
                    .label("version_rank"),
                )
                .join(
                    DagsterCacheNamespace,
                    col(CachedDagsterRun.namespace_id) == DagsterCacheNamespace.id,
                )
                .outerjoin(
                    DerivedJobRun,
                    col(DerivedJobRun.cached_run_id) == CachedDagsterRun.id,
                )
                .where(
                    DagsterCacheNamespace.name == self.namespace,
                    or_(
                        col(DerivedJobRun.id).is_(None),
                        col(DerivedJobRun.latest_eligible).is_(True),
                    ),
                )
            )
            if query.date_mode == "update_time":
                ranked_runs = ranked_runs.where(
                    col(CachedDagsterRun.update_time) >= start_time,
                    col(CachedDagsterRun.update_time) <= end_time,
                )
            else:
                ranked_runs = ranked_runs.where(
                    col(CachedDagsterRun.partition_start_time) <= end_time,
                    col(CachedDagsterRun.partition_end_time) >= start_time,
                )
            ranked_runs_subquery = ranked_runs.subquery()
            latest_run_ids = select(ranked_runs_subquery.c.cached_run_id).where(
                ranked_runs_subquery.c.version_rank == 1
            )
            statement = statement.where(col(CachedDagsterRun.id).in_(latest_run_ids))
        with Session(self.engine) as session:
            results = list(session.exec(statement))

        records: list[dict[str, object]] = []
        for run, derived in results:
            skip_info, skip_reason, missing_files = _skip_columns(derived)
            job_key = parse_job_key(run.job_key)
            records.append(
                {
                    "run_id": run.run_id,
                    "instrument": job_key.instrument,
                    "data_level": job_key.data_level,
                    "descriptor": job_key.descriptor,
                    "job_key": job_key.job_key,
                    "job_name": run.job_name,
                    "partition": run.partition,
                    "partition_prefix": run.partition_prefix,
                    "partition_label": run.partition_label,
                    "repoint": run.repoint,
                    "start_time": run.partition_start_time,
                    "end_time": run.partition_end_time,
                    "status": _status(run, derived),
                    "dagster_status": run.dagster_status,
                    "n_expected": derived.n_expected if derived is not None else None,
                    "n_materialized": (
                        derived.n_materialized if derived is not None else None
                    ),
                    "n_skipped": derived.n_skipped if derived is not None else None,
                    "n_missing": derived.n_missing if derived is not None else None,
                    "skip_info": skip_info,
                    "skip_reason": skip_reason,
                    "missing_files": missing_files,
                    "skipped_reason": _parse_skipped_reason(missing_files),
                    "creation_time": run.creation_time,
                    "update_time": run.update_time,
                    "run_start_time": run.start_time,
                    "run_end_time": run.end_time,
                    "duration_seconds": (
                        (run.end_time - run.start_time).total_seconds()
                        if run.start_time is not None and run.end_time is not None
                        else None
                    ),
                    "parent_run_id": run.parent_run_id,
                    "root_run_id": run.root_run_id,
                    "selected_assets": run.selected_assets,
                    "planned_assets": (
                        derived.planned_assets if derived is not None else []
                    ),
                    "expected_assets": (
                        derived.expected_assets if derived is not None else []
                    ),
                    "source_kind": "dagster_run",
                    "latest_eligible": (
                        derived.latest_eligible if derived is not None else True
                    ),
                    "tags": run.tags,
                    "start_date": None,
                    "end_date": None,
                }
            )

        materialization_statement = (
            select(CachedAssetMaterialization)
            .join(
                DagsterCacheNamespace,
                col(CachedAssetMaterialization.namespace_id)
                == DagsterCacheNamespace.id,
            )
            .where(DagsterCacheNamespace.name == self.namespace)
            .order_by(col(CachedAssetMaterialization.timestamp).desc())
        )
        if query.date_mode == "update_time":
            materialization_statement = materialization_statement.where(
                col(CachedAssetMaterialization.timestamp) >= start_time,
                col(CachedAssetMaterialization.timestamp) <= end_time,
            )
        else:
            materialization_statement = materialization_statement.where(
                col(CachedAssetMaterialization.partition_start_time) <= end_time,
                col(CachedAssetMaterialization.partition_end_time) >= start_time,
            )
        if query.version_mode == "latest":
            ranked_materializations = (
                select(
                    CachedAssetMaterialization.id.label("materialization_id"),
                    func.row_number()
                    .over(
                        partition_by=(
                            col(CachedAssetMaterialization.asset_key),
                            col(CachedAssetMaterialization.partition),
                        ),
                        order_by=col(CachedAssetMaterialization.timestamp).desc(),
                    )
                    .label("version_rank"),
                )
                .join(
                    DagsterCacheNamespace,
                    col(CachedAssetMaterialization.namespace_id)
                    == DagsterCacheNamespace.id,
                )
                .where(DagsterCacheNamespace.name == self.namespace)
            )
            if query.date_mode == "update_time":
                ranked_materializations = ranked_materializations.where(
                    col(CachedAssetMaterialization.timestamp) >= start_time,
                    col(CachedAssetMaterialization.timestamp) <= end_time,
                )
            else:
                ranked_materializations = ranked_materializations.where(
                    col(CachedAssetMaterialization.partition_start_time) <= end_time,
                    col(CachedAssetMaterialization.partition_end_time) >= start_time,
                )
            ranked_subquery = ranked_materializations.subquery()
            latest_ids = select(ranked_subquery.c.materialization_id).where(
                ranked_subquery.c.version_rank == 1
            )
            materialization_statement = materialization_statement.where(
                col(CachedAssetMaterialization.id).in_(latest_ids)
            )
        with Session(self.engine) as session:
            materializations = list(session.exec(materialization_statement))

        for materialization in materializations:
            suffix = "_l0_raw"
            if not materialization.asset_key.endswith(suffix):
                logger.warning(
                    "Ignoring non-L0 materialization cached as L0: %s",
                    materialization.asset_key,
                )
                continue
            instrument = materialization.asset_key.removesuffix(suffix)
            records.append(
                {
                    "run_id": materialization.run_id,
                    "instrument": instrument,
                    "data_level": "l0",
                    "descriptor": "none",
                    "job_key": f"{instrument}_l0_none",
                    "job_name": None,
                    "partition": materialization.partition,
                    "partition_prefix": materialization.partition_prefix,
                    "partition_label": materialization.partition_label,
                    "repoint": materialization.repoint,
                    "start_time": materialization.partition_start_time,
                    "end_time": materialization.partition_end_time,
                    "status": "materialized",
                    "dagster_status": None,
                    "n_expected": 1,
                    "n_materialized": 1,
                    "n_skipped": 0,
                    "n_missing": 0,
                    "skip_info": None,
                    "skip_reason": None,
                    "missing_files": None,
                    "skipped_reason": None,
                    "creation_time": materialization.timestamp,
                    "update_time": materialization.timestamp,
                    "run_start_time": None,
                    "run_end_time": None,
                    "duration_seconds": None,
                    "parent_run_id": None,
                    "root_run_id": None,
                    "selected_assets": [],
                    "planned_assets": [],
                    "expected_assets": [[materialization.asset_key]],
                    "source_kind": "l0_asset_materialization",
                    "latest_eligible": True,
                    "tags": {},
                    "start_date": None,
                    "end_date": None,
                }
            )

        if not records:
            return pd.DataFrame()

        data_df = pd.DataFrame.from_records(records)
        data_df["repoint"] = data_df["repoint"].astype("Int64")
        for column in _DATETIME_COLUMNS:
            data_df[column] = pd.to_datetime(data_df[column], utc=True)
        data_df["start_date"] = data_df["start_time"].dt.normalize()
        data_df["end_date"] = data_df["end_time"].dt.normalize()
        for column in _COUNT_COLUMNS:
            data_df[column] = data_df[column].astype("Int64")
        return data_df
