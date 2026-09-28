"""Ingest Dagster run facts into the dashboard cache."""

import argparse
import asyncio
import datetime
import math
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass

from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select
from tqdm.auto import tqdm

from ..db import create_db_and_tables, engine
from ..db.models import CachedDagsterRun, DagsterCacheNamespace
from ..jobkey import derive_job_key
from ..partitions import parse_partition
from .graphql_api import DagsterGraphQLClient, RunsFilter
from .graphql_api.run_count import (
    RunCountRunsOrErrorPythonError,
    RunCountRunsOrErrorRuns,
)
from .graphql_api.runs_for_ingestion import (
    RunsForIngestionRunsOrErrorPythonError,
    RunsForIngestionRunsOrErrorRuns,
    RunsForIngestionRunsOrErrorRunsResults,
)
from .l0_ingestion import ingest_l0_materializations

DAGSTER_PARTITION_TAG = "dagster/partition"
DEFAULT_PAGE_SIZE = 100
DEFAULT_OVERLAP_BUFFER = datetime.timedelta(minutes=5)


class RunIngestionError(RuntimeError):
    """Indicate that Dagster runs could not be ingested safely."""


@dataclass(frozen=True)
class _IngestionRange:
    start: datetime.datetime
    end: datetime.datetime


def _as_utc(value: datetime.datetime | None) -> datetime.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "Run-ingestion watermarks must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(datetime.UTC)


def _database_datetime_as_utc(
    value: datetime.datetime | None,
) -> datetime.datetime | None:
    """Restore UTC lost when SQLite reads a timezone-aware datetime."""
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC)


def _plan_ingestion_ranges(
    namespace: DagsterCacheNamespace,
    *,
    requested_start: datetime.datetime | None,
    requested_end: datetime.datetime | None,
    overlap_buffer: datetime.timedelta,
    now: datetime.datetime | None = None,
) -> tuple[list[_IngestionRange], datetime.datetime, datetime.datetime]:
    current_start = _database_datetime_as_utc(namespace.run_update_watermark_start)
    current_end = _database_datetime_as_utc(namespace.run_update_watermark_end)

    if now is None:
        now = datetime.datetime.now(datetime.UTC)
    if requested_end is not None:
        requested_end = min(requested_end, now)

    if current_start is None and current_end is None:
        if requested_start is None or requested_end is None:
            msg = "Both watermarks are required to initialize a cache namespace"
            raise RunIngestionError(msg)
        return (
            [_IngestionRange(requested_start, requested_end)],
            requested_start,
            requested_end,
        )
    if current_start is None or current_end is None:
        msg = "Cache namespace has only one run-update watermark"
        raise RunIngestionError(msg)

    desired_start = requested_start or current_start
    desired_end = requested_end or current_end
    if desired_end < current_start or desired_start > current_end:
        msg = (
            "Requested run-update range does not overlap the cache namespace's "
            "existing range"
        )
        raise RunIngestionError(msg)

    ranges: list[_IngestionRange] = []
    new_start = current_start
    new_end = current_end
    if desired_start < current_start:
        ranges.append(
            _IngestionRange(
                desired_start,
                min(current_start + overlap_buffer, current_end),
            )
        )
        new_start = desired_start
    if desired_end > current_end:
        ranges.append(
            _IngestionRange(
                max(current_end - overlap_buffer, current_start),
                desired_end,
            )
        )
        new_end = desired_end

    return ranges, new_start, new_end


def _inclusive_after(value: datetime.datetime | None) -> float | None:
    if value is None:
        return None
    return math.nextafter(value.timestamp(), -math.inf)


def _inclusive_before(value: datetime.datetime | None) -> float | None:
    if value is None:
        return None
    return math.nextafter(value.timestamp(), math.inf)


def _timestamp(value: float | None) -> datetime.datetime | None:
    if value is None:
        return None
    return datetime.datetime.fromtimestamp(value, tz=datetime.UTC)


async def _count_runs(
    client: DagsterGraphQLClient,
    ingestion_range: _IngestionRange,
) -> int:
    response = (
        await client.run_count(
            filter_=RunsFilter(
                updatedAfter=_inclusive_after(ingestion_range.start),
                updatedBefore=_inclusive_before(ingestion_range.end),
            )
        )
    ).runs_or_error
    if isinstance(response, RunCountRunsOrErrorPythonError):
        raise RunIngestionError(response.message)
    if not isinstance(response, RunCountRunsOrErrorRuns):
        msg = f"Dagster rejected the run-count filter: {response.typename__}"
        raise RunIngestionError(msg)
    return response.count


async def _iter_runs(
    client: DagsterGraphQLClient,
    *,
    start_datetime: datetime.datetime | None,
    end_datetime: datetime.datetime | None,
    page_size: int,
) -> AsyncIterator[list[RunsForIngestionRunsOrErrorRunsResults]]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    run_filter = RunsFilter(
        updatedAfter=_inclusive_after(start_datetime),
        updatedBefore=_inclusive_before(end_datetime),
    )

    while True:
        response = (
            await client.runs_for_ingestion(
                filter_=run_filter,
                cursor=cursor,
                limit=page_size,
            )
        ).runs_or_error
        if isinstance(response, RunsForIngestionRunsOrErrorPythonError):
            raise RunIngestionError(response.message)
        if not isinstance(response, RunsForIngestionRunsOrErrorRuns):
            msg = f"Dagster rejected the run filter: {response.typename__}"
            raise RunIngestionError(msg)

        runs = response.results
        if not runs:
            return
        yield runs
        if len(runs) < page_size:
            return

        next_cursor = runs[-1].run_id
        if next_cursor in seen_cursors:
            msg = f"Dagster returned a repeated run cursor: {next_cursor}"
            raise RunIngestionError(msg)
        seen_cursors.add(next_cursor)
        cursor = next_cursor


def _get_or_create_namespace(
    session: Session,
    *,
    name: str,
    graphql_url: str,
) -> DagsterCacheNamespace:
    namespace = session.exec(
        select(DagsterCacheNamespace).where(DagsterCacheNamespace.name == name)
    ).one_or_none()
    if namespace is not None:
        if namespace.graphql_url != graphql_url:
            msg = (
                f"Cache namespace {name!r} belongs to {namespace.graphql_url!r}, "
                f"not {graphql_url!r}"
            )
            raise RunIngestionError(msg)
        return namespace

    namespace = DagsterCacheNamespace(name=name, graphql_url=graphql_url)
    session.add(namespace)
    session.commit()
    session.refresh(namespace)
    return namespace


def _cache_page(
    session: Session,
    *,
    namespace_id: int,
    runs: list[RunsForIngestionRunsOrErrorRunsResults],
) -> None:
    run_ids = [run.run_id for run in runs]
    cached_by_run_id = {
        cached.run_id: cached
        for cached in session.exec(
            select(CachedDagsterRun).where(
                CachedDagsterRun.namespace_id == namespace_id,
                col(CachedDagsterRun.run_id).in_(run_ids),
            )
        )
    }

    for run in runs:
        tags = {tag.key: tag.value for tag in run.tags}
        cached = cached_by_run_id.get(run.run_id)
        if cached is None:
            cached = CachedDagsterRun(
                namespace_id=namespace_id,
                run_id=run.run_id,
                job_name=run.job_name,
                dagster_status=run.status.value,
                creation_time=_timestamp(run.creation_time),
            )
            session.add(cached)

        cached.job_name = run.job_name
        cached.partition = tags.get(DAGSTER_PARTITION_TAG)
        partition = parse_partition(cached.partition)
        cached.partition_prefix = partition.prefix
        cached.partition_label = partition.label
        cached.repoint = partition.repoint
        cached.partition_start_time = partition.start_time
        cached.partition_end_time = partition.end_time
        cached.dagster_status = run.status.value
        cached.creation_time = datetime.datetime.fromtimestamp(
            run.creation_time,
            tz=datetime.UTC,
        )
        cached.update_time = _timestamp(run.update_time)
        cached.start_time = _timestamp(run.start_time)
        cached.end_time = _timestamp(run.end_time)
        cached.parent_run_id = run.parent_run_id
        cached.root_run_id = run.root_run_id
        selected_assets = [asset.path for asset in (run.asset_selection or [])]
        cached.selected_assets = selected_assets
        cached.job_key = derive_job_key(run.job_name, selected_assets).job_key
        cached.tags = tags

    session.commit()


async def ingest_runs(  # noqa: PLR0912, PLR0913
    start_datetime: datetime.datetime | None,
    end_datetime: datetime.datetime | None,
    *,
    namespace_name: str = "default",
    graphql_url: str | None = None,
    api_key: str | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    overlap_buffer: datetime.timedelta = DEFAULT_OVERLAP_BUFFER,
    show_progress: bool = False,
    db_engine: Engine = engine,
    client: DagsterGraphQLClient | None = None,
) -> int:
    """Extend a namespace's cached update-time range in either direction.

    For an existing namespace, ``None`` means that side should remain unchanged.
    A new namespace requires both bounds. Previously covered time is skipped apart
    from ``overlap_buffer`` at each boundary. Watermarks change only after every
    requested extension has been fetched and cached successfully.

    Returns
    -------
    int
        Number of runs returned by Dagster and cached.
    """
    start_datetime = _as_utc(start_datetime)
    end_datetime = _as_utc(end_datetime)
    if (
        start_datetime is not None
        and end_datetime is not None
        and start_datetime > end_datetime
    ):
        msg = "start_datetime must not be later than end_datetime"
        raise ValueError(msg)
    if page_size <= 0:
        msg = "page_size must be positive"
        raise ValueError(msg)
    if overlap_buffer < datetime.timedelta(0):
        msg = "overlap_buffer must not be negative"
        raise ValueError(msg)

    graphql_url = (graphql_url or os.environ["DAGSTER_BASE_URL"]).rstrip("/")
    if not graphql_url.endswith("/graphql"):
        graphql_url = f"{graphql_url}/graphql"

    owns_client = client is None
    if client is None:
        api_key = api_key or os.environ["DAGSTER_API_KEY"]
        client = DagsterGraphQLClient(
            url=graphql_url,
            headers={"x-dagster-api-key": api_key},
        )

    ingested_count = 0
    progress: tqdm[object] | None = None
    try:
        with Session(db_engine) as session:
            namespace = _get_or_create_namespace(
                session,
                name=namespace_name,
                graphql_url=graphql_url,
            )
            if namespace.id is None:
                msg = "Cached Dagster namespace has no database ID"
                raise RunIngestionError(msg)

            ranges, new_start, new_end = _plan_ingestion_ranges(
                namespace,
                requested_start=start_datetime,
                requested_end=end_datetime,
                overlap_buffer=overlap_buffer,
            )
            if ranges:
                total = None
                if show_progress:
                    total = sum(
                        [
                            await _count_runs(client, ingestion_range)
                            for ingestion_range in ranges
                        ]
                    )
                progress = tqdm(
                    total=total,
                    desc="Ingesting runs",
                    disable=not show_progress,
                    unit="run",
                )
            for ingestion_range in ranges:
                async for runs in _iter_runs(
                    client,
                    start_datetime=ingestion_range.start,
                    end_datetime=ingestion_range.end,
                    page_size=page_size,
                ):
                    _cache_page(session, namespace_id=namespace.id, runs=runs)
                    ingested_count += len(runs)
                    if progress is not None:
                        progress.update(len(runs))

            namespace.run_update_watermark_start = new_start
            namespace.run_update_watermark_end = new_end
            namespace.updated_at = datetime.datetime.now(datetime.UTC)
            session.add(namespace)
            session.commit()
    finally:
        if progress is not None:
            progress.close()
        if owns_client:
            await client.http_client.aclose()

    return ingested_count


def main() -> None:
    """Ingest Dagster runs and job-independent L0 materializations."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="prod")
    parser.add_argument("--start-date", default="20260914")
    parser.add_argument("--end-date", default="20260922")
    args = parser.parse_args()
    start_datetime = datetime.datetime.strptime(args.start_date, "%Y%m%d").replace(
        tzinfo=datetime.UTC
    )
    end_datetime = datetime.datetime.strptime(args.end_date, "%Y%m%d").replace(
        tzinfo=datetime.UTC
    )
    create_db_and_tables()

    async def ingest_all() -> tuple[int, int]:
        run_count = await ingest_runs(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            namespace_name=args.namespace,
            show_progress=True,
        )
        l0_count = await ingest_l0_materializations(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            namespace_name=args.namespace,
            show_progress=True,
        )
        return run_count, l0_count

    run_count, l0_count = asyncio.run(ingest_all())
    print(f"Processed {run_count} runs and {l0_count} L0 materializations")


if __name__ == "__main__":
    main()
