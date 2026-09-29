"""Ingest job-independent L0 raw-asset materializations from Dagster."""

import argparse
import asyncio
import datetime
import logging
import os
from collections.abc import AsyncIterator, Collection

from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select
from tqdm.auto import tqdm

from ..db import create_db_and_tables, engine
from ..db.models import (
    CachedAssetMaterialization,
    CachedDagsterRun,
    CacheIngestionState,
    DagsterCacheNamespace,
)
from ..jobkey import current_job_definitions, parse_job_key
from ..partitions import parse_partition
from .graphql_api import AssetKeyInput, DagsterGraphQLClient
from .graphql_api.l_0_materializations import (
    L0MaterializationsAssetNodeOrErrorAssetNode,
    L0MaterializationsAssetNodeOrErrorAssetNodeAssetMaterializations,
)
from .ingestionbase import (
    IngestionRange,
    as_utc,
    get_or_create_ingestion_state,
    plan_ingestion_ranges,
)

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100
DEFAULT_OVERLAP_BUFFER = datetime.timedelta(minutes=5)
L0_INGESTION_STREAM = "l0_materializations"


class L0IngestionError(RuntimeError):
    """Indicate that L0 materializations could not be ingested safely."""


def _event_datetime(timestamp: str) -> datetime.datetime:
    """Convert Dagster's millisecond timestamp string to UTC."""
    return datetime.datetime.fromtimestamp(float(timestamp) / 1000, tz=datetime.UTC)


def _before_timestamp(value: datetime.datetime) -> str:
    """Create an inclusive upper bound for an exclusive-before API argument."""
    return str(int(value.timestamp() * 1000) + 1)


async def _iter_materializations(
    client: DagsterGraphQLClient,
    *,
    asset_key: str,
    ingestion_range: IngestionRange,
    page_size: int,
) -> AsyncIterator[
    list[L0MaterializationsAssetNodeOrErrorAssetNodeAssetMaterializations]
]:
    before = _before_timestamp(ingestion_range.end)
    seen_bounds: set[str] = set()
    while True:
        response = (
            await client.l_0_materializations(
                asset_key=AssetKeyInput(path=[asset_key]),
                limit=page_size,
                before_timestamp_millis=before,
            )
        ).asset_node_or_error
        if not isinstance(response, L0MaterializationsAssetNodeOrErrorAssetNode):
            logger.warning("Dagster has no asset node for %s", asset_key)
            return
        events = response.asset_materializations
        if not events:
            return

        in_range = [
            event
            for event in events
            if ingestion_range.start
            <= _event_datetime(event.timestamp)
            <= ingestion_range.end
        ]
        if in_range:
            yield in_range

        oldest = min(_event_datetime(event.timestamp) for event in events)
        if oldest <= ingestion_range.start or len(events) < page_size:
            return
        next_bound = str(min(float(event.timestamp) for event in events))
        if next_bound in seen_bounds:
            raise L0IngestionError(
                f"Dagster returned a repeated materialization page for {asset_key}"
            )
        seen_bounds.add(next_bound)
        before = next_bound


def _known_instruments(session: Session, namespace_id: int) -> set[str]:
    instruments: set[str] = set()
    for job_key in session.exec(
        select(CachedDagsterRun.job_key).where(
            CachedDagsterRun.namespace_id == namespace_id,
            col(CachedDagsterRun.job_key).is_not(None),
        )
    ):
        instrument = parse_job_key(job_key).instrument
        if instrument is not None:
            instruments.add(instrument)
    return instruments


def _l0_asset_keys(instruments: Collection[str]) -> list[str]:
    definitions = current_job_definitions(instruments)
    l0_definitions = [
        definition
        for definition in definitions.values()
        if definition.data_level == "l0" and definition.descriptor == "none"
    ]
    defined_instruments = {definition.instrument for definition in l0_definitions}
    missing = set(instruments) - defined_instruments
    if missing:
        raise L0IngestionError(
            "Could not determine L0 partition types for: " + ", ".join(sorted(missing))
        )
    return sorted(
        next(iter(definition.expected_assets))[0] for definition in l0_definitions
    )


def _metadata(
    event: L0MaterializationsAssetNodeOrErrorAssetNodeAssetMaterializations,
) -> dict[str, object]:
    result: dict[str, object] = {}
    for entry in event.metadata_entries:
        text = getattr(entry, "text", None)
        if text is not None:
            result[entry.label] = text
    return result


def _store_page(
    session: Session,
    *,
    namespace_id: int,
    asset_key: str,
    events: list[L0MaterializationsAssetNodeOrErrorAssetNodeAssetMaterializations],
) -> int:
    candidates: list[CachedAssetMaterialization] = []
    for event in events:
        timestamp = _event_datetime(event.timestamp)
        partition = parse_partition(event.partition)
        event_key = CachedAssetMaterialization.build_event_key(
            run_id=event.run_id,
            asset_key=asset_key,
            partition=event.partition,
            timestamp=timestamp,
        )
        candidates.append(
            CachedAssetMaterialization(
                namespace_id=namespace_id,
                event_key=event_key,
                asset_key=asset_key,
                partition=event.partition,
                partition_prefix=partition.prefix,
                partition_label=partition.label,
                repoint=partition.repoint,
                partition_start_time=partition.start_time,
                partition_end_time=partition.end_time,
                run_id=event.run_id,
                timestamp=timestamp,
                event_metadata=_metadata(event),
                payload=event.model_dump(mode="json", by_alias=True),
            )
        )

    existing_keys = set(
        session.exec(
            select(CachedAssetMaterialization.event_key).where(
                CachedAssetMaterialization.namespace_id == namespace_id,
                col(CachedAssetMaterialization.event_key).in_(
                    [candidate.event_key for candidate in candidates]
                ),
            )
        )
    )
    new_events = [
        candidate
        for candidate in candidates
        if candidate.event_key not in existing_keys
    ]
    session.add_all(new_events)
    session.commit()
    return len(new_events)


async def ingest_l0_materializations(  # noqa: PLR0913
    start_datetime: datetime.datetime | None,
    end_datetime: datetime.datetime | None,
    *,
    namespace_name: str = "default",
    instruments: Collection[str] | None = None,
    api_key: str | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    overlap_buffer: datetime.timedelta = DEFAULT_OVERLAP_BUFFER,
    show_progress: bool = False,
    db_engine: Engine = engine,
    client: DagsterGraphQLClient | None = None,
) -> int:
    """Extend the cached L0-materialization event-time range."""
    start_datetime = as_utc(start_datetime, label="L0-ingestion")
    end_datetime = as_utc(end_datetime, label="L0-ingestion")
    if (
        start_datetime is not None
        and end_datetime is not None
        and start_datetime > end_datetime
    ):
        raise ValueError("start_datetime must not be later than end_datetime")
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    if overlap_buffer < datetime.timedelta(0):
        raise ValueError("overlap_buffer must not be negative")

    owns_client = client is None
    progress: tqdm[object] | None = None
    with Session(db_engine) as session:
        namespace = session.exec(
            select(DagsterCacheNamespace).where(
                DagsterCacheNamespace.name == namespace_name
            )
        ).one_or_none()
        if namespace is None or namespace.id is None:
            raise L0IngestionError(f"Cache namespace {namespace_name!r} does not exist")
        namespace_id = namespace.id
        graphql_url = namespace.graphql_url
        state = get_or_create_ingestion_state(
            session,
            namespace_id=namespace_id,
            stream=L0_INGESTION_STREAM,
        )
        ranges, new_start, new_end = plan_ingestion_ranges(
            state,
            requested_start=start_datetime,
            requested_end=end_datetime,
            overlap_buffer=overlap_buffer,
            error_type=L0IngestionError,
            stream_label="L0 ingestion",
        )
        selected_instruments = set(
            instruments or _known_instruments(session, namespace_id)
        )

    if client is None:
        client = DagsterGraphQLClient(
            url=graphql_url,
            headers={"x-dagster-api-key": api_key or os.environ["DAGSTER_API_KEY"]},
        )

    ingested = 0
    try:
        asset_keys = _l0_asset_keys(selected_instruments)
        progress = tqdm(
            total=None,
            desc="Ingesting L0 materializations",
            disable=not show_progress,
            unit="event",
        )
        with Session(db_engine) as session:
            for ingestion_range in ranges:
                for asset_key in asset_keys:
                    async for events in _iter_materializations(
                        client,
                        asset_key=asset_key,
                        ingestion_range=ingestion_range,
                        page_size=page_size,
                    ):
                        stored = _store_page(
                            session,
                            namespace_id=namespace_id,
                            asset_key=asset_key,
                            events=events,
                        )
                        ingested += stored
                        progress.update(stored)
            state = session.exec(
                select(CacheIngestionState).where(
                    CacheIngestionState.namespace_id == namespace_id,
                    CacheIngestionState.stream == L0_INGESTION_STREAM,
                )
            ).one()
            state.watermark_start = new_start
            state.watermark_end = new_end
            state.updated_at = datetime.datetime.now(datetime.UTC)
            session.add(state)
            session.commit()
    finally:
        if progress is not None:
            progress.close()
        if owns_client:
            await client.http_client.aclose()
    return ingested


def main() -> None:
    """Ingest L0 materializations from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="prod")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    start = datetime.datetime.strptime(args.start_date, "%Y%m%d").replace(
        tzinfo=datetime.UTC
    )
    end = datetime.datetime.strptime(args.end_date, "%Y%m%d").replace(
        tzinfo=datetime.UTC
    )
    create_db_and_tables()
    asyncio.run(
        ingest_l0_materializations(
            start,
            end,
            namespace_name=args.namespace,
            show_progress=True,
        )
    )


if __name__ == "__main__":
    main()
