"""Shared watermark planning for independently cached ingestion streams."""

import datetime
from dataclasses import dataclass
from typing import TypeVar

from sqlmodel import Session, select

from ..db.models import CacheIngestionState

IngestionErrorT = TypeVar("IngestionErrorT", bound=Exception)


@dataclass(frozen=True)
class IngestionRange:
    """One inclusive time range that remains to be ingested."""

    start: datetime.datetime
    end: datetime.datetime


def as_utc(value: datetime.datetime | None, *, label: str) -> datetime.datetime | None:
    """Validate and normalize an optional ingestion boundary as UTC."""
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} watermarks must be timezone-aware")
    return value.astimezone(datetime.UTC)


def database_datetime_as_utc(
    value: datetime.datetime | None,
) -> datetime.datetime | None:
    """Restore UTC lost when SQLite reads a timezone-aware datetime."""
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC)


def get_or_create_ingestion_state(
    session: Session,
    *,
    namespace_id: int,
    stream: str,
) -> CacheIngestionState:
    """Return the persistent watermark state for one namespace stream."""
    state = session.exec(
        select(CacheIngestionState).where(
            CacheIngestionState.namespace_id == namespace_id,
            CacheIngestionState.stream == stream,
        )
    ).one_or_none()
    if state is None:
        state = CacheIngestionState(namespace_id=namespace_id, stream=stream)
        session.add(state)
        session.commit()
        session.refresh(state)
    return state


def plan_ingestion_ranges(  # noqa: PLR0913
    state: CacheIngestionState,
    *,
    requested_start: datetime.datetime | None,
    requested_end: datetime.datetime | None,
    overlap_buffer: datetime.timedelta,
    error_type: type[IngestionErrorT],
    stream_label: str,
    now: datetime.datetime | None = None,
) -> tuple[list[IngestionRange], datetime.datetime, datetime.datetime]:
    """Plan uncovered extensions and the resulting stream watermarks."""
    current_start = database_datetime_as_utc(state.watermark_start)
    current_end = database_datetime_as_utc(state.watermark_end)
    now = now or datetime.datetime.now(datetime.UTC)
    if requested_end is not None:
        requested_end = min(requested_end, now)

    if current_start is None and current_end is None:
        if requested_start is None or requested_end is None:
            raise error_type(
                f"Both watermarks are required to initialize {stream_label}"
            )
        return (
            [IngestionRange(requested_start, requested_end)],
            requested_start,
            requested_end,
        )
    if current_start is None or current_end is None:
        raise error_type(f"{stream_label} has only one watermark")

    desired_start = requested_start or current_start
    desired_end = requested_end or current_end
    if desired_end < current_start or desired_start > current_end:
        raise error_type(
            f"Requested {stream_label} range does not overlap the existing range"
        )

    ranges: list[IngestionRange] = []
    new_start = current_start
    new_end = current_end
    if desired_start < current_start:
        ranges.append(
            IngestionRange(
                desired_start,
                min(current_start + overlap_buffer, current_end),
            )
        )
        new_start = desired_start
    if desired_end > current_end:
        ranges.append(
            IngestionRange(
                max(current_end - overlap_buffer, current_start),
                desired_end,
            )
        )
        new_end = desired_end
    return ranges, new_start, new_end
