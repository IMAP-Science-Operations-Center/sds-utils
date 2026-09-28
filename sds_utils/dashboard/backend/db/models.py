"""SQLModel tables for cached Dagster job-run data."""

import datetime
import hashlib
import json

from sqlalchemy import JSON, Column, Index, UniqueConstraint
from sqlmodel import Field, SQLModel

from ..settings import DashboardSettings


def _utc_now() -> datetime.datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.datetime.now(datetime.UTC)


class UserProfile(SQLModel, table=True):
    """Identify a dashboard user with persistent preferences."""

    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)


class PersistentSettings(SQLModel, table=True):
    """Store one JSON settings document for a dashboard user."""

    user_id: int = Field(
        foreign_key="userprofile.id",
        primary_key=True,
        unique=True,
    )
    settings: dict[str, object] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )

    def validated_settings(self) -> DashboardSettings:
        """Return typed settings, falling back to defaults for invalid JSON."""
        return DashboardSettings.from_stored(self.settings)


class DagsterCacheNamespace(SQLModel, table=True):
    """Identify cached data belonging to one logical Dagster deployment."""

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    graphql_url: str
    run_update_watermark_start: datetime.datetime | None = None
    run_update_watermark_end: datetime.datetime | None = None
    created_at: datetime.datetime = Field(default_factory=_utc_now)
    updated_at: datetime.datetime = Field(default_factory=_utc_now)


class CacheIngestionState(SQLModel, table=True):
    """Track an independently watermarked ingestion stream."""

    __table_args__ = (UniqueConstraint("namespace_id", "stream"),)

    id: int | None = Field(default=None, primary_key=True)
    namespace_id: int = Field(
        foreign_key="dagstercachenamespace.id",
        index=True,
    )
    stream: str = Field(index=True)
    watermark_start: datetime.datetime | None = None
    watermark_end: datetime.datetime | None = None
    updated_at: datetime.datetime = Field(default_factory=_utc_now)


class CachedDagsterRun(SQLModel, table=True):
    """Cache run-level facts reported by Dagster and event-fetch state."""

    __table_args__ = (
        UniqueConstraint("namespace_id", "run_id"),
        Index(
            "ix_cached_dagster_run_job_partition_creation",
            "namespace_id",
            "job_name",
            "partition",
            "creation_time",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    namespace_id: int = Field(
        foreign_key="dagstercachenamespace.id",
        index=True,
    )
    run_id: str = Field(index=True)
    job_name: str = Field(index=True)
    job_key: str | None = Field(index=True, default=None)
    partition: str | None = Field(default=None, index=True)
    partition_prefix: str | None = None
    partition_label: str | None = None
    repoint: int | None = None
    partition_start_time: datetime.datetime | None = Field(default=None, index=True)
    partition_end_time: datetime.datetime | None = Field(default=None, index=True)
    dagster_status: str = Field(index=True)
    creation_time: datetime.datetime = Field(index=True)
    update_time: datetime.datetime | None = Field(default=None, index=True)
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None
    parent_run_id: str | None = Field(default=None, index=True)
    root_run_id: str | None = Field(default=None, index=True)
    selected_assets: list[list[str]] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False),
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )


class DerivedJobRun(SQLModel, table=True):
    """Store dashboard interpretations derived from cached run facts and events."""

    __table_args__ = (UniqueConstraint("cached_run_id"),)

    id: int | None = Field(default=None, primary_key=True)
    cached_run_id: int = Field(
        foreign_key="cacheddagsterrun.id",
        index=True,
    )
    dashboard_status: str = Field(index=True)
    run_completed: bool = Field(default=False, index=True)
    planned_assets: list[list[str]] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False),
    )
    expected_assets: list[list[str]] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False),
    )
    n_expected: int | None = None
    n_materialized: int | None = None
    n_skipped: int | None = None
    n_missing: int | None = None
    skip_info: dict[str, str] | None = Field(
        default=None,
        sa_column=Column(JSON, nullable=True),
    )


class CachedRunEvent(SQLModel, table=True):
    """Cache selected Dagster events with common fields promoted for queries."""

    __table_args__ = (
        Index(
            "ix_cached_run_event_run",
            "namespace_id",
            "run_id",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    namespace_id: int = Field(
        foreign_key="dagstercachenamespace.id",
        index=True,
    )
    event_key: str = Field(index=True)
    run_id: str = Field(index=True)
    event_type: str = Field(index=True)
    timestamp: datetime.datetime = Field(index=True)
    step_key: str | None = None
    asset_key: str | None = Field(default=None, index=True)
    partition: str | None = Field(default=None, index=True)
    event_metadata: dict[str, object] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )
    payload: dict[str, object] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )

    @classmethod
    def build_event_key(  # noqa: PLR0913
        cls,
        *,
        run_id: str,
        event_type: str,
        timestamp: datetime.datetime,
        step_key: str | None = None,
        asset_key: str | None = None,
        partition: str | None = None,
    ) -> str:
        """Build a stable key from the identifying fields of a Dagster event."""
        key_fields = json.dumps(
            (
                run_id,
                event_type,
                timestamp.isoformat(),
                step_key,
                asset_key,
                partition,
            ),
            separators=(",", ":"),
        )
        return hashlib.sha256(key_fields.encode()).hexdigest()


class CachedAssetMaterialization(SQLModel, table=True):
    """Cache a job-independent Dagster asset materialization event."""

    __table_args__ = (
        UniqueConstraint("namespace_id", "event_key"),
        Index(
            "ix_cached_asset_materialization_asset_partition",
            "namespace_id",
            "asset_key",
            "partition",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    namespace_id: int = Field(
        foreign_key="dagstercachenamespace.id",
        index=True,
    )
    event_key: str = Field(index=True)
    asset_key: str = Field(index=True)
    partition: str | None = Field(default=None, index=True)
    partition_prefix: str | None = None
    partition_label: str | None = None
    repoint: int | None = None
    partition_start_time: datetime.datetime | None = Field(default=None, index=True)
    partition_end_time: datetime.datetime | None = Field(default=None, index=True)
    run_id: str = Field(index=True)
    timestamp: datetime.datetime = Field(index=True)
    event_metadata: dict[str, object] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )
    payload: dict[str, object] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )

    @classmethod
    def build_event_key(
        cls,
        *,
        run_id: str,
        asset_key: str,
        partition: str | None,
        timestamp: datetime.datetime,
    ) -> str:
        """Build a stable identity for one materialization event."""
        key_fields = json.dumps(
            (run_id, asset_key, partition, timestamp.isoformat()),
            separators=(",", ":"),
        )
        return hashlib.sha256(key_fields.encode()).hexdigest()
