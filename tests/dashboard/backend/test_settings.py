"""Tests for persisted dashboard settings."""

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from sds_utils.dashboard.backend.aggbase import AggPreset, AggSpec
from sds_utils.dashboard.backend.db.models import PersistentSettings, UserProfile
from sds_utils.dashboard.backend.projectionbase import (
    JobProjectionMode,
    ProjectionSpec,
)
from sds_utils.dashboard.backend.settings import DashboardSettings
from sds_utils.dashboard.backend.settings_repository import SettingsRepository


def test_dashboard_settings_have_safe_defaults() -> None:
    settings = DashboardSettings()

    assert settings.active_agg_preset == AggPreset.RAW
    assert settings.agg_specs == {
        preset: AggSpec(preset=preset) for preset in AggPreset
    }
    assert settings.projection == ProjectionSpec()
    assert settings.filtering == {}


def test_faulty_stored_field_falls_back_without_discarding_valid_fields() -> None:
    stored = PersistentSettings(
        user_id=1,
        settings={
            "active_agg_preset": "dates_summary",
            "agg_specs": {
                "dates_summary": {"preset": "not-a-preset"},
                "instruments_snapshot": {"preset": "instruments_snapshot"},
            },
            "filtering": {"status": {"excluded_values_regex": "failed"}},
        },
    )

    settings = stored.validated_settings()

    assert settings.active_agg_preset is AggPreset.DATES_SUMMARY
    assert settings.agg_specs[AggPreset.DATES_SUMMARY] == AggSpec(
        preset=AggPreset.DATES_SUMMARY
    )
    assert settings.agg_specs[AggPreset.INSTRUMENTS_SNAPSHOT] == AggSpec(
        preset=AggPreset.INSTRUMENTS_SNAPSHOT
    )
    assert settings.filtering == {
        "status": {"excluded_values_regex": "failed"}
    }


def test_settings_tables_persist_valid_json() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    settings = DashboardSettings(
        projection=ProjectionSpec(
            job_projection_mode=JobProjectionMode.LATEST_JOBS_ONLY
        ),
        filtering={"status": {"excluded_values_regex": "failed"}}
    )

    with Session(db_engine) as session:
        profile = UserProfile(username="dashboard-user")
        session.add(profile)
        session.flush()
        assert profile.id is not None
        session.add(
            PersistentSettings(
                user_id=profile.id,
                settings=settings.model_dump(mode="json"),
            )
        )
        session.commit()
        stored = session.get(PersistentSettings, profile.id)

    assert stored is not None
    assert stored.validated_settings() == settings


def test_settings_repository_loads_defaults_and_round_trips_settings() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    with Session(db_engine) as session:
        profile = UserProfile(username="repository-user")
        session.add(profile)
        session.commit()
        session.refresh(profile)
    assert profile.id is not None
    repository = SettingsRepository(db_engine)

    assert repository.load(profile.id) == DashboardSettings()

    settings = DashboardSettings(
        active_agg_preset=AggPreset.DATES_SUMMARY,
        agg_specs={
            AggPreset.DATES_SUMMARY: AggSpec(
                preset=AggPreset.DATES_SUMMARY,
                extra_columns=["instrument", "descriptor"],
            )
        },
        filtering={"instrument": {"excluded_values_regex": "hit"}},
    )
    repository.save(profile.id, settings)

    assert repository.load(profile.id) == settings


def test_legacy_aggregation_settings_fall_back_to_defaults() -> None:
    settings = DashboardSettings.from_stored(
        {
            "agg": {
                "preset": "data_levels_snapshot",
                "extra_columns": [],
            },
            "dates_summary_extra_columns": ["instrument", "descriptor"],
            "filtering": {"status": {"excluded_values_regex": "failed"}},
        }
    )

    assert settings == DashboardSettings()
