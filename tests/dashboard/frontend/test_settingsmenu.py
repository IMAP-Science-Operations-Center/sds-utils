"""Tests for the dashboard's theme controls and startup preferences."""

from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
from nicegui import ui

from sds_utils.dashboard.backend.settings import DashboardSettings
from sds_utils.dashboard.frontend import app as dashboard_app
from sds_utils.dashboard.frontend import filteredtableview
from sds_utils.dashboard.frontend.filteredtableview import FilteredTableView


def test_theme_switch_and_programmatic_changes_save_preferences(monkeypatch) -> None:
    saved = []
    settings = DashboardSettings(misc_settings={"dark_mode": True})
    for name in (
        "FilterControls",
        "PaneNavigator",
        "ProjectionControls",
        "QueryControls",
        "StatusSummary",
    ):
        monkeypatch.setattr(filteredtableview, name, MagicMock())
    view = FilteredTableView(
        Mock(),
        dagster_url="https://example.com",
        settings=settings,
        sync_settings=lambda value: saved.append(value.model_dump(mode="json")),
    )
    view.update_query = Mock()
    container = ui.column()
    try:
        with container:
            view.build()
        menu = view.settings_menu

        assert view.dark_mode.value is True
        assert menu.dark_mode_switch.value is True

        menu.dark_mode_switch.value = False
        assert view.dark_mode.value is False
        assert saved[-1]["misc_settings"]["dark_mode"] is False

        view.dark_mode.enable()
        assert menu.dark_mode_switch.value is True
        assert saved[-1]["misc_settings"]["dark_mode"] is True
        assert DashboardSettings.from_stored(saved[-1]).misc_settings.dark_mode is True
    finally:
        container.delete()


@pytest.mark.parametrize(
    ("env_value", "saved_dark", "expected"),
    [
        (None, True, True),
        (None, False, False),
        ("1", False, True),
        (" 1 ", False, True),
        ("0", True, False),
        ("", True, False),
    ],
)
def test_startup_theme_only_overrides_saved_preference_when_env_is_set(
    monkeypatch, env_value, saved_dark, expected
) -> None:
    settings = DashboardSettings(misc_settings={"dark_mode": saved_dark})
    repository = Mock()
    repository.load.return_value = settings
    monkeypatch.setattr(dashboard_app, "SettingsRepository", Mock(return_value=repository))
    monkeypatch.setattr(dashboard_app, "DBDataSource", Mock())
    monkeypatch.setattr(dashboard_app, "FilteredTable", Mock())
    monkeypatch.setattr(dashboard_app, "FilteredTableView", Mock())
    monkeypatch.setenv("DAGSTER_BASE_URL", "https://example.com")
    if env_value is None:
        monkeypatch.delenv("DASHBOARD_APP_DARK_MODE", raising=False)
    else:
        monkeypatch.setenv("DASHBOARD_APP_DARK_MODE", env_value)

    dashboard_app.TableApp(1, "test-user").build()

    assert settings.misc_settings.dark_mode is expected
    repository.save.assert_called_once_with(1, settings)


def test_table_refresh_preserves_theme_and_shared_settings() -> None:
    settings = DashboardSettings(misc_settings={"dark_mode": True})
    save = Mock()
    view = FilteredTableView(
        Mock(), dagster_url="https://example.com", settings=settings, sync_settings=save
    )
    view.filters = Mock()
    view.filters.arguments.return_value = {}
    view.navigator = Mock()
    view.navigator.agg_spec = settings.agg_specs[settings.active_agg_preset]
    view.navigator.agg_specs = settings.agg_specs
    view.projection = Mock(spec=settings.projection)
    view.projection.spec = settings.projection
    view.query_controls = Mock()
    view.query_controls.query = settings.query
    view.renderer = Mock()
    view.renderer.display_data.return_value = pd.DataFrame()
    view.standalone_filters_container = MagicMock()
    view.table_container = MagicMock()
    view.status_summary = Mock()

    view.update_table()

    save.assert_called_once_with(settings)
    assert view.settings is settings
    assert settings.misc_settings.dark_mode is True
