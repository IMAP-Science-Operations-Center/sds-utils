"""NiceGUI entry point for the processing-status dashboard."""

import os

from nicegui import ui
from sqlmodel import Session, select

from ..backend.db import create_db_and_tables, engine
from ..backend.db.models import UserProfile
from ..backend.dbdata import DBDataSource
from ..backend.filteredtable import FilteredTable
from ..backend.settings_repository import SettingsRepository
from .filteredtableview import FilteredTableView
from .login import LoginView, dashboard_url, ensure_user_profile
from .uielem import UIElem


class TableApp(UIElem):
    """Construct the database-backed dashboard application."""

    def __init__(self, user_id: int, username: str) -> None:
        self.user_id = user_id
        self.username = username

    def render(self) -> None:
        """Build the dashboard's data source and table view."""
        data_source = DBDataSource(
            engine=engine,
            dagster_namespace="prod",
        )
        table = FilteredTable(data_source)
        settings_repository = SettingsRepository(engine)
        settings = settings_repository.load(self.user_id)
        self.table_view = FilteredTableView(
            table,
            dagster_url=os.environ["DAGSTER_BASE_URL"],
            settings=settings,
            sync_settings=lambda value: settings_repository.save(self.user_id, value),
        ).build()


@ui.page("/")
def login_page() -> None:
    """Render the username login page."""
    username = os.getenv("DASHBOARD_APP_USERNAME", "").strip()
    if username:
        ensure_user_profile(engine, username)
        ui.navigate.to(dashboard_url(username))
        return
    LoginView(engine).build()


@ui.page("/filteredtableview")
def filtered_table_page(username: str = "") -> None:
    """Render the dashboard for an existing user profile."""
    ui.query(".nicegui-content").classes(
        "h-screen flex flex-col flex-nowrap overflow-hidden"
    )
    with Session(engine) as session:
        profile = session.exec(
            select(UserProfile).where(UserProfile.username == username)
        ).first()
    if profile is None:
        ui.label("Unknown username. Please log in first.").classes("text-negative")
        ui.button("Return to login", on_click=lambda: ui.navigate.to("/"))
        return
    if profile.id is None:
        raise RuntimeError("Persisted user profile has no database ID")
    TableApp(profile.id, username).build()


def main() -> None:
    """Start the NiceGUI dashboard server."""
    create_db_and_tables()
    ui.run(
        title="IMAP Processing Status Dashboard",
        favicon="📈",
        reload=False,
        port=8893,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
