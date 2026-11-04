"""Appearance controls for the dashboard's settings menu."""

from collections.abc import Callable

from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.settings import DashboardSettings
from .uielem import UIElem


class SettingsMenu(UIElem):
    """Keep the theme, menu switch, and saved settings synchronized."""

    def __init__(
        self,
        settings: DashboardSettings,
        sync_settings: Callable[[DashboardSettings], None],
    ) -> None:
        self.settings = settings
        self._sync_settings = sync_settings

    def render(self) -> None:
        """Render a settings button and its dark mode switch."""
        self.dark_mode = ui.dark_mode(
            self.settings.misc_settings.dark_mode, on_change=self._update_dark_mode
        )
        with (
            ui.button(icon="settings")
            .props("flat round aria-label=Settings")
            .classes("self-center ml-auto")
        ):
            with ui.menu():
                self.dark_mode_switch = ui.switch("Dark mode").bind_value(
                    self.dark_mode
                )

    def _update_dark_mode(self, event: ValueChangeEventArguments[bool | None]) -> None:
        self.settings.misc_settings.dark_mode = bool(event.value)
        self._sync_settings(self.settings)
