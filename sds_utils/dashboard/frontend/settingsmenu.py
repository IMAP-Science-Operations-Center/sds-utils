"""Appearance controls for the dashboard's settings menu."""

from nicegui import ui

from .uielem import UIElem


class SettingsMenu(UIElem):
    """Expose controls for the dashboard's appearance."""

    def __init__(
        self,
        dark_mode: ui.dark_mode,
    ) -> None:
        self.dark_mode = dark_mode

    def render(self) -> None:
        """Render a settings button and its dark mode switch."""
        with (
            ui.button(icon="settings")
            .props("flat round aria-label=Settings")
            .classes("self-center ml-auto")
        ):
            with ui.menu():
                self.dark_mode_switch = ui.switch("Dark mode").bind_value(
                    self.dark_mode
                )
