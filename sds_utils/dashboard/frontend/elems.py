"""Small reusable NiceGUI elements for the dashboard frontend."""

from collections.abc import Callable
from typing import Any, Self

from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from .uielem import UIElem


def help_icon(text: str, *, classes: str = "") -> None:
    """Render a subtle help icon with a hover tooltip."""
    icon_classes = f"text-grey-6 cursor-help text-sm {classes}".strip()
    with ui.icon("help_outline").classes(icon_classes):
        ui.tooltip(text).classes("max-w-96 whitespace-pre-wrap text-sm")


class Select(UIElem):
    """Render a NiceGUI select with optional embedded help text."""

    def __init__(
        self,
        options: list[Any] | dict[Any, Any],
        *,
        value: Any,
        label: str,
        on_change: Callable[[ValueChangeEventArguments[Any]], Any] | None = None,
        help: str | None = None,
    ) -> None:
        self.options = options
        self.initial_value = value
        self.label = label
        self.on_change = on_change
        self.help = help

    def render(self) -> None:
        """Render the select and append its help icon when configured."""
        self.element = ui.select(
            self.options,
            value=self.initial_value,
            label=self.label,
            on_change=self.on_change,
        )
        if self.help is not None:
            with self.element.add_slot("append"):
                help_icon(self.help)

    @property
    def value(self) -> Any:
        """Return the currently selected value."""
        return self.element.value

    @value.setter
    def value(self, value: Any) -> None:
        self.element.value = value

    def classes(self, classes: str) -> Self:
        """Add css classes to the primary element."""
        self.element.classes(classes)
        return self
