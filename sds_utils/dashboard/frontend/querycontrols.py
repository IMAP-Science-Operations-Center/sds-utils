"""Frontend controls for the dashboard query interval."""

import datetime
from collections.abc import Callable
from typing import Any

from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.data import QuerySpec
from .uielem import UIElem

_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class DateTimeDropdown(UIElem):
    """Edit one datetime through consecutive date and time pickers."""

    def __init__(
        self,
        label: str,
        value: datetime.datetime,
        on_change: Callable[[datetime.datetime], None],
    ) -> None:
        self.label = label
        self.value = value
        self._on_change = on_change

    def render(self) -> None:
        """Render the idle button and its two-step picker menu."""
        self.button = ui.button(icon="event").props("outline no-caps")
        self._update_button_text()
        with self.button:
            self.menu = ui.menu()
            self.menu.on_value_change(self._menu_visibility_changed)
            with self.menu:
                self.date_step = ui.column().classes("items-end gap-2")
                with self.date_step:
                    self.date_picker = ui.date(
                        value=self.value.strftime("%Y-%m-%d"),
                    )
                    ui.button("Next", on_click=self._show_time).props("no-caps")

                self.time_step = ui.column().classes("items-end gap-2")
                with self.time_step:
                    self.time_picker = ui.time(
                        value=self.value.strftime("%H:%M:%S"),
                        mask="HH:mm:ss",
                    ).props("with-seconds format24h")
                    with ui.row().classes("justify-end gap-2"):
                        ui.button("Back", on_click=self._show_date).props(
                            "flat no-caps"
                        )
                        ui.button("Apply", on_click=self._apply).props("no-caps")
                self.time_step.set_visibility(False)

    def _menu_visibility_changed(
        self,
        event: ValueChangeEventArguments[bool],
    ) -> None:
        if event.value:
            self.date_picker.value = self.value.strftime("%Y-%m-%d")
            self.time_picker.value = self.value.strftime("%H:%M:%S")
            self._show_date()

    def _show_date(self) -> None:
        self.date_step.set_visibility(True)
        self.time_step.set_visibility(False)

    def _show_time(self) -> None:
        self.date_step.set_visibility(False)
        self.time_step.set_visibility(True)

    def _apply(self) -> None:
        date_value = self.date_picker.value
        time_value = self.time_picker.value
        if not isinstance(date_value, str) or not isinstance(time_value, str):
            ui.notify("Choose both a date and time", type="warning")
            return
        value = datetime.datetime.combine(
            datetime.date.fromisoformat(date_value),
            datetime.time.fromisoformat(time_value),
        ).replace(tzinfo=self.value.tzinfo)
        self._on_change(value)
        self.menu.close()

    def set_value(self, value: datetime.datetime) -> None:
        """Update the committed datetime displayed by the idle control."""
        self.value = value
        self._update_button_text()

    def _update_button_text(self) -> None:
        self.button.text = f"{self.label}: {self.value.strftime(_DATETIME_FORMAT)}"


class QueryControls(UIElem):
    """Own the editable controls for a query specification."""

    def __init__(
        self,
        query: QuerySpec,
        on_change: Callable[[QuerySpec], None],
    ) -> None:
        self.query = query.model_copy(deep=True)
        self._on_change = on_change

    def render(self) -> None:
        """Render datetime, date-mode, and version-mode controls."""
        self.start = DateTimeDropdown(
            "Start time",
            self.query.start_time,
            self._set_start,
        ).build()
        self.end = DateTimeDropdown(
            "End time",
            self.query.end_time,
            self._set_end,
        ).build()
        self.date_mode = ui.select(
            {
                "update_time": "Update time",
                "partition": "Partition overlap",
            },
            value=self.query.date_mode,
            label="Date selection",
            on_change=self._set_date_mode,
        ).classes("min-w-44")
        self.version_mode = ui.select(
            {
                "latest": "Latest runs",
                "all": "All runs",
            },
            value=self.query.version_mode,
            label="Version selection",
            on_change=self._set_version_mode,
        ).classes("min-w-44")

    def _set_start(self, value: datetime.datetime) -> None:
        self._set_interval(value, self.query.end_time)

    def _set_end(self, value: datetime.datetime) -> None:
        self._set_interval(self.query.start_time, value)

    def _set_date_mode(self, event: ValueChangeEventArguments[Any]) -> None:
        if event.value not in {"update_time", "partition"}:
            return
        self.query = self.query.model_copy(update={"date_mode": event.value})
        self._on_change(self.query)

    def _set_version_mode(self, event: ValueChangeEventArguments[Any]) -> None:
        if event.value not in {"latest", "all"}:
            return
        self.query = self.query.model_copy(update={"version_mode": event.value})
        self._on_change(self.query)

    def _set_interval(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> None:
        end_time_updated = end_time
        if start_time > end_time_updated:
            ui.notify("Start time must not be later than end time", type="negative")
            return
        self.query = self.query.model_copy(
            update={"start_time": start_time, "end_time": end_time_updated}
        )
        self.start.set_value(start_time)
        self.end.set_value(end_time)
        self._on_change(self.query)

    def set_query(self, query: QuerySpec) -> None:
        """Replace the current query and refresh all displayed controls."""
        self.query = query.model_copy(deep=True)
        self.start.set_value(self.query.start_time)
        self.end.set_value(self.query.end_time)
        self.date_mode.value = self.query.date_mode
        self.version_mode.value = self.query.version_mode
