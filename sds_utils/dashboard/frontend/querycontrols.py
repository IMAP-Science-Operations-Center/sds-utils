"""Frontend controls for the dashboard query interval."""

import datetime
from collections.abc import Callable

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
    """Own the start and end datetime controls for a query specification."""

    # amount to offset specified end_date before constructing a query
    END_DELTA = datetime.timedelta(seconds=1)

    def __init__(
        self,
        query: QuerySpec,
        on_change: Callable[[QuerySpec], None],
    ) -> None:
        self.query = query.model_copy(deep=True)
        self._on_change = on_change

    def render(self) -> None:
        """Render start and end datetime dropdowns."""
        self.start = DateTimeDropdown(
            "Start time",
            self.query.start_time,
            self._set_start,
        ).build()
        self.end = DateTimeDropdown(
            "End time",
            self.query.end_time + self.END_DELTA,
            self._set_end,
        ).build()

    def _set_start(self, value: datetime.datetime) -> None:
        self._set_interval(value, self.query.end_time)

    def _set_end(self, value: datetime.datetime) -> None:
        self._set_interval(self.query.start_time, value)

    def _set_interval(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> None:
        end_time_updated = end_time - self.END_DELTA
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
        """Replace the current query and refresh both displayed datetimes."""
        self.query = query.model_copy(deep=True)
        self.start.set_value(self.query.start_time)
        self.end.set_value(self.query.end_time)
