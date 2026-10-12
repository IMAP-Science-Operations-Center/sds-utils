"""Aggregation-pane navigation and temporary drill-down filters."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.aggbase import DATES_SUMMARY_EXTRA_COLUMNS, AggPreset, AggSpec
from ..backend.filtersbase import FilterArguments
from .uielem import UIElem

_DRILLDOWNS = {
    AggPreset.INSTRUMENTS_SNAPSHOT: (
        "instrument",
        AggPreset.DATA_LEVELS_SNAPSHOT,
    ),
    AggPreset.DATA_LEVELS_SNAPSHOT: (
        "data_level",
        AggPreset.DATES_SUMMARY,
    ),
}


@dataclass(frozen=True)
class PaneState:
    """Restorable aggregation and temporary-filter state."""

    agg_spec: AggSpec
    temporary_filters: dict[str, "TemporaryFilter"]


@dataclass(frozen=True)
class TemporaryFilter:
    """Backend filter arguments and their human-readable selected value."""

    arguments: dict[str, Any]
    display_value: str


class PaneNavigator(UIElem):
    """Manage summary panes, drill-down history, and temporary filters."""

    def __init__(
        self,
        active_preset: AggPreset,
        agg_specs: dict[AggPreset, AggSpec],
        on_change: Callable[[], None],
    ) -> None:
        self._agg_specs = {
            preset: agg_specs.get(preset, AggSpec(preset=preset)).model_copy(deep=True)
            for preset in AggPreset
        }
        self.agg_spec = self._agg_specs[active_preset].model_copy(deep=True)
        self.temporary_filters: dict[str, TemporaryFilter] = {}
        self._history: list[PaneState] = []
        self._on_change = on_change
        self._setting_select = False

    @property
    def disabled_filters(self) -> set[str]:
        """Return filters temporarily overridden by drill-down state."""
        return set(self.temporary_filters)

    @property
    def drilldown_filter(self) -> str | None:
        """Return the field represented by clickable columns in this pane."""
        drilldown = _DRILLDOWNS.get(self.agg_spec.preset)
        return drilldown[0] if drilldown is not None else None

    @property
    def agg_specs(self) -> dict[AggPreset, AggSpec]:
        """Return a copy of the last-used specification for every preset."""
        return {
            preset: spec.model_copy(deep=True)
            for preset, spec in self._agg_specs.items()
        }

    def render(self) -> None:
        """Render the summary selector and navigation controls."""
        preset_options = {
            preset.value: preset.name.replace("_", " ").capitalize()
            for preset in AggPreset
        }
        self.summary_view = ui.select(
            preset_options,
            value=self.agg_spec.preset.value,
            label="Summary view",
            on_change=self._select_preset,
        ).classes("min-w-56")
        self.navigation_container = ui.row().classes(
            "w-full items-center gap-2 order-last"
        )
        self._render_navigation()

    def apply_temporary_filters(
        self,
        filter_arguments: FilterArguments,
    ) -> FilterArguments:
        """Compose temporary exact-value filters over persistent arguments."""
        effective = {
            name: arguments.copy() for name, arguments in filter_arguments.items()
        }
        for name, filter_ in self.temporary_filters.items():
            effective[name] = filter_.arguments.copy()
        return effective

    def drill_down(self, value: str) -> None:
        """Move to the configured child pane for a clicked column value."""
        drilldown = _DRILLDOWNS.get(self.agg_spec.preset)
        if drilldown is None:
            return
        filter_name, next_preset = drilldown
        self._history.append(
            PaneState(
                agg_spec=self.agg_spec.model_copy(deep=True),
                temporary_filters=self.temporary_filters.copy(),
            )
        )
        self.temporary_filters[filter_name] = TemporaryFilter(
            arguments={"included_values": [value]},
            display_value=value,
        )
        self._activate_preset(next_preset)
        self._set_select(next_preset)
        self._state_changed()

    def drill_down_dates_row(self, row: dict[str, Any]) -> None:
        """Open raw rows matching one dates-summary result row."""
        if self.agg_spec.preset is not AggPreset.DATES_SUMMARY:
            return
        self._history.append(
            PaneState(
                agg_spec=self.agg_spec.model_copy(deep=True),
                temporary_filters=self.temporary_filters.copy(),
            )
        )
        for name in self.agg_spec.extra_columns:
            value = row.get(name)
            normalized = (
                None
                if value is None or str(value) in {"nan", "<NA>", "None"}
                else str(value)
            )
            self.temporary_filters[name] = TemporaryFilter(
                arguments={"included_values": [normalized]},
                display_value=normalized or "None",
            )
        for name in ("start_date", "end_date"):
            value = str(row[name])
            self.temporary_filters[name] = TemporaryFilter(
                arguments={"value": value},
                display_value=value,
            )
        self._activate_preset(AggPreset.RAW)
        self._set_select(AggPreset.RAW)
        self._state_changed()

    def toggle_extra_column(self, column: str) -> None:
        """Toggle an optional dates-summary grouping column."""
        if (
            self.agg_spec.preset is not AggPreset.DATES_SUMMARY
            or column not in DATES_SUMMARY_EXTRA_COLUMNS
        ):
            return
        enabled = set(self.agg_spec.extra_columns)
        if column in enabled:
            enabled.remove(column)
        else:
            enabled.add(column)
        extra_columns = [
            name for name in DATES_SUMMARY_EXTRA_COLUMNS if name in enabled
        ]
        self.agg_spec = self.agg_spec.model_copy(
            update={"extra_columns": extra_columns}
        )
        self._agg_specs[self.agg_spec.preset] = self.agg_spec.model_copy(deep=True)
        self._state_changed()

    def _select_preset(self, event: ValueChangeEventArguments[Any]) -> None:
        if self._setting_select:
            return
        self._activate_preset(AggPreset(event.value))
        self.temporary_filters.clear()
        self._history.clear()
        self._state_changed()

    def _activate_preset(self, preset: AggPreset) -> None:
        """Activate the last-used specification for a preset."""
        self._agg_specs[self.agg_spec.preset] = self.agg_spec.model_copy(deep=True)
        self.agg_spec = self._agg_specs[preset].model_copy(deep=True)

    def _go_back(self) -> None:
        if not self._history:
            return
        state = self._history.pop()
        self.agg_spec = state.agg_spec
        self._agg_specs[self.agg_spec.preset] = self.agg_spec.model_copy(deep=True)
        self.temporary_filters = state.temporary_filters
        self._set_select(self.agg_spec.preset)
        self._state_changed()

    def _remove_filter(self, name: str) -> None:
        self.temporary_filters.pop(name, None)
        self._state_changed()

    def _set_select(self, preset: AggPreset) -> None:
        self._setting_select = True
        try:
            self.summary_view.value = preset.value
        finally:
            self._setting_select = False

    def _state_changed(self) -> None:
        self._render_navigation()
        self._on_change()

    def _render_navigation(self) -> None:
        self.navigation_container.clear()
        with self.navigation_container:
            if self._history:
                ui.button("Back", icon="arrow_back", on_click=self._go_back).props(
                    "flat no-caps"
                )
            for name, filter_ in self.temporary_filters.items():
                label = name.replace("_", " ").title()
                ui.chip(
                    f"{label}: {filter_.display_value}",
                    removable=True,
                    on_value_change=lambda event, filter_name=name: (
                        self._remove_filter(filter_name) if not event.value else None
                    ),
                ).props("outline")
