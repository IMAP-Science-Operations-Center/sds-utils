"""NiceGUI orchestration for filtered and aggregated dashboard tables."""

import datetime
from abc import abstractmethod
from collections.abc import Callable

from nicegui import ui

from ..backend.data import QuerySpec
from ..backend.filteredtable import FilteredTable, SortSpec
from ..backend.settings import DashboardSettings
from .filtercontrols import FilterControls
from .navigation import PaneNavigator
from .status import StatusSummary
from .tablerenderer import DashboardTableRenderer, TableRenderOptions
from .uielem import UIElem

_SORT_SPECS = {
    "instrument": SortSpec(),
    "data_level": SortSpec(),
    "descriptor": SortSpec(),
    "start_time": SortSpec(ascending=False),
    "start_date": SortSpec(ascending=False),
    "end_date": SortSpec(ascending=False),
}


class FilteredTableViewBase(UIElem):
    """Interface for queryable dashboard table views."""

    @abstractmethod
    def update_query(self, query: QuerySpec) -> None:
        """Update the table query and ultimately the transforms as well."""

    @abstractmethod
    def update_table(self) -> None:
        """Update table transforms but not the query."""


class FilteredTableView(FilteredTableViewBase):
    """Coordinate backend data with focused frontend components."""

    def __init__(
        self,
        table: FilteredTable,
        *,
        dagster_url: str,
        settings: DashboardSettings,
        sync_settings: Callable[[DashboardSettings], None],
    ) -> None:
        self.table = table
        self.settings = settings
        self._sync_settings = sync_settings
        self.renderer = DashboardTableRenderer(dagster_url)

    def render(self) -> None:
        """Create controls, summaries, and the initial table."""
        self.filters = FilterControls(
            self.table.filters,
            initial_arguments=self.settings.filtering,
            on_change=self.update_table,
        )
        self.navigator = PaneNavigator(
            self.settings.active_agg_preset,
            self.settings.agg_specs,
            self.update_table,
        ).build()
        self.status_summary = StatusSummary(self.filters.menus["status"]).build()
        self.standalone_filters_container = ui.row().classes(
            "w-full items-center gap-3 flex-wrap"
        )
        self.table_container = ui.column().classes("w-full")
        self.update_query(
            QuerySpec(
                start_time=datetime.datetime(2026, 8, 1),
                end_time=datetime.datetime(2026, 9, 30) - datetime.timedelta(seconds=1),
                version_mode="latest",
            )
        )

    def update_query(self, query: QuerySpec) -> None:
        """Reload backend data and refresh query-dependent controls."""
        self.table.set_query(query)
        self.table.refresh_data()
        self.full_data_df = self.table.transform_data()
        self.filters.update_query(self.full_data_df)
        self.status_summary.update_query(self.full_data_df, select_new_values=True)
        self.filters.restore_initial_arguments()
        self.update_table()

    def update_table(self) -> None:
        """Apply current state and rebuild the table and summaries."""
        persistent_filters = self.filters.arguments()
        effective_filters = self.navigator.apply_temporary_filters(persistent_filters)
        data_df = self.table.transform_data(
            filter_kwargs=effective_filters,
            agg_spec=self.navigator.agg_spec,
            sort_specs=_SORT_SPECS,
        )
        display_df = self.renderer.display_data(data_df, self.navigator.agg_spec)

        self.standalone_filters_container.clear()
        with self.standalone_filters_container:
            self.filters.render_standalone(
                display_df.columns,
                disabled=self.navigator.disabled_filters,
            )

        self.table_container.clear()
        with self.table_container:
            self.renderer.render(
                display_df,
                filter_controls=self.filters,
                options=TableRenderOptions(
                    disabled_filters=self.navigator.disabled_filters,
                    drilldown_filter=self.navigator.drilldown_filter,
                    on_drill_down=self.navigator.drill_down,
                    on_dates_row_drill_down=self.navigator.drill_down_dates_row,
                    agg_spec=self.navigator.agg_spec,
                    on_toggle_grouping=self.navigator.toggle_extra_column,
                ),
            )

        self.status_summary.update(data_df)
        self._sync_settings(
            DashboardSettings(
                active_agg_preset=self.navigator.agg_spec.preset,
                agg_specs=self.navigator.agg_specs,
                filtering=persistent_filters,
            )
        )
