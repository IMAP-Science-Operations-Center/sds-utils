"""NiceGUI rendering for dashboard dataframes."""

import json
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
from nicegui import ui

from ..backend.aggbase import DATES_SUMMARY_EXTRA_COLUMNS, AggPreset, AggSpec
from .filtercontrols import FilterControls, GroupingControls
from .status import (
    SNAPSHOT_TOTAL_CLASS,
    SNAPSHOT_ZERO_CLASS,
    STATUS_BADGE_COLORS,
    format_status_counts,
    status_count_columns,
)


@dataclass(frozen=True)
class TableRenderOptions:
    """Callbacks and state controlling interactive table headers."""

    disabled_filters: set[str]
    drilldown_filter: str | None
    on_drill_down: Callable[[str], None]
    on_dates_row_drill_down: Callable[[dict[str, object]], None]
    agg_spec: AggSpec
    on_toggle_grouping: Callable[[str], None]


class DashboardTableRenderer:
    """Render dashboard dataframes with specialized cells and headers."""

    def __init__(self, dagster_url: str) -> None:
        self.dagster_url = dagster_url.rstrip("/")

    def display_data(
        self,
        data_df: pd.DataFrame,
        agg_spec: AggSpec,
    ) -> pd.DataFrame:
        """Remove internal fields and format display-only date columns."""
        cols_to_hide = [
            "asset",
            "partition",
            "partition_prefix",
            "job_name",
            "job_key",
            "tags",
            "selected_assets",
            "parent_run_id",
            "root_run_id",
            "skip_info",
            "skip_reason",
            "missing_files",
        ]
        display_df = data_df.drop(columns=cols_to_hide, errors="ignore")
        if any(name is not None for name in display_df.index.names):
            display_df = display_df.reset_index()
        for column in ("start_date", "end_date"):
            if column in display_df.columns:
                display_df[column] = display_df[column].dt.strftime("%Y-%m-%d")
        if agg_spec.preset is AggPreset.DATES_SUMMARY:
            for column in DATES_SUMMARY_EXTRA_COLUMNS:
                if column not in display_df:
                    display_df[column] = None
            display_df["_drilldown"] = None
            fixed = [
                "_drilldown",
                *DATES_SUMMARY_EXTRA_COLUMNS,
                "start_date",
                "end_date",
                "status_counts",
            ]
            ordered = [column for column in fixed if column in display_df]
            ordered.extend(column for column in display_df if column not in ordered)
            display_df = display_df.loc[:, ordered]
        return display_df

    def render(
        self,
        display_df: pd.DataFrame,
        *,
        filter_controls: FilterControls,
        options: TableRenderOptions,
    ) -> None:
        """Render a prepared dataframe in the current NiceGUI container."""
        status_columns = status_count_columns(display_df)
        table_df = display_df.copy()
        for column in status_columns:
            table_df[column] = format_status_counts(table_df[column])

        self.table = ui.table.from_pandas(
            table_df.reset_index(drop=True),
            pagination=25,
        ).classes("dashboard-sticky-table w-full h-full shadow-none border rounded-lg")
        for column in status_columns:
            for row, value in zip(self.table.rows, table_df[column], strict=True):
                row[column] = value
        self.table.props("flat bordered separator=horizontal wrap-cells")
        self._render_run_links(table_df)
        self._render_partition_links()
        self._render_status_badges()
        self._render_status_counts(status_columns)
        if options.agg_spec.preset is AggPreset.DATES_SUMMARY:
            self._render_dates_row_drill_down(options.on_dates_row_drill_down)
        if options.drilldown_filter is not None:
            self._render_drill_down_headers(status_columns, options.on_drill_down)
        grouping = None
        if options.agg_spec.preset is AggPreset.DATES_SUMMARY:
            grouping = GroupingControls(
                columns=set(DATES_SUMMARY_EXTRA_COLUMNS),
                enabled=set(options.agg_spec.extra_columns),
                on_toggle=options.on_toggle_grouping,
            )
        filter_controls.render_headers(
            self.table,
            display_df.columns,
            disabled=options.disabled_filters,
            grouping=grouping,
        )

    def _render_dates_row_drill_down(
        self,
        on_drill_down: Callable[[dict[str, object]], None],
    ) -> None:
        """Render the dates-summary row action leading to the raw table."""
        for column in self.table.columns:
            if column["name"] == "_drilldown":
                column["label"] = ""
                column["align"] = "center"
                column["style"] = "width: 3rem"
                break
        self.table.add_slot(
            "body-cell-_drilldown",
            """
            <q-td :props="props" class="text-center">
                <q-btn
                    flat
                    dense
                    round
                    icon="link"
                    color="primary"
                    @click="$parent.$emit('datesRowDrilldown', props.row)"
                />
            </q-td>
            """,
        )
        self.table.on("datesRowDrilldown", lambda event: on_drill_down(event.args))

    def _render_run_links(self, table_df: pd.DataFrame) -> None:
        if "run_id" not in table_df.columns:
            return
        dagster_url = json.dumps(self.dagster_url)
        self.table.add_slot(
            "body-cell-run_id",
            f"""
            <q-td :props="props">
                <a
                    :href='{dagster_url} + "/runs/" + props.value'
                    class="text-blue-8"
                    target="_blank"
                    rel="noopener noreferrer"
                >{{{{ props.value.slice(0, 8) }}}}</a>
            </q-td>
            """,
        )

    def _render_partition_links(self) -> None:
        self.table.add_slot(
            "body-cell-partition_link",
            """
            <q-td :props="props">
                <a
                    :href="props.value"
                    class="text-blue-8"
                    target="_blank"
                    rel="noopener noreferrer"
                >link</a>
            </q-td>
            """,
        )

    def _render_status_badges(self) -> None:
        badge_colors = json.dumps(STATUS_BADGE_COLORS)
        self.table.add_slot(
            "body-cell-status",
            f"""
            <q-td :props="props">
                <q-badge
                    :color='({badge_colors})[props.value]'
                    :label="props.value"
                    outline
                />
            </q-td>
            """,
        )

    def _render_status_counts(self, columns: list[str]) -> None:
        for column in columns:
            self.table.add_slot(
                f"body-cell-{column}",
                f"""
                <q-td :props="props">
                    <template v-if="Array.isArray(props.value)">
                        <span :class="props.value[0].class">
                            {{{{ props.value[0].text }}}}
                        </span><span class="{SNAPSHOT_TOTAL_CLASS}">: </span>
                        <template
                            v-for="(part, index) in props.value.slice(1)"
                            :key="index"
                        >
                            <span
                                v-if="index"
                                class="{SNAPSHOT_ZERO_CLASS}"
                            > / </span>
                            <span :class="part.class">{{{{ part.text }}}}</span>
                        </template>
                    </template>
                </q-td>
                """,
            )

    def _render_drill_down_headers(
        self,
        columns: list[str],
        on_drill_down: Callable[[str], None],
    ) -> None:
        for column in columns:
            with self.table.add_slot(f"header-cell-{column}"):
                with self.table.header(column):
                    ui.button(
                        column,
                        on_click=lambda _event, value=column: on_drill_down(value),
                    ).props("flat dense no-caps").classes("w-full")
