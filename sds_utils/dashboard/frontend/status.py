"""Status colors and summary cards shared by dashboard views."""

from collections.abc import Callable

import pandas as pd
from nicegui import ui

from ..backend.status import STATUS_ORDER, StatusCounts
from .elems import help_icon
from .filters import StringFilterMenu
from .uielem import UIElem

STATUS_HELP = {
    "materialized": "Materialized: The run created all of its declared assets.",
    "materializing": (
        "Materializing: The run has not completed yet. "
        "It might be queued or in progress."
    ),
    "canceling": (
        "Canceling: The run has been marked for cancellation but not canceled yet."
    ),
    "canceled": "Canceled: The run was canceled before completing.",
    "failed": "Failed: Dagster marked the run as failed.",
    "skipped": "Skipped: The run skipped, typically due to missing dependencies.",
    "missing": (
        "Missing: The run completed without materializing all of its declared assets."
    ),
    "not-run": (
        "Not run: No run was kicked off, "
        "either manually or automatically with a Dagster sensor."
    ),
    "unknown": (
        "Unknown: Detailed data for this run has not yet been ingested. "
        "This most often occurs while ingestion is in progress."
    ),
    "not-found": (
        "Not found: The logic for determining the run's status failed. "
        "This should be rare."
    ),
}

STATUS_CARD_COLORS = {
    "materialized": "#22c55e",
    "materializing": "#8b5cf6",
    "canceling": "#06b6d4",
    "canceled": "#6b7280",
    "failed": "#ef4444",
    "skipped": "#eab308",
    "missing": "#d946ef",
    "not-run": "#64748b",
    "unknown": "#f97316",
    "not-found": "#06b6d4",
}

STATUS_BADGE_COLORS = {
    "materialized": "positive",
    "materializing": "info",
    "canceling": "cyan-8",
    "canceled": "grey-6",
    "failed": "negative",
    "skipped": "yellow-8",
    "missing": "pink-7",
    "not-run": "grey-7",
    "unknown": "orange",
    "not-found": "cyan-8",
}

SNAPSHOT_STATUS_CLASSES = {
    "materialized": "text-green-700",
    "materializing": "text-violet-700",
    "canceling": "text-cyan-600",
    "canceled": "text-current opacity-60",
    "failed": "text-red-600",
    "skipped": "text-yellow-600",
    "missing": "text-fuchsia-600",
    "not-run": "text-current",
    "unknown": "text-orange-700",
    "not-found": "text-cyan-700",
}
SNAPSHOT_TOTAL_CLASS = "text-current opacity-70"
SNAPSHOT_ZERO_CLASS = "text-current opacity-30"


def status_count_columns(data_df: pd.DataFrame) -> list[str]:
    """Return columns whose non-null values are all ``StatusCounts`` objects."""
    columns = []
    for column in data_df.columns:
        values = data_df[column].dropna()
        if (
            not values.empty
            and values.map(lambda value: isinstance(value, StatusCounts)).all()
        ):
            columns.append(column)
    return columns


def format_status_counts(series: pd.Series) -> pd.Series:
    """Format a StatusCounts series for colored, position-padded table cells."""
    counts = [value for value in series.dropna() if isinstance(value, StatusCounts)]
    widths = [
        max(
            (len(str(position_value)) for position_value in position_values),
            default=1,
        )
        for position_values in zip(
            *((sum(value), *value) for value in counts),
            strict=True,
        )
    ]
    status_classes = [
        SNAPSHOT_STATUS_CLASSES[field.replace("_", "-")]
        for field in StatusCounts._fields
    ]

    def format_value(value: object) -> list[dict[str, str]] | None:
        if not isinstance(value, StatusCounts):
            return None
        total = sum(value)
        parts = [
            {
                "text": f"{total:0{widths[0]}d}",
                "class": (
                    f"{SNAPSHOT_TOTAL_CLASS} font-bold"
                    if total
                    else SNAPSHOT_TOTAL_CLASS
                ),
            }
        ]
        parts.extend(
            {
                "text": f"{count:0{width}d}",
                "class": (
                    f"{status_class} font-bold" if count else SNAPSHOT_ZERO_CLASS
                ),
            }
            for count, width, status_class in zip(
                value,
                widths[1:],
                status_classes,
                strict=True,
            )
        )
        return parts

    return series.map(format_value)


class StatusCountCard(UIElem):
    """Base card with shared status-summary sizing and content layout."""

    BASE_CLASSES = "min-w-36 px-4 py-3 shadow-none border-2 select-none"

    def __init__(
        self,
        label: str,
        color: str,
        help_text: str | None,
    ) -> None:
        self.label = label
        self.color = color
        self.help_text = help_text

    def render(self) -> None:
        """Render the consistently sized card header and count."""
        self.card = ui.card().classes(replace=self._classes())
        self.card.style(f"--status-color: {self.color}")
        self._configure_card()
        with self.card:
            with ui.row().classes("w-full items-center justify-between gap-2"):
                ui.label(self.label).classes("text-xs uppercase tracking-wide")
                if self.help_text:
                    help_icon(self.help_text)
            self.count_label = ui.label("0").classes("text-2xl font-semibold")

    def _configure_card(self) -> None:
        """Apply behavior specific to a card subtype."""

    def _classes(self) -> str:
        return f"dashboard-status-card {self.BASE_CLASSES}"

    def set_count(self, shown: int) -> None:
        """Update the displayed count."""
        self.count_label.set_text(f"{shown:,}")


class StatusFilterCard(StatusCountCard):
    """Clickable status count card matching the established dashboard style."""

    INTERACTIVE_CLASSES = "cursor-pointer transition-all duration-150"
    INACTIVE_CLASSES = "dashboard-status-inactive opacity-50"

    def __init__(
        self,
        status: str,
        on_toggle: Callable[[str, bool], None],
        *,
        active: bool = True,
    ) -> None:
        super().__init__(
            label=status,
            color=STATUS_CARD_COLORS[status],
            help_text=STATUS_HELP.get(status),
        )
        self.status = status
        self.on_toggle = on_toggle
        self.active = active

    def _configure_card(self) -> None:
        self.card.props("role=button tabindex=0")
        self.card.on("click", self._toggle)

    def set_count(self, shown: int) -> None:
        """Update the count, showing a dash when this status is unselected."""
        self.count_label.set_text(f"{shown:,}" if self.active else "-")

    def set_active(self, active: bool) -> None:
        """Synchronize the card's active appearance without invoking callbacks."""
        self.active = active
        self.card.classes(replace=self._classes())

    def _toggle(self) -> None:
        self.set_active(not self.active)
        self.on_toggle(self.status, self.active)

    def _classes(self) -> str:
        state_classes = "" if self.active else self.INACTIVE_CLASSES
        return f"{super()._classes()} {self.INTERACTIVE_CLASSES} {state_classes}"


class StatusTotalCard(StatusCountCard):
    """Non-interactive card showing the total across selected statuses."""

    def __init__(self) -> None:
        super().__init__(
            label="(TOTAL)",
            color=STATUS_CARD_COLORS["not-run"],
            help_text="Total: The sum of all the other selected status counts.",
        )


class StatusSummary(UIElem):
    """Row of status cards reflecting the table's current filters."""

    def __init__(
        self,
        status_filter: StringFilterMenu,
    ) -> None:
        self.status_filter = status_filter
        self.statuses = list(STATUS_ORDER)
        self.cards: dict[str, StatusFilterCard] = {}

    def render(self) -> None:
        """Render cards in the canonical status order."""
        ui.add_css(
            """
            .dashboard-status-summary {
                --status-surface: white;
                --status-foreground: #111827;
            }
            .body--dark .dashboard-status-summary {
                --status-surface: var(--q-dark);
                --status-foreground: white;
            }
            .dashboard-status-card {
                background: color-mix(
                    in srgb, var(--status-color) 12%, var(--status-surface)
                );
                border-color: color-mix(
                    in srgb, var(--status-color) 65%, var(--status-surface)
                );
                color: color-mix(
                    in srgb, var(--status-color) 35%, var(--status-foreground)
                );
            }
            .dashboard-status-card.dashboard-status-inactive {
                background: var(--status-surface);
                border-color: color-mix(
                    in srgb, var(--status-foreground) 20%, var(--status-surface)
                );
                color: color-mix(
                    in srgb, var(--status-foreground) 60%, var(--status-surface)
                );
            }
            """
        )
        self.container = ui.row().classes(
            "dashboard-status-summary w-full gap-3 flex-wrap"
        )
        self._render_cards()

    def _render_cards(self) -> None:
        self.cards = {}
        with self.container:
            self.total_card = StatusTotalCard().build()
            for status in self.statuses:
                self.cards[status] = StatusFilterCard(
                    status,
                    self.status_filter.set_value_selected,
                ).build()

    def update_query(
        self,
        source_df: pd.DataFrame,
        *,
        select_new_values: bool,
    ) -> None:
        """Update status choices for newly queried source data."""
        self._update_statuses(source_df)
        self.status_filter.update_values(
            self.statuses,
            select_new_values=select_new_values,
        )
        self.status_filter.update_query(
            source_df,
            select_new_values=select_new_values,
        )

    def update(
        self,
        shown_df: pd.DataFrame,
    ) -> None:
        """Update counts and active state from displayed table data."""
        counts = self._status_counts(shown_df)
        self.total_card.set_count(
            sum(counts.get(status, 0) for status in self.status_filter.selected)
        )

        for status, card in self.cards.items():
            card.set_active(status in self.status_filter.selected)
            card.set_count(counts.get(status, 0))

    @staticmethod
    def _status_counts(data_df: pd.DataFrame) -> dict[str, int]:
        if "status" in data_df:
            return {
                str(status): int(count)
                for status, count in data_df["status"].value_counts().items()
            }

        counts = StatusCounts()
        for column in status_count_columns(data_df):
            for value in data_df[column].dropna():
                counts += value
        return {
            field.replace("_", "-"): count
            for field, count in zip(StatusCounts._fields, counts, strict=True)
        }

    def _update_statuses(self, source_df: pd.DataFrame) -> None:
        unknown = sorted(set(source_df["status"].dropna()) - set(STATUS_ORDER))
        statuses = [*STATUS_ORDER, *unknown]
        if statuses != self.statuses:
            self.statuses = statuses
            self.container.clear()
            self._render_cards()
