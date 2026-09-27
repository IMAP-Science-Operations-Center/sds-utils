"""Status colors and summary cards shared by dashboard views."""

from collections.abc import Callable

import pandas as pd
from nicegui import ui

from ..backend.status import STATUS_ORDER, StatusCounts
from .filters import StringFilterMenu
from .uielem import UIElem

STATUS_CARD_CLASSES = {
    "materialized": "border-green-400 bg-green-50 text-green-900",
    "materializing": "border-violet-400 bg-violet-50 text-violet-900",
    "canceling": "border-cyan-400 bg-cyan-50 text-cyan-900",
    "canceled": "border-gray-400 bg-gray-100 text-gray-900",
    "failed": "border-red-400 bg-red-50 text-red-900",
    "skipped": "border-yellow-400 bg-yellow-50 text-yellow-900",
    "missing": "border-blue-400 bg-blue-50 text-blue-900",
    "not-run": "border-slate-400 bg-slate-100 text-slate-900",
    "unknown": "border-orange-400 bg-orange-50 text-orange-900",
    "not-found": "border-cyan-400 bg-cyan-50 text-cyan-900",
}

STATUS_BADGE_COLORS = {
    "materialized": "positive",
    "materializing": "info",
    "canceling": "cyan-8",
    "canceled": "grey-6",
    "failed": "negative",
    "skipped": "yellow-8",
    "missing": "blue-8",
    "not-run": "grey-7",
    "unknown": "orange",
    "not-found": "cyan-8",
}

SNAPSHOT_STATUS_CLASSES = {
    "materialized": "text-green-700",
    "materializing": "text-violet-700",
    "canceling": "text-cyan-600",
    "canceled": "text-gray-600",
    "failed": "text-red-600",
    "skipped": "text-yellow-600",
    "missing": "text-blue-600",
    "not-run": "text-black",
    "unknown": "text-orange-700",
    "not-found": "text-cyan-700",
}
SNAPSHOT_TOTAL_CLASS = "text-gray-500"
SNAPSHOT_ZERO_CLASS = "text-gray-300"


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
        parts = [
            {
                "text": f"{sum(value):0{widths[0]}d}",
                "class": SNAPSHOT_TOTAL_CLASS,
            }
        ]
        parts.extend(
            {
                "text": f"{count:0{width}d}",
                "class": status_class if count else SNAPSHOT_ZERO_CLASS,
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


class StatusFilterCard(UIElem):
    """Clickable status count card matching the established dashboard style."""

    BASE_CLASSES = (
        "min-w-36 px-4 py-3 shadow-none border-2 cursor-pointer "
        "select-none transition-all duration-150"
    )
    INACTIVE_CLASSES = "border-slate-200 bg-white text-slate-400 opacity-50"

    def __init__(
        self,
        status: str,
        on_toggle: Callable[[str, bool], None],
        *,
        active: bool = True,
    ) -> None:
        self.status = status
        self.on_toggle = on_toggle
        self.active = active

    def render(self) -> None:
        """Render the clickable card and its count."""
        self.card = ui.card().classes(replace=self._classes())
        self.card.props("role=button tabindex=0")
        self.card.tooltip(f"Toggle {self.status} partitions")
        self.card.on("click", self._toggle)
        with self.card:
            ui.label(self.status).classes("text-xs uppercase tracking-wide")
            self.count_label = ui.label("0").classes("text-2xl font-semibold")

    def set_count(self, shown: int) -> None:
        """Update the displayed count."""
        shown_text = f"{shown:,}" if self.active else "-"
        self.count_label.set_text(shown_text)

    def set_active(self, active: bool) -> None:
        """Synchronize the card's active appearance without invoking callbacks."""
        self.active = active
        self.card.classes(replace=self._classes())

    def _toggle(self) -> None:
        self.set_active(not self.active)
        self.on_toggle(self.status, self.active)

    def _classes(self) -> str:
        state_classes = (
            STATUS_CARD_CLASSES[self.status] if self.active else self.INACTIVE_CLASSES
        )
        return f"{self.BASE_CLASSES} {state_classes}"


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
        self.container = ui.row().classes("w-full gap-3 flex-wrap")
        self._render_cards()

    def _render_cards(self) -> None:
        self.cards = {}
        with self.container:
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
