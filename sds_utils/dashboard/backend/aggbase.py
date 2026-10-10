"""Aggregate dashboard rows into supported summary views."""

from enum import StrEnum, auto

import pandas as pd
from pydantic import BaseModel, Field

from .status import StatusCounts

DATES_SUMMARY_EXTRA_COLUMNS = (
    "instrument",
    "data_level",
    "descriptor",
    "skipped_reason",
)


class AggPreset(StrEnum):
    """Supported raw and aggregated table layouts."""

    RAW = auto()
    DATES_SUMMARY = auto()
    DATA_LEVELS_SNAPSHOT = auto()
    INSTRUMENTS_SNAPSHOT = auto()
    JOBS_SUMMARY = auto()


class AggSpec(BaseModel):
    """Select an aggregation preset and optional grouping columns."""

    preset: AggPreset
    extra_columns: list[str] = Field(default_factory=list)


class Aggregator:
    """Apply dashboard aggregation specifications to dataframes."""

    def apply(self, data_df: pd.DataFrame, agg_spec: AggSpec) -> pd.DataFrame:
        """Transform dashboard rows according to an aggregation specification."""
        main_cols = ["start_date", "end_date"]
        match agg_spec.preset:
            case AggPreset.RAW:
                return data_df
            case AggPreset.DATES_SUMMARY:
                horiz_col = None
            case AggPreset.DATA_LEVELS_SNAPSHOT:
                horiz_col = "data_level"
            case AggPreset.INSTRUMENTS_SNAPSHOT:
                horiz_col = "instrument"
            case AggPreset.JOBS_SUMMARY:
                horiz_col = None
                main_cols = ["instrument", "data_level", "descriptor"]
            case _:
                raise NotImplementedError(agg_spec.preset)

        horiz_cols = [horiz_col] if horiz_col is not None else []
        cols_to_keep = [*main_cols, *horiz_cols, *agg_spec.extra_columns]
        cols_to_keep = list(dict.fromkeys(cols_to_keep))

        def _make_status_counts(statuses: pd.Series) -> StatusCounts:
            kwargs: dict[str, int] = statuses.value_counts().to_dict()
            kwargs = {k.replace("-", "_"): v for k, v in kwargs.items()}
            return StatusCounts(**kwargs)

        grouped_df = pd.DataFrame(
            data_df.groupby(cols_to_keep, dropna=False)
            .status.apply(_make_status_counts)
            .rename("status_counts")
        ).reset_index()

        if agg_spec.preset == AggPreset.JOBS_SUMMARY:
            grouped_df2 = pd.DataFrame(
                data_df.groupby(main_cols, dropna=False)
                .dagster_status.value_counts()
                .rename("count")
            ).reset_index()
            pivot_df = grouped_df2.pivot_table(
                columns="dagster_status",
                index=main_cols,
                values="count",
                aggfunc="sum",
                fill_value=0,
            )
            pivot_df = pivot_df.reset_index().rename_axis(columns=None)
            grouped_df = grouped_df.set_index(main_cols)
            pivot_df = pivot_df.set_index(main_cols)
            grouped_df = pd.concat([grouped_df, pivot_df], axis=1).reset_index()

        if horiz_col is not None:
            if agg_spec.extra_columns:
                raise ValueError(
                    "Extra columns are not allowed for this agg preset: "
                    f"{agg_spec.preset}"
                )
            snapshot_df = grouped_df.pivot_table(
                columns=horiz_col,
                index="start_date end_date".split(),
                values="status_counts",
                aggfunc="sum",
            )
            return snapshot_df.sort_index(ascending=False)
        else:
            return grouped_df.sort_values(
                main_cols,
                ascending=False,
                kind="stable",
                ignore_index=True,
            )
