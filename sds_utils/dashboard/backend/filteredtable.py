"""Filtering and aggregation orchestration for dashboard tables."""

from collections import defaultdict
from collections.abc import Iterable

import pandas as pd
from pydantic import BaseModel

from .aggbase import (
    Aggregator,
    AggSpec,
)
from .data import DataSourceBase, QuerySpec
from .filtersbase import (
    FilterArguments,
    FiltersBase,
    StrHierarchySpec,
    StringRegisteredFilter,
    filter_property,
)
from .projectionbase import ProjectionSpec, Projector


class SortSpec(BaseModel):
    """Lightweight specification for sorting options."""

    ascending: bool = True


def _make_data_level_hierachy(values: Iterable[str]) -> dict[str, list[str]]:
    values = [value for value in values if value.lower().startswith("l")]
    hierarchy_sets: dict[str, set[str]] = defaultdict(set)
    for value in values:
        prefix = value[:2]
        hierarchy_sets[prefix].add(value)
    return {key: sorted(values_set) for key, values_set in hierarchy_sets.items()}


class Filters(FiltersBase):
    """Declare the filters available to dashboard table consumers."""

    status = StringRegisteredFilter.property()
    instrument = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy={
                "ENA": "lo hi ultra".split(),
                "In-situ": "codice glows hit idex mag swapi swe".split(),
            },
        ),
    )
    data_level = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy=_make_data_level_hierachy,
        ),
    )
    descriptor = StringRegisteredFilter.property()
    skipped_reason = StringRegisteredFilter.property()
    partition_label = StringRegisteredFilter.property(
        hierarchy=StrHierarchySpec(
            hierarchy=dict(
                Short="daily repoint".split(),
            ),
            other="Long",
        ),
    )

    @filter_property
    def start_date(
        self,
        data_df: pd.DataFrame,
        value: str,
    ) -> pd.DataFrame:
        """Keep rows whose partition start date matches an ISO date."""
        return data_df[data_df["start_date"].dt.strftime("%Y-%m-%d") == value]

    @filter_property
    def end_date(
        self,
        data_df: pd.DataFrame,
        value: str,
    ) -> pd.DataFrame:
        """Keep rows whose partition end date matches an ISO date."""
        return data_df[data_df["end_date"].dt.strftime("%Y-%m-%d") == value]


class FilteredTable:
    """Coordinate data loading, projection, filtering, and aggregation."""

    def __init__(
        self,
        data_source: DataSourceBase,
        projector: Projector | None = None,
        filters: Filters | None = None,
        agg: Aggregator | None = None,
    ) -> None:
        self._projector = projector or Projector()
        self._filters = filters or Filters()
        self._agg = agg or Aggregator()
        self._data_source = data_source
        self._query_spec: QuerySpec | None = None
        self._full_data_df: pd.DataFrame | None = None

    @property
    def filters(self) -> Filters:
        """Return the filters available for this table."""
        return self._filters

    def set_query(self, query_spec: QuerySpec) -> None:
        """Set the query used by the next data refresh."""
        self._query_spec = query_spec

    def refresh_data(self) -> None:
        """Reload the complete dataframe for the configured query."""
        if self._query_spec is None:
            raise ValueError("The set_query method must be run before refresh_data.")
        self._full_data_df = self._data_source.query(self._query_spec)

    def transform_data(
        self,
        projection_spec: ProjectionSpec | None = None,
        filter_kwargs: FilterArguments | None = None,
        agg_spec: AggSpec | None = None,
        sort_specs: dict[str, SortSpec] | None = None,
    ) -> pd.DataFrame:
        """Apply projection, filtering, aggregation, and sorting in that order."""
        if self._full_data_df is None:
            self.refresh_data()
        data_df = self._full_data_df
        if projection_spec is not None:
            data_df = self._projector.apply(data_df, projection_spec)
        data_df = self._filters.apply(data_df, filter_kwargs)
        if agg_spec is not None:
            data_df = self._agg.apply(data_df, agg_spec)
        if sort_specs is not None:
            data_df = self._sort(data_df, sort_specs)
        return data_df

    @classmethod
    def _sort(
        cls, data_df: pd.DataFrame, sort_spec: dict[str, SortSpec]
    ) -> pd.DataFrame:
        """Sort the dataframe by the indicated columns."""
        all_cols = set(data_df.columns)
        sort_spec = {k: v for k, v in sort_spec.items() if k in all_cols}
        cols = list(sort_spec.keys())
        asc = [spec.ascending for spec in sort_spec.values()]
        if cols:
            data_df = data_df.sort_values(cols, ascending=asc)
        return data_df
