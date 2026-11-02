"""Shared dataframe schemas and data-source query types."""

import datetime
from typing import Literal

import pandas as pd
import pandera.pandas as pa
from pydantic import BaseModel


class DataSchema(pa.DataFrameModel):
    """Schema of normalized dashboard rows."""

    asset: str
    instrument: str
    data_level: str
    descriptor: str
    partition: str
    partition_label: str
    repoint: int = pa.Field(nullable=True)
    start_time: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    end_time: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    updated: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ns", "tz": "UTC"},
        nullable=True,
    )
    status: str
    missing_file: str = pa.Field(nullable=True)
    skip_reason: str = pa.Field(nullable=True)
    missing_files: str = pa.Field(nullable=True)
    partition_link: str
    start_date: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    end_date: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})

    class Config:
        """Require normalized data to contain exactly the declared columns."""

        strict = True


class QuerySpec(BaseModel):
    """Describe the date and version constraints for a dashboard query."""

    start_time: datetime.datetime
    end_time: datetime.datetime
    end_delta: datetime.timedelta = datetime.timedelta(seconds=1)
    date_mode: Literal["update_time", "partition"] = "update_time"
    version_mode: Literal["all", "latest"] = "all"

    @property
    def true_end_time(self) -> datetime.datetime:
        """Return the adjusted end time to be used in the query."""
        return self.end_time - self.end_delta


class DataSourceBase:
    """Base interface for dashboard dataframe providers."""

    def query(self, query: QuerySpec) -> pd.DataFrame:
        """Load dashboard rows matching a query specification."""
        raise NotImplementedError
