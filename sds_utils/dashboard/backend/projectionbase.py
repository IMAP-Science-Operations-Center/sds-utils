"""Logical-row projections applied before dashboard filtering."""

import re
from collections.abc import Callable, Collection
from enum import StrEnum, auto
from typing import Any, NamedTuple

import pandas as pd
from pydantic import BaseModel

from .jobkey import CurrentJobDefinition, current_job_definitions


class JobProjectionMode(StrEnum):
    """Select which relationship to current job definitions is displayed."""

    ALL_RUNS = auto()
    LATEST_JOBS_ONLY = auto()
    LEGACY_RUNS_ONLY = auto()


class JobProjectionType(StrEnum):
    """Describe how one logical row relates to current job definitions."""

    CURRENT_RUN = auto()
    ASSUMED_CURRENT_RUN = auto()
    LEGACY_RUN = auto()
    NOT_RUN = auto()


class JobProjectionReason(StrEnum):
    """Explain the evidence used for a job-projection classification."""

    PLANNED_ASSETS_MATCH = auto()
    SELECTED_ASSETS_MATCH = auto()
    EXPECTED_ASSETS_MATCH = auto()
    INSUFFICIENT_EVIDENCE = auto()
    JOB_KEY_NOT_DEFINED = auto()
    PLANNED_ASSETS_MISMATCH = auto()
    SELECTED_ASSETS_MISMATCH = auto()
    EXPECTED_RUN_MISSING = auto()


class ProjectionSpec(BaseModel):
    """Configure logical-row transformations before filtering."""

    job_projection_mode: JobProjectionMode = JobProjectionMode.ALL_RUNS


class _Classification(NamedTuple):
    projection: str
    type: JobProjectionType
    reason: JobProjectionReason


DefinitionLoader = Callable[[Collection[str]], dict[str, CurrentJobDefinition]]

_IDEX_PARTITION_LABEL = re.compile(r"^idex(?P<days>\d+)$")


def _asset_set(value: object) -> frozenset[tuple[str, ...]]:
    if not isinstance(value, list):
        return frozenset()
    return frozenset(
        tuple(part for part in path if isinstance(part, str))
        for path in value
        if isinstance(path, list) and all(isinstance(part, str) for part in path)
    )


def _partition_type(partition_label: str) -> str:
    """Translate concrete partition labels to dependency-YAML partition types."""
    if partition_label.startswith("cadence-"):
        return partition_label.removeprefix("cadence-")
    match = _IDEX_PARTITION_LABEL.fullmatch(partition_label)
    if match is not None:
        return f"{match.group('days')}d"
    return partition_label


class Projector:
    """Classify actual runs and synthesize missing current-job occurrences."""

    def __init__(self, definition_loader: DefinitionLoader = current_job_definitions):
        self._definition_loader = definition_loader

    def apply(
        self,
        data_df: pd.DataFrame,
        projection_spec: ProjectionSpec,
    ) -> pd.DataFrame:
        """Apply a job projection to the queried logical rows."""
        if data_df.empty:
            return self._add_empty_columns(data_df)

        instruments = {
            str(value)
            for value in data_df.get("instrument", pd.Series(dtype="string")).dropna()
        }
        definitions = self._definition_loader(instruments)
        projected_df = self._classify(data_df, definitions)

        match projection_spec.job_projection_mode:
            case JobProjectionMode.ALL_RUNS:
                return projected_df
            case JobProjectionMode.LEGACY_RUNS_ONLY:
                return projected_df[
                    projected_df["job_projection_type"] == JobProjectionType.LEGACY_RUN
                ].reset_index(drop=True)
            case JobProjectionMode.LATEST_JOBS_ONLY:
                current_df = projected_df[
                    projected_df["job_projection_type"] != JobProjectionType.LEGACY_RUN
                ].reset_index(drop=True)
                return self._add_not_run_rows(current_df, projected_df, definitions)
            case _:
                raise NotImplementedError(projection_spec.job_projection_mode)

    @staticmethod
    def _add_empty_columns(data_df: pd.DataFrame) -> pd.DataFrame:
        result = data_df.copy()
        result["job_projection"] = pd.Series(dtype="string")
        result["job_projection_type"] = pd.Series(dtype="string")
        result["job_projection_reason"] = pd.Series(dtype="string")
        return result

    def _classify(
        self,
        data_df: pd.DataFrame,
        definitions: dict[str, CurrentJobDefinition],
    ) -> pd.DataFrame:
        result = data_df.copy()
        classifications = [
            self._classify_row(row, definitions) for _, row in result.iterrows()
        ]
        result["job_projection"] = [value.projection for value in classifications]
        result["job_projection_type"] = [value.type for value in classifications]
        result["job_projection_reason"] = [value.reason for value in classifications]
        if "source_kind" not in result:
            result["source_kind"] = "dagster_run"
        return result

    @staticmethod
    def _classify_row(
        row: pd.Series,
        definitions: dict[str, CurrentJobDefinition],
    ) -> _Classification:
        job_key = row.get("job_key")
        definition = definitions.get(str(job_key)) if pd.notna(job_key) else None
        if definition is None:
            return _Classification(
                "legacy",
                JobProjectionType.LEGACY_RUN,
                JobProjectionReason.JOB_KEY_NOT_DEFINED,
            )

        expected = definition.expected_assets
        planned = _asset_set(row.get("planned_assets"))
        if planned:
            return _Classification(
                definition.job_key if planned == expected else "legacy",
                JobProjectionType.CURRENT_RUN
                if planned == expected
                else JobProjectionType.LEGACY_RUN,
                JobProjectionReason.PLANNED_ASSETS_MATCH
                if planned == expected
                else JobProjectionReason.PLANNED_ASSETS_MISMATCH,
            )

        selected = _asset_set(row.get("selected_assets"))
        if selected:
            return _Classification(
                definition.job_key if selected == expected else "legacy",
                JobProjectionType.CURRENT_RUN
                if selected == expected
                else JobProjectionType.LEGACY_RUN,
                JobProjectionReason.SELECTED_ASSETS_MATCH
                if selected == expected
                else JobProjectionReason.SELECTED_ASSETS_MISMATCH,
            )

        derived_expected = _asset_set(row.get("expected_assets"))
        if derived_expected == expected and derived_expected:
            return _Classification(
                definition.job_key,
                JobProjectionType.CURRENT_RUN,
                JobProjectionReason.EXPECTED_ASSETS_MATCH,
            )
        return _Classification(
            definition.job_key,
            JobProjectionType.ASSUMED_CURRENT_RUN,
            JobProjectionReason.INSUFFICIENT_EVIDENCE,
        )

    def _add_not_run_rows(
        self,
        current_df: pd.DataFrame,
        all_actual_df: pd.DataFrame,
        definitions: dict[str, CurrentJobDefinition],
    ) -> pd.DataFrame:
        partition_rows = self._partition_rows(all_actual_df)
        actual_pairs = set(
            zip(current_df["job_projection"], current_df["partition"], strict=True)
        )
        phantom_rows: list[dict[str, Any]] = []
        for definition in definitions.values():
            for partition_row in partition_rows.get(definition.partition_type, []):
                partition = partition_row.get("partition")
                if (definition.job_key, partition) in actual_pairs:
                    continue
                phantom_rows.append(
                    self._phantom_row(all_actual_df, definition, partition_row)
                )
        if not phantom_rows:
            return current_df
        return pd.concat(
            [current_df, pd.DataFrame.from_records(phantom_rows)],
            ignore_index=True,
        )

    @staticmethod
    def _partition_rows(
        data_df: pd.DataFrame,
    ) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {}
        seen: set[tuple[str, object]] = set()
        for row in data_df.to_dict("records"):
            partition_type = row.get("partition_label")
            partition = row.get("partition")
            if not isinstance(partition_type, str) or pd.isna(partition):
                continue
            partition_type = _partition_type(partition_type)
            identity = (partition_type, partition)
            if identity in seen:
                continue
            seen.add(identity)
            result.setdefault(partition_type, []).append(row)
        return result

    @staticmethod
    def _phantom_row(
        data_df: pd.DataFrame,
        definition: CurrentJobDefinition,
        partition_row: dict[str, Any],
    ) -> dict[str, Any]:
        row: dict[str, Any] = {column: None for column in data_df.columns}
        for column in (
            "partition",
            "partition_prefix",
            "partition_label",
            "repoint",
            "start_time",
            "end_time",
            "start_date",
            "end_date",
        ):
            row[column] = partition_row.get(column)
        row.update(
            {
                "instrument": definition.instrument,
                "data_level": definition.data_level,
                "descriptor": definition.descriptor,
                "job_key": definition.job_key,
                "status": "not-run",
                "n_expected": len(definition.expected_assets),
                "n_materialized": 0,
                "n_skipped": 0,
                "n_missing": 0,
                "selected_assets": [],
                "planned_assets": [],
                "expected_assets": [list(path) for path in definition.expected_assets],
                "source_kind": "expectation",
                "job_projection": definition.job_key,
                "job_projection_type": JobProjectionType.NOT_RUN,
                "job_projection_reason": JobProjectionReason.EXPECTED_RUN_MISSING,
            }
        )
        return row
