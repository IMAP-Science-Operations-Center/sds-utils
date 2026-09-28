"""Derive logical job keys from Dagster job names or selected assets."""

import logging
import re
from collections.abc import Collection
from functools import lru_cache
from typing import NamedTuple

import httpx
import yaml

logger = logging.getLogger(__name__)

_DEPENDENCIES_URL = (
    "https://raw.githubusercontent.com/IMAP-Science-Operations-Center/"
    "sds-data-manager/dev/sds_data_manager/orchestration/dependencies/"
    "imap_{instrument}_dependencies.yaml"
)
_JOB_NAME_PATTERN = re.compile(
    r"^(?P<instrument>[^_]+)_(?P<data_level>[^_]+)_"
    r"(?P<descriptor>[^_]+)(?:_.+)?$"
)
_YAML_JOB_PATTERN = re.compile(r"^\(([^,]+),\s*([^)]+)\)$")
_INSTRUMENT_PATTERN = re.compile(r"^[a-z][a-z0-9-]*$")
_ASSET_COMPONENT_COUNT = 3
_L0_DESCRIPTOR = "raw"


class JobKeyParts(NamedTuple):
    """The inferred logical job identity and its component fields."""

    job_key: str | None
    instrument: str | None
    data_level: str | None
    descriptor: str | None


class CurrentJobDefinition(NamedTuple):
    """Normalized current job identity and output asset set."""

    job_key: str
    instrument: str
    data_level: str
    descriptor: str
    partition_type: str
    expected_assets: frozenset[tuple[str, ...]]


def _asset_parts(path: list[str]) -> tuple[str, str] | None:
    if len(path) != 1:
        return None
    parts = path[0].split("_", _ASSET_COMPONENT_COUNT - 1)
    if len(parts) != _ASSET_COMPONENT_COUNT or not all(parts):
        return None
    return parts[0], parts[1]


def _contains_asset(value: object, expected: tuple[str, str, str]) -> bool:
    """Return whether a nested dependency value contains an asset reference."""
    if isinstance(value, dict):
        identity = tuple(
            value.get(field) for field in ("source", "data_type", "descriptor")
        )
        if identity == expected:
            return True
        return any(_contains_asset(child, expected) for child in value.values())
    if isinstance(value, list):
        return any(_contains_asset(child, expected) for child in value)
    return False


@lru_cache(maxsize=32)
def _dependencies_for_instrument(instrument: str) -> dict[str, object]:
    """Load and cache one instrument's current dependency YAML."""
    if not _INSTRUMENT_PATTERN.fullmatch(instrument):
        return {}
    url = _DEPENDENCIES_URL.format(instrument=instrument)
    try:
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        dependencies: object = yaml.safe_load(response.text)
    except (httpx.HTTPError, yaml.YAMLError) as error:
        logger.warning("Could not load job definitions for %s: %s", instrument, error)
        return {}

    if not isinstance(dependencies, dict):
        logger.warning("Unexpected job definitions for %s at %s", instrument, url)
        return {}
    return {str(key): value for key, value in dependencies.items()}


@lru_cache(maxsize=32)
def _job_outputs_for_instrument(
    instrument: str,
) -> dict[frozenset[str], tuple[str, str] | None]:
    """Load and cache output sets from the current dev dependency YAML."""
    output_sets: dict[frozenset[str], tuple[str, str] | None] = {}
    dependencies = _dependencies_for_instrument(instrument)
    for job_name, spec in dependencies.items():
        match = _YAML_JOB_PATTERN.fullmatch(job_name)
        if match is None or not isinstance(spec, dict):
            continue
        outputs = spec.get("outputs")
        if not isinstance(outputs, list):
            continue
        names = frozenset(
            f"{output['source']}_{output['data_type']}_{output['descriptor']}".replace(
                "-", ""
            )
            for output in outputs
            if isinstance(output, dict)
            and all(
                isinstance(output.get(field), str)
                for field in ("source", "data_type", "descriptor")
            )
        )
        if not names:
            continue
        identity = (match.group(1).strip(), match.group(2).strip().replace("-", ""))
        if names in output_sets and output_sets[names] != identity:
            output_sets[names] = None
        else:
            output_sets[names] = identity
    return output_sets


def current_job_definitions(
    instruments: Collection[str],
) -> dict[str, CurrentJobDefinition]:
    """Load normalized current job definitions for the requested instruments."""
    definitions: dict[str, CurrentJobDefinition] = {}
    for instrument in sorted(set(instruments)):
        l0_partition_types: set[str] = set()
        for job_name, spec in _dependencies_for_instrument(instrument).items():
            match = _YAML_JOB_PATTERN.fullmatch(job_name)
            if match is None or not isinstance(spec, dict):
                continue
            partition_type = spec.get("partition")
            outputs = spec.get("outputs")
            if not isinstance(partition_type, str) or not isinstance(outputs, list):
                continue
            if _contains_asset(
                spec.get("inputs"),
                (instrument, "l0", _L0_DESCRIPTOR),
            ):
                l0_partition_types.add(partition_type)
            expected_assets = frozenset(
                (
                    (
                        f"{output['source']}_{output['data_type']}_"
                        f"{output['descriptor']}"
                    ).replace("-", ""),
                )
                for output in outputs
                if isinstance(output, dict)
                and all(
                    isinstance(output.get(field), str)
                    for field in ("source", "data_type", "descriptor")
                )
            )
            if not expected_assets:
                continue
            data_level = match.group(1).strip()
            descriptor = match.group(2).strip().replace("-", "")
            job_key = f"{instrument}_{data_level}_{descriptor}"
            definition = CurrentJobDefinition(
                job_key=job_key,
                instrument=instrument,
                data_level=data_level,
                descriptor=descriptor,
                partition_type=partition_type,
                expected_assets=expected_assets,
            )
            existing = definitions.get(job_key)
            if existing is not None and existing != definition:
                raise ValueError(f"Ambiguous current job definition: {job_key}")
            definitions[job_key] = definition
        if len(l0_partition_types) > 1:
            logger.warning(
                "Current jobs disagree about the partition type for %s_l0_raw: %s",
                instrument,
                sorted(l0_partition_types),
            )
        elif l0_partition_types:
            partition_type = l0_partition_types.pop()
            job_key = f"{instrument}_l0_none"
            definitions[job_key] = CurrentJobDefinition(
                job_key=job_key,
                instrument=instrument,
                data_level="l0",
                descriptor="none",
                partition_type=partition_type,
                expected_assets=frozenset({(f"{instrument}_l0_raw",)}),
            )
    return definitions


def derive_job_key(
    job_name: str | None, selected_assets: list[list[str]]
) -> JobKeyParts:
    """Prefer a named job, then exact YAML outputs, then asset-name inference."""
    if job_name and job_name != "__ASSET_JOB":
        match = _JOB_NAME_PATTERN.fullmatch(job_name)
        if match is not None:
            instrument, data_level, descriptor = match.groups()
            return JobKeyParts(
                f"{instrument}_{data_level}_{descriptor}",
                instrument,
                data_level,
                descriptor,
            )

    return _derive_from_assets(selected_assets)


def parse_job_key(job_key: str | None) -> JobKeyParts:
    """Split a stored job key into its dashboard identity fields."""
    if job_key is None:
        return JobKeyParts(None, None, None, None)
    match job_key.split("_", 2):
        case [instrument, data_level]:
            return JobKeyParts(job_key, instrument, data_level, None)
        case [instrument, data_level, descriptor]:
            return JobKeyParts(job_key, instrument, data_level, descriptor)
        case _:
            return JobKeyParts(job_key, None, None, None)


def _derive_from_assets(selected_assets: list[list[str]]) -> JobKeyParts:
    if not selected_assets:
        return JobKeyParts(None, None, None, None)
    parsed_assets = [_asset_parts(path) for path in selected_assets]
    if any(parts is None for parts in parsed_assets):
        return JobKeyParts(None, None, None, None)
    asset_parts = [parts for parts in parsed_assets if parts is not None]
    instruments = {parts[0] for parts in asset_parts}
    if len(instruments) != 1:
        return JobKeyParts(None, None, None, None)
    instrument = instruments.pop()

    selected_names = frozenset(path[0] for path in selected_assets)
    job_identity = _job_outputs_for_instrument(instrument).get(selected_names)
    if job_identity is not None:
        data_level, descriptor = job_identity
        return JobKeyParts(
            f"{instrument}_{data_level}_{descriptor}",
            instrument,
            data_level,
            descriptor,
        )

    levels = [level for _, level in asset_parts if level != "ancillary"]
    if not levels:
        return JobKeyParts(None, instrument, None, None)
    data_level = min(levels)
    return JobKeyParts(f"{instrument}_{data_level}", instrument, data_level, None)
