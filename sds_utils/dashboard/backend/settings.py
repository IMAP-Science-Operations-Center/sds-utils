"""Typed persistent settings for the dashboard interface."""

from collections.abc import Mapping
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from .aggbase import AggPreset, AggSpec
from .filtersbase import FilterArguments
from .projectionbase import ProjectionSpec


def _default_agg_specs() -> dict[AggPreset, AggSpec]:
    """Create a default aggregation specification for every preset."""
    return {preset: AggSpec(preset=preset) for preset in AggPreset}


class DashboardSettings(BaseModel):
    """Projection, aggregation, and filtering choices persisted for one user."""

    model_config = ConfigDict(extra="forbid")

    active_agg_preset: AggPreset = AggPreset.RAW
    agg_specs: dict[AggPreset, AggSpec] = Field(default_factory=_default_agg_specs)
    projection: ProjectionSpec = Field(default_factory=ProjectionSpec)
    filtering: FilterArguments = Field(default_factory=dict)

    @field_validator("agg_specs", mode="before")
    @classmethod
    def _validate_agg_specs(cls, value: object) -> dict[AggPreset, AggSpec]:
        """Validate each preset independently and default malformed entries."""
        defaults = _default_agg_specs()
        if not isinstance(value, Mapping):
            return defaults
        validated: dict[AggPreset, AggSpec] = {}
        for preset, default in defaults.items():
            candidate = value.get(preset, value.get(preset.value, default))
            try:
                spec = TypeAdapter(AggSpec).validate_python(candidate)
                if spec.preset is not preset:
                    raise ValueError("Aggregation preset does not match mapping key")
                validated[preset] = spec
            except (TypeError, ValidationError, ValueError):
                validated[preset] = default
        return validated

    @classmethod
    def from_stored(cls, value: object) -> Self:
        """Validate stored fields independently, defaulting invalid fields."""
        defaults = cls()
        if not isinstance(value, Mapping):
            return defaults
        try:
            return cls.model_validate(value)
        except ValidationError as error:
            if any(item["type"] == "extra_forbidden" for item in error.errors()):
                return defaults

        validated: dict[str, object] = {}
        for name, field in cls.model_fields.items():
            default = getattr(defaults, name)
            try:
                validated[name] = TypeAdapter(
                    field.rebuild_annotation()
                ).validate_python(value.get(name, default))
            except (TypeError, ValidationError, ValueError):
                validated[name] = default
        return cls.model_validate(validated)
