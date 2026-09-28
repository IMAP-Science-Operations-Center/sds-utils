"""Frontend controls for logical-row projection settings."""

from collections.abc import Callable
from typing import Any

from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.projectionbase import JobProjectionMode, ProjectionSpec
from .uielem import UIElem


class ProjectionControls(UIElem):
    """Own the dashboard's global projection specification."""

    def __init__(
        self,
        projection_spec: ProjectionSpec,
        on_change: Callable[[], None],
    ) -> None:
        self.spec = projection_spec.model_copy(deep=True)
        self._on_change = on_change

    def render(self) -> None:
        """Render the job-projection selector."""
        options = {
            mode.value: mode.name.replace("_", " ").capitalize()
            for mode in JobProjectionMode
        }
        ui.select(
            options,
            value=self.spec.job_projection_mode.value,
            label="Run projection",
            on_change=self._select_mode,
        ).classes("min-w-56")

    def _select_mode(self, event: ValueChangeEventArguments[Any]) -> None:
        self.spec = self.spec.model_copy(
            update={"job_projection_mode": JobProjectionMode(event.value)}
        )
        self._on_change()
