"""Frontend controls for logical-row projection settings."""

from collections.abc import Callable
from typing import Any

from nicegui.events import ValueChangeEventArguments

from ..backend.projectionbase import JobProjectionMode, ProjectionSpec
from .elems import Select
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
        Select(
            options,
            value=self.spec.job_projection_mode.value,
            label="Run projection",
            on_change=self._select_mode,
            help=(
                'Set the "Run projection" behavior.\n'
                '- "All runs": Don\'t do run projection.\n'
                '- "Latest jobs only": Project runs onto the latest job definitions '
                "(from dependency yaml files)\n"
                '- "Legacy runs only": Show only runs that could not be projected '
                "onto a job definition"
            ),
        ).build().classes("min-w-56")

    def _select_mode(self, event: ValueChangeEventArguments[Any]) -> None:
        self.spec = self.spec.model_copy(
            update={"job_projection_mode": JobProjectionMode(event.value)}
        )
        self._on_change()
