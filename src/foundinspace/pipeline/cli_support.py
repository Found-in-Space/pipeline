"""Shared Click helpers for project-backed pipeline commands."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import click

from foundinspace.pipeline.project import (
    PipelineProject,
    load_project,
    validate_controlled_project,
)
from foundinspace.pipeline.provenance import ProvenanceError, record_command

project_option = click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)


def load_project_or_die(
    project_path: Path,
    *required: str,
    validate_controlled: bool = True,
) -> PipelineProject:
    """Load a project and translate configuration errors for Click."""
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        if validate_controlled and project.run.is_configured:
            validate_controlled_project(project)
        return project
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


def require_input_file(path: Path, *, label: str) -> Path:
    """Require a regular build input without attempting acquisition."""
    if not path.is_file():
        raise click.ClickException(
            f"{label} does not exist: {path}. Run the matching download command first."
        )
    return path


@contextmanager
def record_command_or_die(
    project: PipelineProject,
    *,
    command: str,
    parameters: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Path | Sequence[Path]] | None = None,
    outputs: Mapping[str, Path | Sequence[Path]] | None = None,
) -> Iterator[None]:
    """Record a command and render provenance failures as normal CLI errors."""
    try:
        with record_command(
            project,
            command=command,
            parameters=parameters,
            inputs=inputs,
            outputs=outputs,
        ):
            yield
    except ProvenanceError as exc:
        raise click.ClickException(str(exc)) from exc
