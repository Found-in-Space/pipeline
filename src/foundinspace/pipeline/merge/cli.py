from pathlib import Path

import click

from foundinspace.pipeline.merge.pipeline import run_merge
from foundinspace.pipeline.project import load_project


@click.group(name="merge")
def cli():
    """Merge Gaia/HIP/overrides into HEALPix-partitioned Parquet output."""


def _load_project_or_die(project_path: Path, *required: str):
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        return project
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command(name="build")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build(
    project_path: Path,
    force: bool,
) -> None:
    """Run the streaming merge and emit HEALPix-partitioned outputs."""
    project = _load_project_or_die(
        project_path,
        "merge",
        "gaia",
        "hip",
        "gaia-to-hip",
        "overrides",
    )
    output_dir = project.merge.output_dir
    report = run_merge(
        gaia_dir=project.gaia.output_dir,
        hip_path=project.hip.output_parquet,
        crossmatch_path=project.gaia_to_hip.output_parquet,
        overrides_path=project.overrides.output_parquet,
        output_dir=output_dir,
        sidecar_output_dir=project.merge.sidecar_output_dir,
        healpix_order=project.merge.healpix_order,
        force=force,
    )
    click.echo(f"Wrote merged shards under {(output_dir / 'healpix').resolve()}")
    click.echo(f"Wrote sidecars under {project.merge.sidecar_output_dir.resolve()}")
    click.echo(f"Merge report: {(output_dir / 'merge_report.json').resolve()}")
    click.echo(
        "Summary: "
        f"emitted={report.rows_emitted_total:,}, "
        f"matched={report.matched_pairs_scored:,}, "
        f"unmatched_gaia={report.unmatched_gaia:,}, "
        f"unmatched_hip={report.unmatched_hip:,}"
    )
