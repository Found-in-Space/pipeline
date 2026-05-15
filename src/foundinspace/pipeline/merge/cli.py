from pathlib import Path

import click

from foundinspace.pipeline.merge.pipeline import run_merge
from foundinspace.pipeline.merge.quality_report import run_quality_report
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


@cli.command(name="quality-report")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
@click.option(
    "--distance-disagreement-threshold",
    type=float,
    default=0.25,
    show_default=True,
    help="Matched-pair fractional distance disagreement threshold.",
)
@click.option(
    "--ruwe-threshold",
    type=float,
    default=1.4,
    show_default=True,
    help="Gaia RUWE threshold used for suspicious matched-pair reports.",
)
def quality_report(
    project_path: Path,
    force: bool,
    distance_disagreement_threshold: float,
    ruwe_threshold: float,
) -> None:
    """Audit merged output for suspicious non-overridden stars."""
    project = _load_project_or_die(
        project_path,
        "merge",
        "gaia",
        "hip",
        "gaia-to-hip",
        "overrides",
        "identifiers",
    )
    report = run_quality_report(
        gaia_dir=project.gaia.output_dir,
        hip_path=project.hip.output_parquet,
        crossmatch_path=project.gaia_to_hip.output_parquet,
        overrides_path=project.overrides.output_parquet,
        merge_dir=project.merge.output_dir,
        identifiers_path=project.identifiers.output_parquet,
        distance_disagreement_threshold=distance_disagreement_threshold,
        ruwe_threshold=ruwe_threshold,
        force=force,
    )
    report_path = project.merge.output_dir / "merge_quality_report.json"
    click.echo(f"Quality report: {report_path.resolve()}")
    click.echo(f"Quality issues: {Path(report.issues_path).resolve()}")
    click.echo(
        "Summary: "
        f"issues={report.total_issues:,}, "
        f"matched_pair={report.matched_pair_issues:,}, "
        f"merged_row={report.merged_row_issues:,}"
    )
