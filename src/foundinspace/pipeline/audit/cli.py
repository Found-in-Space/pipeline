from pathlib import Path

import click

from foundinspace.pipeline.audit.pipeline import (
    default_audit_dir,
    run_audit_match,
    run_audit_report,
)
from foundinspace.pipeline.project import load_project


@click.group(name="audit")
def cli():
    """Build local crossmatch cleanup and review audit artifacts."""


def _load_project_or_die(project_path: Path, *required: str):
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        return project
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command(name="match")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
@click.option(
    "--max-sep-arcsec",
    type=float,
    default=5.0,
    show_default=True,
    help="Maximum angular separation for broad local Gaia/HIP evidence.",
)
@click.option(
    "--max-mag-delta",
    type=float,
    default=0.5,
    show_default=True,
    help="Maximum apparent-magnitude difference for broad local evidence.",
)
@click.option(
    "--auto-sep-arcsec",
    type=float,
    default=0.25,
    show_default=True,
    help="Maximum angular separation for automatic supplemental matches.",
)
@click.option(
    "--auto-mag-delta",
    type=float,
    default=0.25,
    show_default=True,
    help="Maximum apparent-magnitude difference for automatic supplemental matches.",
)
@click.option(
    "--auto-distance-frac-diff",
    type=float,
    default=0.10,
    show_default=True,
    help="Maximum fractional distance difference for automatic supplemental matches.",
)
def match_cmd(
    project_path: Path,
    force: bool,
    max_sep_arcsec: float,
    max_mag_delta: float,
    auto_sep_arcsec: float,
    auto_mag_delta: float,
    auto_distance_frac_diff: float,
) -> None:
    """Build local Gaia/HIP match evidence and supplemental crossmatch maps."""
    project = _load_project_or_die(
        project_path,
        "merge",
        "gaia",
        "hip",
        "gaia-to-hip",
        "overrides",
    )
    try:
        report = run_audit_match(
            gaia_dir=project.gaia.output_dir,
            hip_path=project.hip.output_parquet,
            official_crossmatch_path=project.gaia_to_hip.output_parquet,
            overrides_path=project.overrides.output_parquet,
            audit_dir=default_audit_dir(project.merge.output_dir),
            max_sep_arcsec=max_sep_arcsec,
            max_mag_delta=max_mag_delta,
            auto_sep_arcsec=auto_sep_arcsec,
            auto_mag_delta=auto_mag_delta,
            auto_distance_frac_diff=auto_distance_frac_diff,
            force=force,
        )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Match evidence: {Path(report.match_evidence_path).resolve()}")
    click.echo(
        f"Supplemental crossmatch: {Path(report.supplemental_crossmatch_path).resolve()}"
    )
    click.echo(
        f"Combined crossmatch: {Path(report.combined_crossmatch_path).resolve()}"
    )
    if report.distance_histogram_png_path:
        click.echo(
            f"Distance histogram: {Path(report.distance_histogram_png_path).resolve()}"
        )
    if report.distance_quality_plot_png_path:
        click.echo(
            "Distance vs quality: "
            f"{Path(report.distance_quality_plot_png_path).resolve()}"
        )
    click.echo(
        "Summary: "
        f"evidence={report.evidence_rows:,}, "
        f"supplemental={report.supplemental_rows:,}, "
        f"combined={report.combined_rows:,}"
    )


@cli.command(name="report")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def report_cmd(project_path: Path, force: bool) -> None:
    """Build octree review sidecar and manual override candidate reports."""
    project = _load_project_or_die(
        project_path,
        "merge",
        "gaia",
        "hip",
        "gaia-to-hip",
        "overrides",
        "identifiers",
    )
    audit_dir = default_audit_dir(project.merge.output_dir)
    report = run_audit_report(
        gaia_dir=project.gaia.output_dir,
        hip_path=project.hip.output_parquet,
        official_crossmatch_path=project.gaia_to_hip.output_parquet,
        overrides_path=project.overrides.output_parquet,
        identifiers_path=project.identifiers.output_parquet,
        merge_dir=project.merge.output_dir,
        sidecar_output_dir=project.merge.sidecar_output_dir,
        healpix_order=project.merge.healpix_order,
        audit_dir=audit_dir,
        force=force,
    )
    click.echo(f"Octree review: {Path(report.octree_review_path).resolve()}")
    click.echo(f"Manual candidates: {Path(report.manual_candidates_path).resolve()}")
    click.echo(
        "Summary: "
        f"octree_review={report.octree_review_rows:,}, "
        f"manual_candidates={report.manual_candidate_rows:,}"
    )
