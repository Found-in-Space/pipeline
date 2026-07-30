from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
)
from foundinspace.pipeline.gaia.download.fieldsets import load_gaia_field_sets
from foundinspace.pipeline.merge.pipeline import run_merge
from foundinspace.pipeline.merge.quality_report import run_quality_report


@click.group(name="merge")
def cli():
    """Merge Gaia/HIP/overrides into HEALPix-partitioned Parquet output."""


@cli.command(name="build")
@project_option
@click.option(
    "--crossmatch-path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Override Gaia/HIP crossmatch path; defaults to [gaia-to-hip] output.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build(
    project_path: Path,
    crossmatch_path: Path | None,
    force: bool,
) -> None:
    """Run the streaming merge and emit HEALPix-partitioned outputs."""
    project = load_project_or_die(
        project_path,
        "merge",
        "gaia",
        "hip",
        "gaia-to-hip",
        "overrides",
    )
    output_dir = project.merge.output_dir
    configured_crossmatch = project.gaia_to_hip.output_parquet
    if (
        crossmatch_path is not None
        and crossmatch_path.resolve() != configured_crossmatch.resolve()
    ):
        raise click.ClickException(
            "--crossmatch-path may no longer override "
            "[gaia-to-hip].output_parquet; configure the accepted map in the "
            "project so merge, identifiers, and quality reporting use one input"
        )
    if crossmatch_path is not None:
        click.echo(
            "Warning: --crossmatch-path is deprecated; the configured "
            "[gaia-to-hip] output is authoritative.",
            err=True,
        )
    carry_fields = None
    if project.gaia_download.configured:
        carry_fields = load_gaia_field_sets(project.gaia_download.carry_field_sets)

    with record_command_or_die(
        project,
        command="merge build",
        parameters={
            "force": force,
            "healpix_order": project.merge.healpix_order,
        },
        inputs={
            "gaia_staged": project.gaia.output_dir,
            "hipparcos_staged": project.hip.output_parquet,
            "accepted_crossmatch": configured_crossmatch,
            "overrides": project.overrides.output_parquet,
        },
        outputs={
            "merged": output_dir,
            "sidecars": project.merge.sidecar_output_dir,
        },
    ):
        report = run_merge(
            gaia_dir=project.gaia.output_dir,
            hip_path=project.hip.output_parquet,
            crossmatch_path=configured_crossmatch,
            overrides_path=project.overrides.output_parquet,
            output_dir=output_dir,
            sidecar_output_dir=project.merge.sidecar_output_dir,
            carry_fields=carry_fields,
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
@project_option
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
    project = load_project_or_die(
        project_path,
        "merge",
        "gaia",
        "hip",
        "gaia-to-hip",
        "overrides",
        "identifiers",
    )
    report_path = project.merge.output_dir / "merge_quality_report.json"
    with record_command_or_die(
        project,
        command="merge quality-report",
        parameters={
            "force": force,
            "distance_disagreement_threshold": distance_disagreement_threshold,
            "ruwe_threshold": ruwe_threshold,
        },
        inputs={
            "gaia_staged": project.gaia.output_dir,
            "hipparcos_staged": project.hip.output_parquet,
            "accepted_crossmatch": project.gaia_to_hip.output_parquet,
            "overrides": project.overrides.output_parquet,
            "merged": project.merge.output_dir,
            "identifiers": project.identifiers.output_parquet,
        },
        outputs={
            "quality_report": report_path,
            "quality_issues": project.merge.output_dir / "merge_quality_issues.parquet",
        },
    ):
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
    click.echo(f"Quality report: {report_path.resolve()}")
    click.echo(f"Quality issues: {Path(report.issues_path).resolve()}")
    click.echo(
        "Summary: "
        f"issues={report.total_issues:,}, "
        f"matched_pair={report.matched_pair_issues:,}, "
        f"merged_row={report.merged_row_issues:,}"
    )
