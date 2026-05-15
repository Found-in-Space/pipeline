# CLI Reference

Run commands with `uv` from the repository root:

```bash
uv run fis-pipeline --help
```

## Project

```bash
uv run fis-pipeline project init [--profile full|small] project.toml
```

Writes a starter `project.toml`. `full` is the default.

## Catalog Stages

```bash
uv run fis-pipeline gaia download queries --project project.toml
uv run fis-pipeline gaia download plan --project project.toml
uv run fis-pipeline gaia download run --project project.toml
uv run fis-pipeline gaia build --project project.toml [--force]
uv run fis-pipeline hip download --project project.toml [--force]
uv run fis-pipeline hip build --project project.toml [--force] [--limit N]
```

`gaia download queries` writes `count.adql` and `download.adql` for the small
profile, so a learner can paste the download query into the Gaia Archive web UI
and save the VOTable gzip under `[gaia] input_dir`.
`gaia download plan` writes the resumable Gaia count and batch plan.
`gaia download run` submits, resumes, downloads, and deletes Gaia archive jobs
as needed. It can be interrupted with `Ctrl-C` and rerun against the same
project file; saved job IDs and batch state are reused. `gaia build` reads
VOTables from `[gaia] input_dir`. `hip build` downloads the Hipparcos ECSV if it
is missing, then writes `[hip] output_parquet`.

## Sidecars

```bash
uv run fis-pipeline gaia-to-hip download --project project.toml [--force]
uv run fis-pipeline gaia-to-hip build --project project.toml [--force]
uv run fis-pipeline identifiers download --project project.toml [--force]
uv run fis-pipeline identifiers build --project project.toml [--force]
uv run fis-pipeline overrides build --project project.toml [--force]
```

## Merge

```bash
uv run fis-pipeline merge build --project project.toml [--force]
uv run fis-pipeline merge quality-report --project project.toml [--force]
```

Writes HEALPix-partitioned Parquet under `[merge] output_dir/healpix/`,
merge-aligned sidecars under `[merge] sidecar_output_dir`, plus
`merge_report.json` and `merge_decisions.parquet`. The quality-report command
adds `merge_quality_report.json` and `merge_quality_issues.parquet`, flagging
suspicious non-overridden rows for review.
