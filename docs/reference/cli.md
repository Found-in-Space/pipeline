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
uv run fis-pipeline gaia build --project project.toml [--force]
uv run fis-pipeline hip download --project project.toml [--force]
uv run fis-pipeline hip build --project project.toml [--force] [--limit N]
```

`gaia build` reads VOTables from `[gaia] input_dir`. `hip build` downloads the
Hipparcos ECSV if it is missing, then writes `[hip] output_parquet`.

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
```

Writes HEALPix-partitioned Parquet under `[merge] output_dir/healpix/`, plus
`merge_report.json` and `merge_decisions.parquet`.
