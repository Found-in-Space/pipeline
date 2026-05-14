# Start Here

The pipeline reads catalog files, prepares per-catalog Parquet outputs, applies
manual corrections, and writes a canonical merged table partitioned by HEALPix.

## Install

```bash
uv sync
uv run fis-pipeline --help
```

## Create a Project File

For a beginner-friendly run:

```bash
uv run fis-pipeline project init --profile small project.toml
```

For the conventional full local layout:

```bash
uv run fis-pipeline project init project.toml
```

The generated file uses paths relative to the project file location. See
[project-file.md](project-file.md) and [profiles.md](profiles.md).

## Current Small-Profile Flow

The non-Gaia catalog sources are small and automated:

```bash
uv run fis-pipeline hip build --project project.toml
uv run fis-pipeline gaia-to-hip build --project project.toml
uv run fis-pipeline identifiers build --project project.toml
uv run fis-pipeline overrides build --project project.toml
```

Gaia VOTable download automation is intentionally the next step. Until then,
place Gaia `.vot`, `.vot.gz`, or `.vot.xz` files in `[gaia] input_dir`, then run:

```bash
uv run fis-pipeline gaia build --project project.toml
uv run fis-pipeline merge build --project project.toml
```

## Learn by Stage

Read the stage pages in order if you want to understand or modify the pipeline:

1. [Gaia](stages/gaia.md)
2. [Hipparcos](stages/hipparcos.md)
3. [Gaia to Hipparcos](stages/gaia-to-hip.md)
4. [Identifiers](stages/identifiers.md)
5. [Overrides](stages/overrides.md)
6. [Merge](stages/merge.md)
