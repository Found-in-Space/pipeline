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

## Small-Profile Flow

Build the small non-Gaia catalog sources:

```bash
uv run fis-pipeline hip build --project project.toml
uv run fis-pipeline gaia-to-hip build --project project.toml
uv run fis-pipeline identifiers build --project project.toml
uv run fis-pipeline overrides build --project project.toml
```

Download and stage the small Gaia sample:

```bash
uv run fis-pipeline gaia download plan --project project.toml
uv run fis-pipeline gaia download run --project project.toml
uv run fis-pipeline gaia build --project project.toml
```

For a browser-assisted first Gaia run, generate the ADQL instead:

```bash
uv run fis-pipeline gaia download queries --project project.toml
```

Paste the generated `download.adql` into the Gaia Archive web UI, download a
VOTable gzip, place it under `[gaia] input_dir`, and then run `gaia build`.

Merge the staged catalogs:

```bash
uv run fis-pipeline merge build --project project.toml
```

The small profile writes dense HEALPix shards from core fields. The full profile
also carries Gaia enrichment into motion and mass sidecars. For Gaia download
details, credentials, and resume behavior, see
[operations/gaia-download.md](operations/gaia-download.md).

## Learn by Stage

Read the stage pages in order if you want to understand or modify the pipeline:

1. [Gaia](stages/gaia.md)
2. [Hipparcos](stages/hipparcos.md)
3. [Gaia to Hipparcos](stages/gaia-to-hip.md)
4. [Identifiers](stages/identifiers.md)
5. [Overrides](stages/overrides.md)
6. [Merge](stages/merge.md)
