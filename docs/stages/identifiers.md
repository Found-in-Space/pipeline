# Identifiers Stage

## Purpose

Build a sparse sidecar for display identifiers such as HIP, HD, Bayer,
Flamsteed, constellation, Gaia DR3 ID, and proper name.

## Inputs

- Vizier identifier source catalogs, downloaded automatically when missing.
- Optional Gaia-to-Hipparcos crossmatch Parquet if it already exists.
- Optional override YAML identifier blocks.

## Command

```bash
uv run fis-pipeline identifiers build --project project.toml
```

Use `identifiers download` when you only want the cached ECSV files:

```bash
uv run fis-pipeline identifiers download --project project.toml
```

## Outputs

- `[identifiers] output_parquet`

## Owning Modules

- `foundinspace.pipeline.identifiers.download`
- `foundinspace.pipeline.identifiers.pipeline`

## Tests

- `tests/identifiers/test_pipeline.py`

## Learning Notes

The sidecar is deliberately sparse so billions of dense star rows do not carry
mostly empty human-label columns.
