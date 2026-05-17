# Overrides Stage

## Purpose

Normalize curated YAML overrides into a merger-ready Parquet table.

## Inputs

- Explicit `[overrides] include_files`.
- `builtin:sun.yaml` for the packaged pipeline Sun override.
- File paths may point at versioned catalog publications, such as the
  Found-In-Space manual override catalog.

## Command

```bash
uv run fis-pipeline overrides build --project project.toml
```

## Outputs

- `[overrides] output_parquet`

## Project Configuration

```toml
[overrides]
output_parquet = "data/processed/overrides.parquet"
include_files = [
  "builtin:sun.yaml",
  "../catalogs/publications/20260517.1/catalog/alpha_cen.yaml",
  "../catalogs/publications/20260517.1/catalog/binaries.yaml",
]
```

Every override source must be listed explicitly. The stage does not scan
directories or load non-Sun curated overrides from the pipeline package.

## Owning Modules

- `foundinspace.pipeline.overrides.loader`
- `foundinspace.pipeline.overrides.pipeline`

## Tests

- `tests/overrides/test_loader.py`
- `tests/overrides/test_overrides_prepare.py`

## Learning Notes

Overrides are the place to study how manual astronomical judgment is recorded
without hiding it from downstream audit artifacts.
