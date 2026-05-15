# Gaia Stage

## Purpose

Read Gaia VOTable batches, choose usable astrometry and photometry, normalize
coordinates, preserve configured Gaia carry-through fields, and write staged
Parquet files.

## Inputs

- VOTable files in `[gaia] input_dir`.
- Optional `[gaia] mag_limit` for a local apparent-magnitude filter.
- Optional `[gaia_download] carry_field_sets` for enrichment sidecar columns.
  The small profile leaves this empty; the full profile enables `motion` and
  `mass`.

## Command

```bash
uv run fis-pipeline gaia download queries --project project.toml
uv run fis-pipeline gaia download plan --project project.toml
uv run fis-pipeline gaia download run --project project.toml
uv run fis-pipeline gaia build --project project.toml
```

Use `gaia download queries` for the small-profile browser flow. It writes ADQL
files only; the user downloads the VOTable from the Gaia Archive web UI and
places it under `[gaia] input_dir`.

## Outputs

One Parquet file per input VOTable in `[gaia] output_dir`. Dense columns remain
stable; configured carry-through fields are written as nullable `gaia_*`
columns for merge sidecars.

## Owning Modules

- `foundinspace.pipeline.gaia.pipeline`
- `foundinspace.pipeline.gaia.astrometry`
- `foundinspace.pipeline.gaia.photometry`
- `foundinspace.pipeline.gaia.download`
- `foundinspace.pipeline.common.coords`

## Tests

- `tests/gaia/test_pipeline.py`
- `tests/gaia/test_download_fieldsets_query.py`
- `tests/gaia/test_download_planner_state.py`
- `tests/gaia/test_download_runner.py`
- `tests/gaia/test_astrometry.py`
- `tests/gaia/test_photometry.py`
- `tests/common/test_coords.py`

## Learning Notes

This is the best stage for studying noisy catalog distances, temperature
fallbacks, and epoch-normalized coordinates.
