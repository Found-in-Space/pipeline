# Gaia Stage

## Purpose

Read Gaia VOTable batches, choose usable astrometry and photometry, normalize
coordinates, and write staged Parquet files.

## Inputs

- VOTable files in `[gaia] input_dir`.
- Optional `[gaia] mag_limit` for a local apparent-magnitude filter.

## Command

```bash
uv run fis-pipeline gaia build --project project.toml
```

## Outputs

One Parquet file per input VOTable in `[gaia] output_dir`.

## Owning Modules

- `foundinspace.pipeline.gaia.pipeline`
- `foundinspace.pipeline.gaia.astrometry`
- `foundinspace.pipeline.gaia.photometry`
- `foundinspace.pipeline.common.coords`

## Tests

- `tests/gaia/test_pipeline.py`
- `tests/gaia/test_astrometry.py`
- `tests/gaia/test_photometry.py`
- `tests/common/test_coords.py`

## Learning Notes

This is the best stage for studying noisy catalog distances, temperature
fallbacks, and epoch-normalized coordinates.
