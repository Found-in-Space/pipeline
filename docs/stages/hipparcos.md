# Hipparcos Stage

## Purpose

Download the Hipparcos New Reduction catalog, process its astrometry and
photometry, and write a staged Parquet file.

## Inputs

- `[hip] download_ecsv`, downloaded automatically when missing.

## Command

```bash
uv run fis-pipeline hip build --project project.toml
```

Use `hip download` when you only want the cached ECSV:

```bash
uv run fis-pipeline hip download --project project.toml
```

## Outputs

- `[hip] output_parquet`

## Owning Modules

- `foundinspace.pipeline.hipparcos.download`
- `foundinspace.pipeline.hipparcos.pipeline`
- `foundinspace.pipeline.hipparcos.astrometry`
- `foundinspace.pipeline.hipparcos.photometry`

## Tests

- `tests/hipparcos/test_pipeline.py`

## Learning Notes

Hipparcos is small enough to process in one pass, so it is a friendly place to
learn the staged-output schema before looking at streaming Gaia batches.
