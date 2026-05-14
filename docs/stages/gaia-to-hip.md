# Gaia to Hipparcos Stage

## Purpose

Build the crossmatch sidecar used by the merge stage to deduplicate Gaia and
Hipparcos rows.

## Inputs

- Gaia DR3 `gaiadr3.hipparcos2_best_neighbour`, downloaded automatically when
  missing.

## Command

```bash
uv run fis-pipeline gaia-to-hip build --project project.toml
```

Use `gaia-to-hip download` when you only want the cached ECSV:

```bash
uv run fis-pipeline gaia-to-hip download --project project.toml
```

## Outputs

- `[gaia-to-hip] output_parquet`

## Owning Modules

- `foundinspace.pipeline.gaia_to_hip.download`
- `foundinspace.pipeline.gaia_to_hip.pipeline`

## Tests

- `tests/gaia_to_hip/test_download.py`
- `tests/gaia_to_hip/test_pipeline.py`

## Learning Notes

This stage is intentionally a sidecar. The dense star rows keep only canonical
identity; cross-catalog links are resolved by merge policy.
