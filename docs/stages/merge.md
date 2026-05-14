# Merge Stage

## Purpose

Stream Gaia staged files, combine them with Hipparcos, apply crossmatch and
override policy, and write canonical HEALPix-partitioned output.

## Inputs

- `[gaia] output_dir`
- `[hip] output_parquet`
- `[gaia-to-hip] output_parquet`
- `[overrides] output_parquet`
- `[merge] healpix_order`

## Command

```bash
uv run fis-pipeline merge build --project project.toml
```

## Outputs

Under `[merge] output_dir`:

- `healpix/{pixel}/*.parquet`
- `merge_report.json`
- `merge_decisions.parquet`

## Owning Modules

- `foundinspace.pipeline.merge.pipeline`
- `foundinspace.pipeline.merge.policy`
- `foundinspace.pipeline.merge.shards`

## Tests

- `tests/merge/test_pipeline.py`

## Learning Notes

The merge stage is where canonical identity, pair-aware overrides, winner
selection, and audit decisions come together. See
[../reference/merge-policy.md](../reference/merge-policy.md).
