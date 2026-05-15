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
- Optional `[merge] sidecar_output_dir`

## Command

```bash
uv run fis-pipeline merge build --project project.toml
```

## Outputs

Under `[merge] output_dir`:

- `healpix/{pixel}/*.parquet`
- `merge_report.json`
- `merge_decisions.parquet`

Under `[merge] sidecar_output_dir`, or a sibling `sidecars` directory when not
set:

- `gaia_enrichment/{pixel}/*.parquet`
- `motion/{pixel}/*.parquet`
- `mass/{pixel}/*.parquet`

## Owning Modules

- `foundinspace.pipeline.merge.pipeline`
- `foundinspace.pipeline.merge.policy`
- `foundinspace.pipeline.merge.shards`
- `foundinspace.pipeline.merge.sidecars`

## Tests

- `tests/merge/test_pipeline.py`

## Learning Notes

The merge stage is where canonical identity, pair-aware overrides, winner
selection, and audit decisions come together. See
[../reference/merge-policy.md](../reference/merge-policy.md).
