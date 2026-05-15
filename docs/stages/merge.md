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

For curated catalog releases, pass a combined Gaia-HIP map:

```bash
uv run fis-pipeline merge build --project project.toml \
  --crossmatch-path path/to/combined_gaia_hip_map.parquet
```

After a merge, write the post-merge quality audit:

```bash
uv run fis-pipeline merge quality-report --project project.toml
```

## Outputs

Under `[merge] output_dir`:

- `healpix/{pixel}/*.parquet`
- `merge_report.json`
- `merge_decisions.parquet`
- `merge_quality_report.json`
- `merge_quality_issues.parquet`

Under `[merge] sidecar_output_dir`, or a sibling `sidecars` directory when not
set:

- `gaia_enrichment/{pixel}/*.parquet`
- `motion/{pixel}/*.parquet`
- `mass/{pixel}/*.parquet`

## Owning Modules

- `foundinspace.pipeline.merge.pipeline`
- `foundinspace.pipeline.merge.policy`
- `foundinspace.pipeline.merge.quality_report`
- `foundinspace.pipeline.merge.shards`
- `foundinspace.pipeline.merge.sidecars`

## Tests

- `tests/merge/test_pipeline.py`
- `tests/merge/test_quality_report.py`

## Learning Notes

The merge stage is where canonical identity, pair-aware overrides, winner
selection, and audit decisions come together. See
[../reference/merge-policy.md](../reference/merge-policy.md).
