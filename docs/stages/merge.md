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

To merge with an audit-produced Gaia/HIP map:

```bash
uv run fis-pipeline merge build --project project.toml \
  --crossmatch-path data/processed/merged/audit/combined_gaia_hip_map.parquet
```

After a merge, write the post-merge quality audit:

```bash
uv run fis-pipeline merge quality-report --project project.toml
```

To also scan for likely unresolved Gaia/Hipparcos duplicates, install or run
with the optional audit dependency group:

```bash
uv run --group audit fis-pipeline merge quality-report --project project.toml \
  --include-close-pairs --force
```

## Outputs

Under `[merge] output_dir`:

- `healpix/{pixel}/*.parquet`
- `merge_report.json`
- `merge_decisions.parquet`
- `merge_quality_report.json`
- `merge_quality_issues.parquet`

The close-pair audit flags non-overridden Gaia/Hipparcos rows that are near each
other on the sky, have similar reconstructed apparent magnitudes, and are not
present as an exact pair in the Gaia-HIP crossmatch.

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
