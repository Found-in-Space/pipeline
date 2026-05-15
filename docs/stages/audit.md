# Audit Stage

## Purpose

Build local Gaia/Hipparcos cleanup artifacts for bright-star inspection runs:
supplemental crossmatches, octree review sidecars, and manual override queues.

## Inputs

- `[gaia] output_dir`
- `[hip] output_parquet`
- `[gaia-to-hip] output_parquet`
- `[overrides] output_parquet`
- `[identifiers] output_parquet` for reports
- `[merge] output_dir`

## Commands

Build local close-pair evidence and conservative supplemental crossmatch maps:

```bash
uv run --group audit fis-pipeline audit match --project project.toml
```

Use the combined map for a cleaned merge:

```bash
uv run fis-pipeline merge build --project project.toml \
  --crossmatch-path data/processed/merged/audit/combined_gaia_hip_map.parquet --force
```

After the cleaned merge, write display and manual-review reports:

```bash
uv run --group audit fis-pipeline audit report --project project.toml --force
```

## Outputs

Under `[merge] output_dir/audit`:

- `match_evidence.parquet`
- `supplemental_gaia_hip_map.parquet`
- `combined_gaia_hip_map.parquet`
- `distance_pct_histogram.png`
- `distance_pct_histogram.svg`
- `distance_pct_histogram_bins.csv`
- `distance_pct_vs_astrometry_quality.png`
- `distance_pct_vs_astrometry_quality.svg`
- `distance_quality_summary.csv`
- `distance_threshold_summary.csv`
- `distance_threshold_summary.json`
- `audit_match_report.json`
- `octree_review.parquet`
- `manual_override_candidates.parquet`
- `manual_override_candidates.csv`
- `audit_report.json`

Under `[merge] sidecar_output_dir`:

- `octree_review/{pixel}/*.parquet`

## Learning Notes

The audit stage keeps automatic cleanup conservative. It auto-matches only
one-to-one, non-conflicting, non-overridden Gaia/HIP close pairs. Pairs are
accepted automatically when they are either extremely close in sky position and
apparent magnitude, or when their distances agree within 10%. Conflicts,
ambiguous many-neighbour cases, and extreme physical rows are routed to review
artifacts instead of becoming automatic overrides.
