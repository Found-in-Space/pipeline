# `project.toml` Reference

`format_version = 1` is the only supported project format.

## `[gaia]`

- `input_dir`: directory containing `.vot`, `.vot.gz`, or `.vot.xz` files.
- `output_dir`: directory for staged Gaia Parquet files.
- `mag_limit`: optional apparent Gaia G magnitude limit.

## `[gaia_download]`

Optional unless you use `fis-pipeline gaia download ...`.

- `mode`: `small` or `full`.
- `access`: `auto`, `anonymous`, or `authenticated`.
- `mag_limit`: Gaia Archive G magnitude limit. Required for `small`; omitted
  for `full`.
- `state_db`: local SQLite path for counts, batch plans, jobs, and resume state.
- `row_cap`: target maximum rows per authenticated batch.
- `max_active_jobs`: maximum authenticated Gaia jobs to keep in flight.
- `carry_field_sets`: checked-in Gaia field sets to preserve for sidecars.

## `[gaia-to-hip]`

- `download_ecsv`: cached Gaia DR3 `hipparcos2_best_neighbour` ECSV path.
- `output_parquet`: processed crossmatch sidecar path.

## `[hip]`

- `download_ecsv`: cached Hipparcos New Reduction ECSV path.
- `output_parquet`: processed Hipparcos star table path.

## `[identifiers]`

- `hip_hd_ecsv`: cached HIP-to-HD catalog path.
- `iv27a_catalog_ecsv`: cached Bayer/Flamsteed catalog path.
- `iv27a_proper_names_ecsv`: cached proper-name catalog path.
- `output_parquet`: processed identifier sidecar path.

## `[overrides]`

- `output_parquet`: processed override table path.
- `data_dir`: optional custom override YAML directory. If omitted, packaged
  overrides are used.

## `[merge]`

- `output_dir`: directory for merged shards and reports.
- `healpix_order`: non-negative HEALPix order for merged output partitioning.
- `sidecar_output_dir`: optional directory for merge-aligned sidecars. If
  omitted, defaults to `sidecars` beside `[merge] output_dir`.
