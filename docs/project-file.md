# Project File

Pipeline commands require a TOML project file:

```bash
uv run fis-pipeline project init --profile small project.toml
```

The file is the single source of truth for catalog paths, processed outputs,
and merge settings. Paths may be absolute or relative to the project file
directory. Environment-variable syntax is rejected so runs are reproducible.

## Sections

- `[gaia]`: `input_dir`, `output_dir`, optional `mag_limit`.
- `[gaia_download]`: optional Gaia downloader mode, access, state, batching,
  and carry-through field-set configuration.
- `[gaia-to-hip]`: `download_ecsv`, `output_parquet`.
- `[hip]`: `download_ecsv`, `output_parquet`.
- `[identifiers]`: `hip_hd_ecsv`, `iv27a_catalog_ecsv`,
  `iv27a_proper_names_ecsv`, `output_parquet`.
- `[overrides]`: `output_parquet`, `include_files`.
- `[merge]`: `output_dir`, `healpix_order`, optional `sidecar_output_dir`.

Only the sections needed by a command must be present. Missing sections fail
when the command accesses them, not when the project is loaded.

See [reference/project-toml.md](reference/project-toml.md) for the full contract.
