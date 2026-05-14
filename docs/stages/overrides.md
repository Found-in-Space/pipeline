# Overrides Stage

## Purpose

Normalize curated YAML overrides into a merger-ready Parquet table.

## Inputs

- Packaged override YAML files in `foundinspace.pipeline.overrides.data`.
- Optional custom `[overrides] data_dir`.

## Command

```bash
uv run fis-pipeline overrides build --project project.toml
```

## Outputs

- `[overrides] output_parquet`

## Owning Modules

- `foundinspace.pipeline.overrides.loader`
- `foundinspace.pipeline.overrides.pipeline`

## Tests

- `tests/overrides/test_loader.py`
- `tests/overrides/test_overrides_prepare.py`

## Learning Notes

Overrides are the place to study how manual astronomical judgment is recorded
without hiding it from downstream audit artifacts.
