# Project Profiles

Profiles are starter `project.toml` templates. They do not contain catalog data.

```bash
uv run fis-pipeline project init --profile full project.toml
uv run fis-pipeline project init --profile small project-small.toml
```

The templates are checked in under [../profiles](../profiles).

## `full`

`full` is the default. It uses the conventional local layout:

- raw downloads under `data/catalogs/`
- processed catalog outputs under `data/processed/`
- Gaia VOTable input under `data/catalogs/gaia`
- merged output under `data/processed/merged`

This is the profile used by:

```bash
uv run fis-pipeline project init project.toml
```

## `small`

`small` is for learning and quick iteration with real data. It keeps all paths
under `data/catalogs/` and `data/processed/`, sets `[gaia] mag_limit = 8.0`, and
uses a separate Gaia input/output directory so small runs do not collide with
full local outputs.

The non-Gaia pieces are already automated:

```bash
uv run fis-pipeline hip build --project project-small.toml
uv run fis-pipeline gaia-to-hip build --project project-small.toml
uv run fis-pipeline identifiers build --project project-small.toml
uv run fis-pipeline overrides build --project project-small.toml
```

Gaia download automation is planned next. When that exists, it should write the
small Gaia VOTables to `[gaia] input_dir`. Until then, put Gaia VOTables there
manually and run:

```bash
uv run fis-pipeline gaia build --project project-small.toml
uv run fis-pipeline merge build --project project-small.toml
```

No sample catalog data should be committed for either profile.
