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
under `data/catalogs/` and `data/processed/`, sets the Gaia download and local
build magnitude limits to `G <= 9`, and uses a separate Gaia input/output
directory so small runs do not collide with full local outputs.

The non-Gaia pieces are already automated:

```bash
uv run fis-pipeline hip build --project project-small.toml
uv run fis-pipeline gaia-to-hip build --project project-small.toml
uv run fis-pipeline identifiers build --project project-small.toml
uv run fis-pipeline overrides build --project project-small.toml
```

Gaia download automation writes VOTables to `[gaia] input_dir`:

```bash
uv run fis-pipeline gaia download plan --project project-small.toml
uv run fis-pipeline gaia download run --project project-small.toml
uv run fis-pipeline gaia build --project project-small.toml
uv run fis-pipeline merge build --project project-small.toml
```

For a browser-based first run, generate ADQL without submitting a TAP job from
Python:

```bash
uv run fis-pipeline gaia download queries --project project-small.toml
```

Paste the generated `download.adql` into the Gaia Archive web UI, download the
result as a VOTable gzip, and place it under `[gaia] input_dir`. The same
`gaia build` and `merge build` commands then apply.

Gaia anonymous async jobs can fail when the Archive's anonymous storage quota is
already full, even for a small query. If that happens, retry later or use an
authenticated Gaia Archive account with `GAIA_CREDENTIALS_FILE` or
`GAIA_USER`/`GAIA_PASS`.

No sample catalog data should be committed for either profile.
