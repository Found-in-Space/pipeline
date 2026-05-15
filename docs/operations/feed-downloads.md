# Feed Downloads

This is the operational checklist for getting source catalogs onto disk. The
pipeline keeps catalog data out of git; profiles define paths, and commands
download or build the local files as needed.

For a first run, use the `small` profile and keep the project on a data volume
with enough space:

```bash
uv run fis-pipeline project init --profile small project.toml
```

## 1. Hipparcos

Build Hipparcos first:

```bash
uv run fis-pipeline hip build --project project.toml
```

If `[hip].download_ecsv` is missing, the command downloads the Hipparcos New
Reduction catalog and then writes `[hip].output_parquet`.

## 2. Gaia To Hipparcos Crossmatch

Build the Gaia DR3 to Hipparcos sidecar:

```bash
uv run fis-pipeline gaia-to-hip build --project project.toml
```

If `[gaia-to-hip].download_ecsv` is missing, the command downloads
`gaiadr3.hipparcos2_best_neighbour` and writes the processed Parquet sidecar.
The merge uses this sidecar to compare matched Gaia and Hipparcos rows.

When Gaia credentials are available through `GAIA_CREDENTIALS_FILE` or
`GAIA_USER`/`GAIA_PASS`, this download logs in and uses authenticated Gaia
Archive access. Without those variables it still works anonymously.

## 3. Identifiers

Build the sparse name/designation sidecar:

```bash
uv run fis-pipeline identifiers build --project project.toml
```

If the configured ECSV files are missing, the command downloads the small
Vizier sources used for HIP/HD, Bayer/Flamsteed, and proper-name mappings.

## 4. Overrides

Build manual override rows:

```bash
uv run fis-pipeline overrides build --project project.toml
```

By default this uses packaged curated YAML files. Set `[overrides].data_dir` in
the project file only when working with a separate local override directory.

## 5. Gaia

Gaia is the large feed, so it has its own planning and resume state.

For the scripted path:

```bash
uv run fis-pipeline gaia download plan --project project.toml
uv run fis-pipeline gaia download run --project project.toml
uv run fis-pipeline gaia build --project project.toml
```

For the browser-assisted small path:

```bash
uv run fis-pipeline gaia download queries --project project.toml
```

Paste the generated `download.adql` into the Gaia Archive web UI, download the
result as VOTable gzip, place it under `[gaia].input_dir`, and run `gaia build`.

See [gaia-download.md](gaia-download.md) for credentials, anonymous limits,
resuming interrupted jobs, remote deletion, carry-through field sets, and
sidecar behavior.

## 6. Merge

Once all staged inputs exist, run:

```bash
uv run fis-pipeline merge build --project project.toml
```

The merge writes dense HEALPix shards, a decision table, and a report. When Gaia
carry-through field sets are configured, it also writes merge-aligned Gaia
sidecars for enrichment, motion, and mass.

## Useful Checks

Show the resolved command surface:

```bash
uv run fis-pipeline --help
uv run fis-pipeline gaia download --help
```

Inspect generated Gaia ADQL:

```bash
uv run fis-pipeline gaia download queries --project project.toml
```

Rerun a resumable Gaia download after interruption:

```bash
uv run fis-pipeline gaia download run --project project.toml
```
