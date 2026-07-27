# Run Configuration

This pipeline is configured by a TOML project file. The project file is the
record of where source catalog downloads live, where processed artifacts are
written, which Gaia calculation engine is used, and how the final merged output
is partitioned.

## Create A Project

Install dependencies and inspect the CLI:

```bash
uv sync
uv run fis-pipeline --help
```

Create a starter project:

```bash
uv run fis-pipeline project init --profile small project.toml
```

Use `--profile full` or omit `--profile` for the conventional full local
layout. Profiles are only starter templates; publication runs should commit or
archive the resolved project file used for the run.

## Pipeline Flow

Build the smaller supporting feeds:

```bash
uv run fis-pipeline hip build --project project.toml
uv run fis-pipeline gaia-to-hip build --project project.toml
uv run fis-pipeline identifiers build --project project.toml
uv run fis-pipeline overrides build --project project.toml
```

Plan, download, and stage Gaia:

```bash
uv run fis-pipeline gaia download plan --project project.toml
uv run fis-pipeline gaia download run --project project.toml
uv run fis-pipeline gaia build --project project.toml
```

If Gaia result-size estimates need to change after some batches already
finished, replan only unexecuted work:

```bash
uv run fis-pipeline gaia download replan-pending --project project.toml --bytes-per-row 250
uv run fis-pipeline gaia download run --project project.toml
```

Downloaded or remotely completed batches are preserved; only planned or failed
pending batches are replaced.

For a browser-assisted Gaia run, generate ADQL instead of submitting jobs:

```bash
uv run fis-pipeline gaia download queries --project project.toml
```

Paste `download.adql` into the Gaia Archive web UI, download the VOTable gzip,
place it under `[gaia].input_dir`, then run `gaia build`.

Build the publication view:

```bash
uv run fis-pipeline merge build --project project.toml
uv run fis-pipeline merge quality-report --project project.toml
```

The current merge requires a pre-resolved one-to-one Gaia/Hipparcos map. The
policy-neutral pairing evidence published by `../catalogs` is intentionally
many-to-one and is not a valid direct input. A pipeline pairing-resolution
stage is planned but not yet implemented; see
[Gaia–Hipparcos pairing resolution](pairing-resolution.md). Until that stage
exists, use H2BN or another independently resolved one-to-one map.

## Project File Contract

`format_version = 1` is the supported project format. Paths may be absolute or
relative to the project file. Environment-variable syntax inside path strings is
rejected so runs are reproducible.

The important sections are:

- `[gaia]`: VOTable input directory, staged Parquet output directory, optional
  local Gaia G magnitude limit, and `calculation_engine`.
- `[gaia_download]`: Gaia Archive mode, access, row planning, async quota
  settings, optional source `WHERE` fragment, and optional carry field sets.
- `[gaia-to-hip]`: cached Gaia DR3 `hipparcos2_best_neighbour` path, processed
  crossmatch path, and optional supplemental mapping Parquets that are already
  resolved and one-to-one. Pairing-evidence Parquets are not supplemental maps.
- `[hip]`: cached Hipparcos New Reduction ECSV path and processed Hipparcos
  Parquet path.
- `[identifiers]`: source ECSV paths and processed sparse identifier sidecar
  path.
- `[overrides]`: processed override table path plus optional custom override
  YAML paths.
- `[merge]`: merged output directory, HEALPix order, and optional sidecar output
  directory.

Only sections required by the command being run must be present. Missing
sections fail when accessed.

## Gaia Query Bundle

Gaia download planning uses two closely related queries generated from the same
resolved acquisition spec:

- `count.adql` counts eligible rows grouped by HEALPix level 3 for planning.
- `download.adql` or batch ADQL files use the same eligibility criteria and add
  the selected fields required by the configured calculation engine and carry
  field sets.

The count query can omit projection-only left joins that do not affect row
eligibility, but both queries must be generated together from the same spec.
Hand-editing one query without regenerating the bundle breaks the contract.

## Gaia Download Sizing

`[gaia_download].remote_result_bytes_per_row` estimates compressed Gaia Archive
result bytes per selected row. The estimate drives remote quota guards and can
reduce the effective row cap below `[gaia_download].row_cap`.

The completed 2026-06 full Gaia DR3 download with `calculation_engine =
"dr3_ap_bj_core"` and no carry field sets recorded 1,467,744,818 selected rows
and 146,842,060,166 downloaded bytes: 100.046 bytes per row overall. Batch-level
bytes per row ranged from 94.228 to 104.920, with median 100.237 and p90
104.500. For the same query shape, `120` bytes per row would leave margin; use a
larger value, such as `250`, when fields or joins change.

## Credentials And Resume State

Gaia Archive credentials are read from `GAIA_CREDENTIALS_FILE` or
`GAIA_USER`/`GAIA_PASS`. The Gaia-to-Hipparcos downloader uses the same
credential mechanism when authenticated access is available.

Scripted Gaia downloads record row counts, batch plans, async job IDs, query
hashes, local result paths, and retry state in `[gaia_download].state_db`.
Interrupted downloads should be resumed with the same project file and state
database.

## Publication Capture

A publication run should retain enough information to prove which data and code
created the output. Capture at least:

- the exact `project.toml`;
- the pipeline git SHA;
- the Gaia `calculation_engine`;
- the generated Gaia count and download ADQL bundle;
- hashes for the ADQL files and downloaded source files;
- the Gaia download state database or exported state summary;
- the exact accepted one-to-one Gaia/Hipparcos map used by the run;
- the source pairing-evidence artifact, resolution decisions, policy version,
  thresholds, report, and hashes once pairing resolution is implemented;
- override YAML paths or packaged override version;
- merge report, merge decisions, quality report, and output checksums.

If `../catalogs` publishes a source or deduplication snapshot, its manifest
should pin the pipeline SHA, calculation engine, query bundle hashes, and files
used by this pipeline run.

For local source-download evidence bundles, use the layout and checksum
conventions in [Download packages](download-packages.md).
