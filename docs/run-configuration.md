# Run Configuration

This pipeline is configured by a TOML project file. A controlled project lives
in a named run directory and is the
record of where source catalog downloads live, where processed artifacts are
written, which Gaia calculation engine is used, and how the final merged output
is partitioned.

## Create A Project

Install dependencies and inspect the CLI:

```bash
uv sync
uv run fis-pipeline --help
```

Create a named controlled run:

```bash
uv run fis-pipeline project init-run --profile small \
  /data/fis/runs/stars/stars-small-20260730
```

Edit `/data/fis/runs/stars/stars-small-20260730/pipeline.toml` to reference the
exact immutable raw and derived inputs. Heavy inputs may live elsewhere; only
their paths and identities are captured in the run directory. The checked-in
profiles pin Found in Space stellar overrides release `20260730.2` by its
Zenodo Version DOI and SHA-256 and explicitly select both published YAML
catalogs. An ad-hoc project may instead omit the Zenodo fields and use its own
explicit `source_paths`; a controlled project that deliberately uses no
overrides must remove all four Zenodo fields and set `source_paths = []`.
Then validate the complete contract:

```bash
uv run fis-pipeline project validate --project \
  /data/fis/runs/stars/stars-small-20260730/pipeline.toml
```

Use `--profile full` or omit `--profile` for a full run. The older
`project init PROJECT_PATH` command remains available for ad-hoc projects, but
it does not create controlled-run metadata.

## Pipeline Flow

Acquire the smaller supporting feeds explicitly, then build them:

```bash
uv run fis-pipeline hip download --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline gaia-to-hip download --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline identifiers download --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline overrides download --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml

uv run fis-pipeline hip build --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline gaia-to-hip build --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline identifiers build --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline overrides build --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
```

Build commands never acquire or replace raw inputs. Their `--force` flag
controls only derived outputs; use `download --force` for an intentional
re-acquisition. `identifiers build` requires the configured accepted
Gaia–Hipparcos map and fails rather than silently omitting Gaia identifiers.
`hip build --limit` additionally requires `--output` pointing somewhere other
than the canonical project output.

Plan, download, and stage Gaia:

```bash
uv run fis-pipeline gaia download plan --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline gaia download run --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
uv run fis-pipeline gaia build --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
```

If Gaia result-size estimates need to change after some batches already
finished, replan only unexecuted work:

```bash
uv run fis-pipeline gaia download replan-pending --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml --bytes-per-row 250
uv run fis-pipeline gaia download run --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
```

Downloaded or remotely completed batches are preserved; only planned or failed
pending batches are replaced.

For a browser-assisted Gaia run, generate ADQL instead of submitting jobs:

```bash
uv run fis-pipeline gaia download queries --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
```

Paste `download.adql` into the Gaia Archive web UI and place the VOTable gzip
under `[gaia].input_dir`. Browser-managed input directories must use
`input_mode = "directory"`. Scripted acquisitions use
`input_mode = "download-state"` and require a complete, matching state DB.

Build the publication view:

```bash
uv run fis-pipeline merge build --project /data/fis/runs/stars/stars-small-20260730/pipeline.toml
```

Quality reporting is not part of the normal pipeline. To request it for a
particular build, explicitly add `--quality-report` to the command above. The
merge then evaluates the audit rules against the staged matched pair and each
merged output batch while those rows are already in memory. It writes findings
through a bounded Parquet buffer and does not rescan the staged or merged
catalogues.

`merge quality-report` remains available only for retrospective recovery when a
merge was built without inline reporting. It rereads the staged Gaia catalogue
for matched-pair evidence and scans all merged shards, so it is substantially
more expensive.

The current merge requires a pre-resolved one-to-one Gaia/Hipparcos map. The
policy-neutral pairing evidence published by `../catalogs` is intentionally
many-to-one and is not a valid direct input. A pipeline pairing-resolution
stage is planned but not yet implemented; see
[Gaia–Hipparcos pairing resolution](pairing-resolution.md). Until that stage
exists, use H2BN or another independently resolved one-to-one map.

## Project File Contract

`format_version = 1` remains the supported project format. Paths may be absolute or
relative to the project file. Environment-variable syntax inside path strings is
rejected so runs are reproducible.

The important sections are:

- `[run]`: controlled-run ID and provenance directory. The ID must match the
  directory containing `pipeline.toml`.
- `[gaia]`: explicit `input_mode`, VOTable input directory, staged Parquet
  output directory, optional local Gaia G magnitude limit, and
  `calculation_engine`.
- `[gaia_download]`: Gaia Archive mode, access, row planning, async quota
  settings, optional source `WHERE` fragment, and optional carry field sets.
- `[gaia-to-hip]`: cached Gaia DR3 `hipparcos2_best_neighbour` path, processed
  crossmatch path, and optional supplemental mapping Parquets that are already
  resolved and one-to-one. Pairing-evidence Parquets are not supplemental maps.
- `[hip]`: cached Hipparcos New Reduction ECSV path and processed Hipparcos
  Parquet path.
- `[identifiers]`: source ECSV paths and processed sparse identifier sidecar
  path.
- `[overrides]`: processed override table path, explicit YAML sources, and an
  optional complete Zenodo acquisition pin consisting of
  `zenodo_version_doi`, `zenodo_archive`, `zenodo_archive_sha256`, and
  `zenodo_download_dir`.
- `[merge]`: merged output directory, HEALPix order, and optional sidecar output
  directory.

Ad-hoc projects need only the sections used by a command. A project containing
`[run]` is controlled and every command enforces the complete production
contract, including explicit Gaia input and override selection.

## Version-pinned Override Release

The standard profiles configure:

```toml
[overrides]
output_parquet = "data/processed/overrides.parquet"
zenodo_version_doi = "10.5281/zenodo.21703732"
zenodo_archive = "fis-overrides-20260730.2.zip"
zenodo_archive_sha256 = "1f4a7544fe1257d50d7f5a00e9626404a05ea077bc1a305c72fb9eee6448981f"
zenodo_download_dir = "data/catalogs/fis-overrides-20260730.2"
source_paths = [
  "data/catalogs/fis-overrides-20260730.2/catalog/alpha_cen.yaml",
  "data/catalogs/fis-overrides-20260730.2/catalog/distance_resolution_v1_resolved.yaml",
]
```

The Concept DOI `10.5281/zenodo.21703731` is deliberately absent from the
configuration because it resolves to the latest release rather than this exact
input snapshot.

`overrides download` derives the Zenodo record ID from the configured Version
DOI, retrieves the
[published record](https://zenodo.org/records/21703732), and requires the
record's own DOI to equal the configured value. This rejects the Concept DOI,
which follows the latest version. It then selects the exact named ZIP, verifies
both the pinned SHA-256 and Zenodo's checksum, safely extracts it, verifies
every file in
`checksums.sha256`, and checks `manifest.toml` for the release, series, Version
DOI, format, and ordered two-catalog consumer contract.

The cached directory retains the original ZIP and the retrieved
`zenodo-record.json` metadata receipt alongside the verified publication
contents. Without `--force`, a valid cached release is reused without network
access; an invalid or partial cache fails closed. `--force` stages and verifies
the replacement before swapping it into place. Both runtime consumers,
`overrides build` and `identifiers build`, independently revalidate the cached
archive, receipt, checksum manifest, publication manifest, and selected source
paths before loading any YAML.

## Command Provenance

Every project-backed command in a controlled run writes a new JSON event under
`[run].provenance_dir`. Events are never reused or overwritten. Each records
the run ID, exact project hash and resolved paths, pipeline Git SHA and dirty
state, parameters, start/end/status, and content identities for referenced
inputs and outputs. Directories receive deterministic tree hashes; no catalog
payload is copied into the provenance directory.

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

A controlled run automatically captures the project hash, resolved paths, code
identity, command parameters, status, and artifact identities. Retain the named
run directory plus:

- the exact `pipeline.toml` and its `provenance/` events;
- the pipeline git SHA;
- the Gaia `calculation_engine`;
- the generated Gaia count and download ADQL bundle;
- hashes for the ADQL files and downloaded source files;
- the Gaia download state database or exported state summary;
- the exact accepted one-to-one Gaia/Hipparcos map used by the run;
- the source pairing-evidence artifact, resolution decisions, policy version,
  thresholds, report, and hashes once pairing resolution is implemented;
- the override YAML paths, Version DOI, original Zenodo ZIP, metadata receipt,
  archive SHA-256, internal checksum manifest, and extracted source hashes;
- merge report, merge decisions, optional quality report, and output checksums.

If `../catalogs` publishes a source or deduplication snapshot, its manifest
should pin the pipeline SHA, calculation engine, query bundle hashes, and files
used by this pipeline run.

For local source-download evidence bundles, use the layout and checksum
conventions in [Download packages](download-packages.md).
