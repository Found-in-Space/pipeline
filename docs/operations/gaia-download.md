# Gaia Download

This page is the step-by-step path for getting Gaia VOTable files into the
pipeline. It covers the scripted downloader, the browser-assisted small run, and
the pieces that make long downloads resumable.

## 1. Start From A Profile

Create a small project for a first run:

```bash
uv run fis-pipeline project init --profile small project-small.toml
```

Create a full-layout project when you are ready for authenticated all-sky
downloads:

```bash
uv run fis-pipeline project init --profile full project.toml
```

The Gaia downloader reads `[gaia_download]` from the project file. Existing
projects without that section can still run `gaia build` from manually supplied
VOTables, but `gaia download ...` requires it.

## 2. Choose Access

The small profile defaults to:

```toml
[gaia_download]
mode = "small"
access = "auto"
mag_limit = 9.0
carry_field_sets = []
```

`access = "auto"` uses anonymous Gaia Archive jobs when the counted row total
fits the anonymous cap. If the query is too large, the downloader switches to
authenticated access.

The small profile uses core Gaia pipeline fields only. The full profile has no
magnitude limit and explicitly enables the enriched `motion` and `mass` carry
field sets.

For unattended authenticated runs, set one of:

```bash
export GAIA_CREDENTIALS_FILE=/path/to/gaia-credentials.txt
export GAIA_USER=...
export GAIA_PASS=...
```

`GAIA_CREDENTIALS_FILE` takes precedence over `GAIA_USER` and `GAIA_PASS`.
Commands never prompt for credentials.

The Gaia-to-Hipparcos crossmatch downloader uses the same environment variables
opportunistically: it logs in when credentials are available and otherwise falls
back to anonymous access.

## 3. Optional Browser Path

For a beginner-friendly small run, you can generate ADQL and submit it manually
in the Gaia Archive web UI:

```bash
uv run fis-pipeline gaia download queries --project project-small.toml
```

This writes:

```text
data/processed/gaia-download-small-browser-queries/count.adql
data/processed/gaia-download-small-browser-queries/download.adql
```

Paste `download.adql` into the Gaia Archive web UI, download the result as
VOTable gzip, and place it under `[gaia] input_dir`.

## 4. Plan The Download

Scripted downloads always start with a HEALPix level-3 count query:

```bash
uv run fis-pipeline gaia download plan --project project-small.toml
```

The count query validates the generated source query and gives exact row counts
before result jobs are submitted. The planner stores counts, batch membership,
query hashes, output paths, and job-ready ADQL in `[gaia_download].state_db`.

Small runs normally produce one all-sky magnitude-limited batch. Full runs use
deterministic HEALPix batch packing under `[gaia_download].row_cap`, with
one-tile over-cap batches recorded explicitly.

## 5. Run Or Resume

Run the planned jobs:

```bash
uv run fis-pipeline gaia download run --project project-small.toml
```

The runner records Gaia async job IDs immediately after submission. During long
waits it prints a polling heartbeat with the active job ID, phase, local state,
and expected rows.

If you interrupt with `Ctrl-C`, rerun the same command. It reloads the SQLite
state, resumes polling by job ID, downloads completed results, and continues.

For authenticated runs, remote deletion is part of success. If a local download
succeeds but deleting the Gaia Archive result fails, the batch remains
`delete_pending`. A resumed run retries those deletes before submitting more
jobs so the Gaia account quota is not silently consumed.

## 6. Build Gaia Staging

Once VOTables exist under `[gaia] input_dir`, build the staged Gaia Parquet:

```bash
uv run fis-pipeline gaia build --project project-small.toml
```

The dense Gaia output schema remains stable. When carry-through field sets are
configured, those fields are written as nullable `gaia_*` columns for merge
sidecars. Missing carry-through fields in manually supplied or older VOTables
become null columns rather than build failures.

Gaia VOTable parsing requires `votpipe >= 0.2.1`, which supports
variable-length VOTable character arrays used by fields such as `flags_flame`.

## 7. Merge And Sidecars

After the other stages are built, merge:

```bash
uv run fis-pipeline merge build --project project-small.toml
```

`merge build` writes compact dense HEALPix shards under `[merge].output_dir`.
When Gaia carry-through fields are configured, it also writes merge-aligned
sidecars under `[merge].sidecar_output_dir` or, if omitted, a `sidecars`
directory beside `[merge].output_dir`.

The full profile's Gaia carry field sets produce:

```text
gaia_enrichment/
motion/
mass/
```

Gaia enrichment follows the matched Gaia counterpart even when Hipparcos or an
override wins the dense merged row. Override drops, unmatched Hipparcos rows,
and manual-only override adds do not emit Gaia enrichment.

## Troubleshooting

Anonymous Gaia async jobs share Archive storage. If Gaia reports
`Filesystem quota exceeded for user anonymous`, retry later or switch to
authenticated access.

Full authenticated downloads are large. Put project paths on a data volume with
enough space, and keep `max_active_jobs` conservative unless you have verified
both Gaia Archive quota and local disk capacity.
