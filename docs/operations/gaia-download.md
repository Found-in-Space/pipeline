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

The checked-in profiles use core Gaia pipeline fields only. Enriched `motion`,
`mass`, or other carry field sets should be enabled explicitly by release
projects that intend to produce those sidecars.

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

Scripted downloads always start with a background Gaia async HEALPix level-3
count query:

```bash
uv run fis-pipeline gaia download plan --project project-small.toml
```

The count query validates the generated source query and gives exact row counts
before result jobs are submitted. The planner stores the count job id while it
is running, then stores counts, batch membership, query hashes, output paths,
and job-ready ADQL in `[gaia_download].state_db`.
It also writes the exact local ADQL files used for the run beside the state DB:
`count.adql` and one `<batch>.adql` file per download batch.

For release-specific cuts that must happen in the Gaia Archive before download,
add a `source_where_adql` fragment:

```toml
[gaia_download]
source_where_adql = """
g.phot_g_mean_mag <= 15
OR d.r_med_photogeo <= 100
OR d.r_med_geo <= 100
"""
```

The fragment is appended to both count and download queries, so the generated
`.adql` files are the provenance record to carry into a catalog release.

Small runs normally produce one all-sky magnitude-limited batch. Full runs use
deterministic HEALPix batch packing under `[gaia_download].row_cap`, then cap
authenticated batches further with `[gaia_download].remote_result_bytes_per_row`
and `[gaia_download].remote_max_result_bytes_per_job`. One-tile over-cap
batches are recorded explicitly.

## 5. Run Or Resume

Run the planned jobs:

```bash
uv run fis-pipeline gaia download run --project project-small.toml
```

The runner records Gaia async job IDs immediately after submission. During long
waits it prints a polling heartbeat with the active job ID, phase, local state,
and expected rows.

Before submitting each authenticated result job, the runner inspects the Gaia
Archive account's async jobs, counts known completed result sizes even when
those jobs were not created by this pipeline state DB, and combines that with
the planner's estimated size for active and next batches. Submission is blocked
unless the projected Gaia Archive result usage leaves at least
`[gaia_download].remote_min_free_bytes` free under
`[gaia_download].remote_quota_bytes`, which defaults to Gaia's standard 20 GB
job-result quota and can be raised for accounts with increased quota. External
active jobs also block submission because Gaia does not reserve result storage
until they complete. Failed or unknown-size remote jobs block submission because
Gaia can report them as `0 bytes` while they still consume file quota; delete
those jobs before retrying.

If you interrupt with `Ctrl-C`, rerun the same command. It reloads the SQLite
state, resumes polling by job ID, downloads completed results, and continues.

For authenticated runs, remote deletion is part of success. If a local download
succeeds but deleting the Gaia Archive result fails, the batch remains
`delete_pending`. A resumed run retries those deletes before submitting more
jobs so the Gaia account quota is not silently consumed. Failed authenticated
jobs are also deleted before the batch is considered terminal, because failed
Archive jobs can still consume file quota.

Transient Gaia Archive backend failures, such as temporary service outages or
JPA transaction errors, are requeued after the failed remote job is deleted.
Each batch gets up to three transient retries; query, quota, and data errors
remain terminal so they do not loop indefinitely.

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

Configured Gaia carry field sets may produce:

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
