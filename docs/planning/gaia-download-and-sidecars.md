# Gaia Download And Sidecar Plan

This note captures the current planning thread for Gaia download automation and
the downstream sidecars needed by octree builds. It is a planning document, not
the current command contract. Current supported commands remain documented in
[feed downloads](../operations/feed-downloads.md) and the stage pages.

## Implementation Tracker

Use this section as the live work tracker. The detailed spec below explains the
rules and context for each slice.

- [ ] **Stage 0: Command and config shape.** Decide the public CLI surface and
  project TOML section names before writing downloader internals. The expected
  shape is a `gaia download plan` command first, followed by a run/executor
  command, with all paths still rooted in the existing project file.
- [ ] **Stage 1: Query spec and field sets.** Build a small internal model for
  Gaia query inputs: core pipeline columns, optional carry-through field sets,
  magnitude limits, source filters, table aliases, and stable output names. This
  stage should generate both count ADQL and download ADQL from the same spec.
- [ ] **Stage 2: Universal HEALPix count planning.** Implement the level-3
  HEALPix count query as the first step for every download, including small
  runs. Persist the count table and report exact row totals so the tool can
  validate the query and decide whether anonymous execution is allowed.
- [ ] **Stage 3: Batch planner.** Convert the count table into a download plan.
  Small runs normally collapse to one batch when they fit the anonymous row cap.
  Full runs use deterministic near-limit HEALPix packing, including explicit
  handling for any single tile that exceeds the configured cap.
- [ ] **Stage 4: Archive client and durable state.** Add a testable Gaia Archive
  client boundary around `astroquery`, with lazy imports, fakeable job objects,
  job-id tracking, query hashes, output paths, phases, errors, retry counts, and
  local progress state.
- [ ] **Stage 5: Anonymous executor.** Run one-batch anonymous downloads from a
  saved plan: submit async, record the job id immediately, poll/reload by job id,
  stream the completed VOTable to `[gaia] input_dir`, and verify the local file.
- [ ] **Stage 6: Authenticated scheduler.** Add unattended credentials from
  environment variables, bounded concurrent jobs, remote quota and local disk
  checks, streaming downloads, verification, required remote deletion, and
  `delete_pending` recovery.
- [ ] **Stage 7: Gaia staging carry-through.** Teach Gaia staging to preserve
  configured query-backed and stage-backed carry-through fields while keeping
  `OUTPUT_COLS` unchanged for the dense pipeline contract.
- [ ] **Stage 8: Merge-aligned Gaia enrichment sidecar.** Preserve Gaia
  enrichment under the canonical merged identity even when Hipparcos or
  overrides win the dense row. Keep the dense merged HEALPix shards compact.
- [ ] **Stage 9: Derived sidecar inputs.** Build post-merge, unquantized,
  unit-documented sidecar input tables such as motion or mass. Leave final
  byte-level payload encoding and quantization to the octree repository.
- [ ] **Stage 10: Documentation and operational polish.** Update the start-here
  path, profiles docs, operations docs, and tests so the small-to-large story is
  scripted, teachable, resumable, and clear about what is stored locally versus
  downloaded from Gaia.

## Design Constraints

- This is the first implementation of Gaia source downloads in this repo. No old
  downloader command surface, legacy wrappers, compatibility aliases, or
  migrated script APIs need to be supported.
- This code is also teaching material. Prefer clear data structures, explicit
  steps, and readable control flow over clever compression of ideas.
- Scale still matters more than prettiness when the two conflict. The full run
  deals with 1 billion plus rows, so streaming, batching, bounded memory, and
  resumability must win over locally elegant but slow or memory-heavy code.
- Use clear naming conventions for query specs, count plans, batches, jobs,
  states, and output files. Add comments where they make non-obvious archive,
  astronomy, or scaling choices easier to follow.
- Humans will run these commands. Long-running operations must produce useful
  status output: use `tqdm` where loop progress is measurable, and ordinary
  status logging for archive jobs, polling, downloads, verification, deletion,
  and other work that does not map cleanly to a local progress loop.

## Goals

- Automate the main Gaia VOTable download flow.
- Keep the pipeline a single project with one CLI and one project file format.
- Do not commit sample catalogue data.
- Keep the dense merged star schema compact.
- Preserve Gaia enrichment fields for later octree sidecar builders, even when
  Hipparcos or manual overrides win the dense merged row.
- Separate required calculation fields from optional carry-through Gaia fields,
  so new sidecar enrichments can be added without editing the core pipeline
  calculation code.
- Support the educational small-to-large data story from the website: a small
  bright-star download first, then larger authenticated runs, then full all-sky
  batching.
- Treat the old `three-dee` downloader as historical reference only: capture the
  useful requirements and algorithms, but implement fresh pipeline-native code.

## Current State

The current Gaia stage only builds from existing VOTable files. The command is:

```bash
uv run fis-pipeline gaia build --project project.toml
```

`[gaia] input_dir` is where VOTable files must already exist. The main Gaia
source download is still manual and documented as a recipe in
[feed downloads](../operations/feed-downloads.md).

The only automated Gaia Archive download in this repo today is the
Gaia-to-Hipparcos crossmatch:

```bash
uv run fis-pipeline gaia-to-hip download --project project.toml
uv run fis-pipeline gaia-to-hip build --project project.toml
```

Gaia staging currently writes `OUTPUT_COLS` plus a very small diagnostic set:

```text
ruwe
phot_g_mean_mag
```

That means any additional Gaia fields present in the raw VOTable are dropped
before merge.

## Website Walkthrough Alignment

The current website walkthrough
([Download the star data](https://foundin.space/build/download-the-star-data/))
frames the workflow as a small-to-large progression:

- `G <= 9`: bright-star sample, roughly hundreds of thousands of stars, small
  enough for a quick first run and no Gaia Archive account.
- `G <= 12` and `G <= 15`: larger magnitude-limited extracts that need a free
  Gaia Archive account and async jobs.
- Full sky: no magnitude limit, all-sky HEALPix level-3 partitioning and
  batched async downloads.

That product and teaching story should remain. The website can show the manual
ADQL route, but the repo should also support the small path as a scripted
command so beginners can get real Gaia data without copying SQL by hand.

The website page is older than this repo pass and should be updated later. In
particular, the current repository uses project TOML paths and
`fis-pipeline gaia build --project project.toml`, where the Gaia build command
reads all VOTables from `[gaia] input_dir`.

## Prior Art In `three-dee`

The sibling `three-dee` repo contains older Gaia download machinery that is
useful as historical reference only. Do not keep its command surface, module
layout, wrappers, or code shape in this repository. The new implementation
should be native to `found-in-space-pipeline` and its project TOML/CLI model.

Reference the old code to understand the problems already encountered:

- `src/three_dee/gaia.py`: column lists, ADQL helpers, Gaia login, job logging,
  async polling, raw streaming of completed TAP results to disk, and redownload
  helpers.
- `src/three_dee/fetch/gaia_shells.py`: old volume-limited 100 pc, 200 pc,
  500 pc, and 1000 pc shell downloads.
- `src/three_dee/fetch/gaia_large.py`: older distance and magnitude tier query.
- `src/three_dee/fetch/gaia_longrange.py`: older long-range bright-tracer query.
- `src/three_dee/fetch/gaia_for_hip.py`: Gaia-for-Hipparcos fetch through an
  uploaded Gaia Archive user table.
- `docs/gaia_healpix.ipynb` and `docs/gaia_healpix.md`: HEALPix level-3 batch
  query sketch using `source_id / 9007199254740992`.
- `docs/feed-downloads.md`: the clearest prior-art recipe for all-sky row
  counts and row-cap batch packing. It includes the level-3 count query and a
  repeated subset-sum planner that tries to fill each batch as close as possible
  to the target row limit.

The useful output from this review is a set of requirements, not code to keep:

- canonical Gaia column lists
- ADQL query construction
- lazy Gaia Archive login
- async submit and job logging
- polling by phase
- raw streaming download to avoid materialising huge results in memory
- redownload or resume by job id
- HEALPix level-3 row counting and batch planning

When implementing this in the pipeline repo, prefer fresh, small, tested modules
over transplanting old scripts. Any reused idea should be expressed through the
current package boundaries and public commands.

## Gaia Query Shape

The main automated Gaia query should continue to select the fields already
needed by the pipeline:

```text
source_id
ra
dec
parallax
parallax_error
pmra
pmdec
phot_g_mean_mag
phot_bp_mean_mag
phot_rp_mean_mag
ruwe
r_med_geo
r_lo_geo
r_hi_geo
r_med_photogeo
r_lo_photogeo
r_hi_photogeo
mg_gspphot
mg_gspphot_lower
mg_gspphot_upper
ag_gspphot
teff_esphs
teff_gspspec
teff_espucd
teff_gspphot
teff_gspphot_lower
teff_gspphot_upper
logg_esphs
logg_gspspec
logg_gspphot
logg_gspphot_lower
logg_gspphot_upper
```

For octree sidecars, the query should also include raw enrichment fields from
`gaiadr3.gaia_source`:

```text
ref_epoch
pm
pmra_error
pmdec_error
pmra_pmdec_corr
radial_velocity
radial_velocity_error
rv_method_used
rv_nb_transits
rv_visibility_periods_used
rv_expected_sig_to_noise
rv_renormalised_gof
rv_chisq_pvalue
rv_amplitude_robust
```

It should join `gaiadr3.astrophysical_parameters AS ap` with a `LEFT JOIN` and
select:

```text
mass_flame
mass_flame_lower
mass_flame_upper
flags_flame
evolstage_flame
lum_flame
lum_flame_lower
lum_flame_upper
radius_flame
radius_flame_lower
radius_flame_upper
teff_gspphot
logg_gspphot
mh_gspphot
ag_gspphot
```

Optionally, it can join `gaiadr3.astrophysical_parameters_supp AS aps` with a
`LEFT JOIN` for GSP-Spec/FLAME enrichment:

```text
mass_flame_spec
mass_flame_spec_lower
mass_flame_spec_upper
lum_flame_spec
lum_flame_spec_lower
lum_flame_spec_upper
radius_flame_spec
radius_flame_spec_lower
radius_flame_spec_upper
flags_flame_spec
evolstage_flame_spec
bc_flame_spec
```

Important rule: do not filter the main catalogue on mass, luminosity, radius, or
radial velocity unless intentionally building a specialist extract. These fields
are sparse enrichments.

Some Gaia field names can appear in more than one table or in both core and
enrichment contexts, for example `teff_gspphot`, `logg_gspphot`, and
`ag_gspphot`. The query builder should emit stable, unambiguous output column
names: keep the names Gaia staging already expects for core fields, and alias
optional enrichment fields when needed so VOTables do not contain duplicate
column names.

### Core Fields Versus Carry-Through Fields

The Gaia query has two different responsibilities:

- **Core calculation fields** are required by Gaia staging to compute the dense
  star row: distance, coordinates, absolute magnitude, temperature,
  quality flags, and merge diagnostics.
- **Carry-through fields** are columns that should be staged, merged into the
  Gaia enrichment sidecar, and made available for later octree sidecar builders
  without changing the core calculations.

Core fields are code-owned because the pipeline depends on their semantics.
Carry-through fields should be config-owned where practical.

Carry-through is broader than "new Gaia columns". A downstream sidecar builder
may need:

- raw Gaia fields that are also required by the core pipeline, such as `pmra`,
  `pmdec`, `parallax`, `parallax_error`, `phot_g_mean_mag`, or Bailer-Jones
  distance bounds;
- cleaning-stage/intermediate fields produced by Gaia staging, such as
  `distance_use_pc`, selected distance source, `r_lo_pc`, `r_hi_pc`,
  `bp_rp`, or `photometry_quality`;
- optional enrichment fields that are not used by the dense star pipeline, such
  as FLAME mass, radius, luminosity, or radial-velocity quality columns.

The dense merged output should stay compact, but the raw/cleaning information
needed for later derived sidecars must survive in a merge-aligned sidecar keyed
by final pipeline identity.

A future implementation should support a checked-in field-set definition rather
than requiring code edits for every new carry-through field. One possible shape:

```toml
[[gaia.carry_fields]]
name = "mass_flame"
expression = "ap.mass_flame"
dtype = "float64"
sidecar = "mass"

[[gaia.carry_fields]]
name = "pmra_masyr"
source = "input"
column = "pmra"
dtype = "float64"
sidecar = "motion"

[[gaia.carry_fields]]
name = "distance_use_pc"
source = "stage"
column = "distance_use_pc"
dtype = "float64"
sidecar = "motion"

[[gaia.carry_fields]]
name = "flags_flame"
expression = "ap.flags_flame"
dtype = "string"
sidecar = "mass"
```

The project profile could then select field groups:

```toml
[gaia-download]
carry_field_sets = ["motion", "mass"]
```

or point at a local field-set file:

```toml
[gaia-download]
carry_fields_file = "gaia-carry-fields.toml"
```

The exact TOML shape is open, but the contract should be:

- Core query fields are always present and cannot be disabled by carry-field
  config.
- Carry fields may reference only known query aliases such as `g`, `d`, `ap`,
  and optional `aps`.
- Carry fields may also reference known staging columns produced by Gaia
  cleaning.
- Carry fields are selected with stable output names.
- Query-backed carry fields are validated before submission so a typo fails
  before launching a large Gaia job.
- Stage-backed carry fields are validated against the Gaia staging contract.
- Carry fields are written as nullable columns when absent from older input
  files or not produced by a particular staging path.
- Carry fields are not allowed to change `WHERE` filters unless a command is
  explicitly building a specialist extract.
- Carry fields are carried through staging and merge sidecars as raw or
  cleaning-stage values. Derived sidecar builders decide how to transform or
  quantize them.
- Field-set docs should explain units and source Gaia table for each field.

This keeps the education path friendly: learners can add a Gaia column to a
documented field-set file, rerun the download/build, and see it appear in the
sidecar data without touching astrometry, photometry, or merge-policy code.

## Download Flow

The future Gaia download implementation should support at least two download
modes aligned with the checked-in project profiles:

- `small`: beginner-friendly, real-data download with a conservative magnitude
  limit and output under `[gaia] input_dir`.
- `full`: scalable all-sky download using HEALPix level-3 partitions.

All Gaia downloads should start with a planning query that groups matching rows
by level-3 HEALPix tile. This applies even to the small path. The count query
validates that the generated ADQL matches the intended source query, gives exact
row numbers before any result download is scheduled, and produces the common
input for either a one-batch or many-batch plan.

For the small path, the scripted flow should:

1. Build the same sidecar-aware Gaia query shape as the full path.
2. Add a conservative `phot_g_mean_mag` limit, probably matching the website's
   `G <= 9` bright-star story unless the `small` profile deliberately chooses a
   stricter default.
3. Run the HEALPix row-count query for that filtered source query.
4. If the exact total fits the anonymous row cap, produce a one-batch plan and
   run it anonymously unless credentials are explicitly requested.
5. If the exact total exceeds the anonymous row cap, refuse anonymous execution
   with a clear suggestion to lower the magnitude limit, use authenticated mode,
   or switch to a partitioned plan.
6. Submit the planned batch through `astroquery` and write a gzip VOTable under
   `[gaia] input_dir`.
7. Always record async job ids and query metadata; use the same logging and
   redownload machinery as authenticated runs.
8. Leave the subsequent build command unchanged:

```bash
uv run fis-pipeline gaia build --project project-small.toml
```

For full runs, the flow should be:

1. Build or fetch an all-sky level-3 count table using the same filter as the
   source query.
2. Normalize counts into a stable local CSV.
3. Plan batches under a configurable row cap.
4. Submit async Gaia Archive jobs for each batch.
5. Log job ids with output filenames and query metadata.
6. Poll jobs by phase.
7. Stream completed result bytes to VOTable files under `[gaia] input_dir`.
8. Support redownload or resume from the job log.

The existing manual row-cap planning from the docs is a useful starting point,
but implementation should be deterministic and testable as ordinary Python
helpers.

Implementation-wise, `small` and `full` should share one pipeline:

```text
query spec
HEALPix count query
exact row estimate
download plan
scheduler/executor
```

The mode only changes how the plan is formed from the count table. A small run
normally collapses all counted tiles into one batch. A full run packs counted
tiles into many near-limit batches.

### Gaia Archive Access Modes

Gaia download automation must support anonymous and authenticated access, but
the scheduler should treat them differently.

Current Gaia ESA Archive limits, checked against the Gaia FAQ and programmatic
access docs on 2026-05-15:

| Mode | Rows | Async retention | Jobs quota | Use in this project |
|---|---:|---:|---:|---|
| Anonymous | 3,000,000 rows max | 3 days | N/A | small bright-star downloads |
| Authenticated | unlimited rows | unlimited until deleted | 20 GB job-output filesystem quota | full all-sky batches |

Both modes use asynchronous TAP jobs for this project. The full all-sky plan
requires authenticated access because anonymous jobs are capped at 3,000,000
rows.

Use the current Gaia FAQ as the operational source of truth for limits. Older
Gaia tutorial pages may still mention older quota values.

Relevant `astroquery` calls:

```python
from astroquery.gaia import Gaia

# Optional, only for authenticated mode.
Gaia.login(user=user, password=password)
# or
Gaia.login(credentials_file=credentials_file)

job = Gaia.launch_job_async(
    query,
    name=job_name,
    output_format="votable_gzip",
    background=True,
)
job_id = job.jobid

job = Gaia.load_async_job(jobid=job_id, load_results=False)
phase = job.get_phase(update=True)

if phase == "ERROR":
    error = job.get_error(verbose=False)

Gaia.remove_jobs([job_id])
```

For unattended authenticated runs, credentials should come from the environment,
not from checked-in project files. Support:

```text
GAIA_USER
GAIA_PASS
GAIA_CREDENTIALS_FILE
```

Credential precedence should be:

1. `GAIA_CREDENTIALS_FILE`, passed to `Gaia.login(credentials_file=...)`.
2. `GAIA_USER` plus `GAIA_PASS`, passed to
   `Gaia.login(user=..., password=...)`.
3. No prompt in unattended commands. If credentials are required and absent,
   fail with a clear message.

Anonymous mode should never require these environment variables.

For command-line or lower-level fallback, ESA documents:

- Submit async jobs with `POST .../tap/async` and `PHASE=run`.
- The response contains the job URL under `/tap/async/{job_id}`.
- Completed results are available at `/tap/async/{job_id}/results/result`.
- Jobs can be deleted with `POST .../tap/deletejobs` and
  `JOB_IDS=job_id1,job_id2...`.

The old `three-dee` downloader streamed completed result bytes directly from
`async/{job_id}/results/result`. Keep that approach conceptually for large
downloads so the pipeline does not materialise full Gaia result tables in
memory, but do not transplant the old script code.

### Download Progress State

Downloads need durable local state. A CSV is acceptable for the first pass, but
SQLite would make recovery and concurrent status updates safer. The state must
not be catalogue data and should not be committed.

This state is per planned output batch. It complements, rather than replaces,
the HEALPix plan table described below, which is per level-3 tile.

Track one row per planned Gaia output batch:

```text
batch_id
hp3_values
expected_rows
estimated_result_bytes
query_hash
query_text_path
output_path
access_mode                 # anonymous | authenticated
job_name
job_id
job_url
phase                       # Gaia/UWS phase, e.g. PENDING, QUEUED, EXECUTING, COMPLETED, ERROR, ABORTED
state                       # planned, submitted, running, completed_remote, downloading, downloaded, delete_pending, deleted_remote, failed
submitted_at
last_polled_at
completed_at
download_started_at
downloaded_at
remote_deleted_at
downloaded_bytes
error_message
retry_count
```

Rules:

- Job IDs must be recorded immediately after submission. This is especially
  important for anonymous jobs because the local job log is the only reliable
  way to retrieve the result later.
- Anonymous jobs must be downloaded within the Gaia retention window.
- A restart must be able to load every non-terminal job from `job_id`, poll its
  phase, and continue.
- `ERROR` and `ABORTED` must be recorded with `job.get_error(...)` where
  available.
- For authenticated runs, `downloaded` is not terminal until the remote job has
  also been deleted. For anonymous runs, `downloaded` may be terminal because
  remote deletion is optional.
- Failed authenticated jobs should be eligible for remote deletion after the
  error has been captured, because Gaia's FAQ notes that failed jobs can still
  contribute to quota issues.

### Authenticated Scheduler

The authenticated full-sky scheduler should maintain a bounded set of in-flight
jobs. Default to four active jobs, with a configurable limit.

Definitions:

- `active`: submitted jobs that are not in a terminal local state.
- `remote_occupied`: jobs whose server-side results have not yet been deleted.
- `local_pending_bytes`: estimated bytes for active jobs that may still need to
  be written locally.

For authenticated runs, jobs in `downloaded` or `delete_pending` still count as
`remote_occupied` until `remote_deleted_at` is set.

Scheduling rules:

- Before submitting a new job, check:
  - active job count is below `max_active_jobs` (default `4`);
  - expected server-side result size plus existing undeleted remote results fits
    inside a conservative quota budget below the Gaia job-output quota;
  - local free space can hold the largest pending download plus a safety margin;
  - the planned output path does not already contain a completed file unless
    force/resume policy allows it.
- Keep up to four jobs running when space permits. This keeps the archive busy
  without assuming unlimited remote quota.
- Poll active jobs by `job.get_phase(update=True)`.
- When a job reaches `COMPLETED`, stream it to a temporary local file beside the
  final output path.
- Verify the local file before marking the batch downloaded. Minimal
  verification should include file existence, non-zero size, and a readable
  VOTable header; stronger verification can compare row count where affordable.
- Rename the temporary file atomically to the final output path.
- Only after local verification succeeds, delete the upstream job with
  `Gaia.remove_jobs([job_id])`.
- Do not submit another job if completed remote results are waiting to be
  downloaded and deleted and the quota budget is close to full.
- On delete failure, keep the state as `delete_pending` and retry before
  scheduling more work.

This means full automation is not "submit all batches and hope". It is a
closed-loop scheduler:

```text
submit until active limit or quota/space limit
poll
download completed jobs
verify local files
delete remote jobs
schedule more
repeat
```

The remote-delete step is part of success for authenticated full downloads, not
cleanup best effort.

### Anonymous Scheduler

Anonymous mode is intended for small scripted downloads.

Rules:

- Enforce the anonymous row cap at planning/query time. A profile that might
  exceed 3,000,000 rows should require authenticated mode.
- Use `background=True` so the job ID is available immediately.
- Record `job_id`, `job_url`, query hash, and output path before waiting.
- Poll or reload by job ID with `Gaia.load_async_job(jobid=..., load_results=False)`.
- Download as soon as possible after completion; anonymous results are not a
  durable upstream cache.
- Remote deletion after successful anonymous download is optional, but allowed.

### HEALPix Row Counts And Batch Packing

Every Gaia download should have an explicit HEALPix planning step before
submitting result-download jobs.

First, query Gaia for row counts per level-3 HEALPix tile using the same filter
as the eventual source download query:

```sql
SELECT
  (g.source_id / 9007199254740992) AS hp3,
  COUNT(*) AS n
FROM gaiadr3.gaia_source AS g
JOIN external.gaiaedr3_distance AS d
  ON d.source_id = g.source_id
WHERE
  g.astrometric_params_solved IN (31, 95)
  AND (
    d.r_med_photogeo IS NOT NULL
    OR d.r_med_geo IS NOT NULL
  )
GROUP BY 1
ORDER BY n DESC
```

The resulting local plan table should keep at least:

```text
hp3
count
batch
downloaded
```

`downloaded` is a resume marker. Re-planning should ignore tiles already marked
as downloaded, so interrupted full-sky downloads can continue without manual
bookkeeping.

The count query should use the same row-producing filter as the download query.
Optional enrichment joins such as `ap` and `aps` do not belong in the count query
unless a specialist extract intentionally uses them as filters.

The sum of the count table is the authoritative row estimate for planning. Use
it to decide whether anonymous execution is allowed, whether a small run can be
downloaded as one batch, and how to pack full runs.

For packing batches, port the intent of the old `three-dee` recipe: repeatedly
choose the subset of remaining tile counts whose total is the largest value not
exceeding the target row cap. The old recipe used a dynamic-programming
subset-sum helper named `best_subset_under_limit`, wrapped by
`batch_under_limit(values, limit=55_000_000)`.

Required planner behavior:

- Given pending `(hp3, count)` rows and a row cap, emit deterministic batch
  labels and membership.
- Each batch should be as close as possible to the cap under the subset-sum
  strategy, not merely a greedy accumulation.
- After a batch is chosen, remove its tiles and repeat until no pending tiles
  remain.
- If a single tile exceeds the limit, emit it as a one-tile batch and mark it as
  over-cap rather than dropping it.
- Preserve the mapping back to `hp3` values so each download query can inject
  `g.source_id / 9007199254740992 IN (...)`.
- Write the plan to disk so it can be inspected, kept with local run state, and
  reused by resume/redownload commands.

The default cap from the prior notes is `55_000_000` rows. It should be
configurable for testing, archive-limit changes, and local storage constraints.

## Gaia Staging Output

Gaia staging should distinguish:

- core dense star columns, still `OUTPUT_COLS`
- merge diagnostic columns, currently `ruwe` and `phot_g_mean_mag`
- Gaia enrichment columns for future sidecars

A likely implementation shape:

```python
GAIA_DECISION_AUX_COLS = [
    "ruwe",
    "phot_g_mean_mag",
]

GAIA_ENRICHMENT_COLS = [
    "gaia_ref_epoch",
    "gaia_pm_masyr",
    "gaia_pmra_masyr",
    "gaia_pmdec_masyr",
    "...",
]

GAIA_OUTPUT_COLS = OUTPUT_COLS + GAIA_DECISION_AUX_COLS + GAIA_ENRICHMENT_COLS
```

The exact naming should be chosen once, documented, and tested. Prefixing raw
enrichment columns with `gaia_` is useful because merged rows may use a
Hipparcos or manual canonical identity while still carrying Gaia sidecar data.

## Merge Contract

The merge winner decides the dense star row. Gaia enrichment follows the
Gaia counterpart, not the winning catalogue.

Rules:

- Unmatched Gaia row: dense row from Gaia; Gaia enrichment from the same row.
- Gaia/Hipparcos match, Gaia wins: dense row from Gaia under Hipparcos
  canonical identity; Gaia enrichment from Gaia row.
- Gaia/Hipparcos match, Hipparcos wins: dense row from Hipparcos; Gaia
  enrichment still preserved from the matched Gaia row.
- Override replace: dense row from override; Gaia enrichment preserved if the
  target or crossmatch partner Gaia row exists.
- Override drop: no dense row and no Gaia enrichment row.
- Override add/manual-only: no Gaia enrichment unless override YAML later grows
  an explicit enrichment block.
- Unmatched Hipparcos row: no Gaia enrichment.

Do not widen the dense merged HEALPix output with all enrichment fields. Instead
produce a merge-aligned Gaia enrichment sidecar keyed by the canonical merged
identity:

```text
source
source_id
gaia_source_id
...Gaia enrichment fields...
```

The sidecar should be sharded using the winning dense row's `ra_deg` and
`dec_deg`, not necessarily Gaia's original coordinates. That keeps sidecar files
spatially aligned with octree input shards even when Hipparcos or an override
provides the dense position.

## Derived Sidecar Inputs For Octree

Raw Gaia enrichment should be carried first. Derived sidecar inputs should then
be built after merge from the dense rows plus Gaia enrichment, while everything
is still keyed by canonical pipeline identity.

Motion sidecar candidates:

```text
source
source_id
gaia_source_id
ref_epoch
pmra_masyr
pmdec_masyr
pm_total_masyr
pmra_error_masyr
pmdec_error_masyr
pmra_pmdec_corr
radial_velocity_kms
radial_velocity_error_kms
mu_x_icrs_radyr
mu_y_icrs_radyr
mu_z_icrs_radyr
vx_icrs_kms
vy_icrs_kms
vz_icrs_kms
motion_flags
```

Mass sidecar candidates:

```text
source
source_id
gaia_source_id
mass_flame_solar
mass_flame_lower_solar
mass_flame_upper_solar
mass_flame_spec_solar
mass_flame_spec_lower_solar
mass_flame_spec_upper_solar
lum_flame_solar
radius_flame_solar
logg_gspphot
teff_gspphot
mh_gspphot
flags_flame
evolstage_flame
flags_flame_spec
evolstage_flame_spec
```

Suggested flags:

```text
HAS_PM
HAS_RADIAL_VELOCITY
HAS_ANGULAR_VECTOR
HAS_TANGENTIAL_VELOCITY
HAS_FULL_3D_VELOCITY
HAS_MASS_FLAME
HAS_MASS_FLAME_SPEC
```

## Quantization And Encoding Boundary

The pipeline should not own final octree payload quantization. Its job is to
produce cleaned, physically meaningful Parquet data with stable identities,
units, provenance, nullable enrichment columns, and semantic flags.

The octree project already owns runtime-oriented encoding. In the current
`found-in-space-octree` Stage 00 flow it:

- computes Morton codes and render levels from merged pipeline coordinates;
- encodes render positions as node-relative `float32` values in `[-1, 1]`;
- stores absolute magnitude as signed centimagnitudes;
- encodes effective temperature as an 8-bit logarithmic payload value;
- writes the fixed 16-byte render column that later stages gzip and pack into
  `stars.octree`.

Later octree stages build optional sidecars in render-cell order after the base
dataset package exists.

That placement is correct because byte-level encoding depends on octree-local
details: node boundaries, render level, payload order, record version, runtime
precision budget, compression, and sidecar family format.

The boundary should be:

- **Pipeline**: astronomy/data semantics, source selection, cleaning,
  plausibility, units, raw Gaia carry-through fields, derived physical columns,
  and semantic bitfields such as `quality_flags`.
- **Octree**: spatial indexing, node assignment, render order, payload record
  layout, compression, sidecar family layout, quantization, dequantization, and
  payload versioning.

If an enrichment requires astronomy-specific post-processing, build that as a
derived sidecar input from the merged dense rows plus Gaia enrichment while the
data is still keyed by canonical pipeline identity. Examples are tangential
velocity vectors, motion quality flags, or selected mass/luminosity estimates.
Keep those derived outputs in ordinary typed columns with documented units.

Final compact encodings for those derived fields belong in the octree sidecar
builder. The same unquantized sidecar input should be reusable for multiple
future payload formats without re-downloading Gaia or changing merge behavior.

The useful exception is semantic compactness. Existing fields like
`quality_flags` are acceptable in the pipeline because the bits describe data
quality and provenance, not a runtime storage layout. Small categorical enums
are also acceptable when they are part of the data contract rather than the
viewer payload format.

## Testing Requirements

Download helpers:

- Query builders produce stable ADQL for small/full profiles.
- Output format mapping supports expected Gaia Archive formats.
- Batch planner is deterministic, respects row caps where possible, and chooses
  the closest-to-cap subset for each batch.
- Batch planner has toy-count tests that prove it does not degrade into simple
  greedy accumulation.
- Authenticated mode reads Gaia credentials from `GAIA_CREDENTIALS_FILE` or
  `GAIA_USER` / `GAIA_PASS` and fails without prompting when credentials are
  required but missing.
- Job log read/write is deterministic and handles repeated descriptions.
- Polling/downloading can be tested with fake Gaia jobs and fake responses.
- `astroquery` remains lazily imported so `fis-pipeline --help` is quiet.

Gaia staging:

- Enrichment columns survive when present in the input VOTable.
- Missing enrichment columns are filled with nulls.
- Core `OUTPUT_COLS` values remain unchanged.

Merge:

- Unmatched Gaia writes enrichment sidecar rows.
- Matched pair with Gaia winner writes enrichment under Hipparcos identity.
- Matched pair with Hipparcos winner still writes Gaia enrichment under
  Hipparcos identity.
- Override replace preserves Gaia enrichment when a Gaia counterpart exists.
- Override drop emits no enrichment.
- Unmatched Hipparcos emits no enrichment.
- Dense merged output schema remains unchanged.

## Open Questions

- Should Gaia download configuration live in `[gaia]` or a new
  `[gaia-download]` section?
- Should `astrophysical_parameters_supp` be enabled by default or behind an
  explicit option?
- What is the exact small-profile query: magnitude-limited all-sky, local
  distance-limited sample, or a fixed small set of HEALPix level-3 tiles?
- Should the merge-aligned enrichment sidecar be emitted by `merge build`, or by
  a separate `sidecars build` stage that consumes merge decisions plus staged
  Gaia data?
- What should the canonical sidecar file layout be for downstream octree
  builders: one sidecar per merged HEALPix shard, or separate partition roots by
  sidecar type?
