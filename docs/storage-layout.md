# Controlled Data Storage

Large source acquisitions and build products live outside the Git checkout.
The durable data root is a single filesystem so source payloads and acquisition
packages can share hardlinks:

```text
/data/fis/
  incoming/
  raw/
  packages/
    acquisitions/
  derived/
  runs/
    stars/
  releases/
    stars/
  scratch/
  legacy/
```

Repository `data/` symlinks may be kept for historical convenience, but a
controlled build must use an explicit project file under its versioned run
directory. A moving repository symlink must not determine a publication input.

## Namespace Contracts

`incoming/` contains incomplete transfers and active downloads. Nothing in this
tree is a controlled input. Transfers should be resumable and promoted only
after their manifests, checksums, file counts, and byte totals pass.

`raw/` contains verified source acquisitions. Paths include the upstream
catalog release and a stable acquisition or retrieval identifier. Raw files are
immutable after verification.

`packages/acquisitions/` contains immutable acquisition evidence bundles
described by [Download packages](download-packages.md). A package is the
provenance authority for its acquisition. Its large payload may be hardlinked
to the corresponding operational view under `raw/`.

`derived/` contains reusable, immutable derived inputs that are not raw
downloads or final releases. Examples include compact Gaia matching tables and
catalog-preparation evidence. Each snapshot must record its producer Git SHA
and input hashes.

`runs/stars/<run-id>/` is mutable working state for one controlled pipeline and
octree attempt. It contains the exact project files, code SHAs, input manifest,
logs, staged Parquet, merge output, and octree intermediates.

`releases/stars/<release-id>/` contains validated final products, reports,
manifests, provenance, and checksums. Promotion from a completed run should be
an atomic rename on the same filesystem. A promoted release is immutable.

`scratch/` contains disposable transfer and database spill files. It must never
contain the only copy of an input, decision table, report, or final artifact.

`legacy/` quarantines pre-controlled data whose provenance or purpose has not
been normalized. New builds must not consume legacy files implicitly.

## Acquisition Naming

Use source, upstream release, and immutable acquisition identity in the path.
For example:

```text
raw/gaia/dr3/fis-gaia-dr3-full-20260627-b4db1f05-3e64bfe97038/
  gaia-votables/

packages/acquisitions/
  fis-gaia-dr3-full-20260627-b4db1f05-3e64bfe97038/
```

Small support inputs use the same principle:

```text
raw/hipparcos/i311/<retrieval-id>/hipparcos2.ecsv
raw/gaia/dr3/hipparcos2-best-neighbour/<retrieval-id>/
raw/gaia/dr3/hipparcos2-neighbourhood/<retrieval-id>/
raw/fis-overrides/20260730.2/10.5281-zenodo.21703732/
```

Convenience aliases such as `current` may exist for interactive inspection,
but controlled project files must reference immutable identifiers.

When the override release is cached inside a run instead of the shared raw
tree, `[overrides].zenodo_download_dir` is its authoritative location. That
directory retains the deposited ZIP, `zenodo-record.json`, extracted
publication contents, and checksum manifest. It is an immutable acquired input;
`overrides download --force` replaces it only after a new staged copy passes
the complete validation contract.

## Hardlink Rules

Hardlinks work only within one filesystem. The raw operational tree and the
acquisition package must therefore both live under `/data/fis`.

- Transfer a source tree and its package together from their common ancestor
  with `rsync -aH`. Separate transfers lose cross-directory hardlink identity.
- Verify SHA-256 checksums as well as device, inode, and link-count agreement
  after transfer.
- Treat every pathname to a shared inode as read-only. Modifying any hardlink
  modifies all of them.
- Do not hardlink mutable staged data or intermediates between runs.
- Use explicit paths or symlinks when a run consumes immutable input.
- Hardlinks save local space; they are not backups.

Do not introduce a generic content-addressed object store unless acquisition
volume or duplication makes its additional tooling worthwhile. Stable package
IDs and checksum manifests are the current identity contract.

## Run Layout

A star build should use a structure like:

```text
runs/stars/<run-id>/
  RUN_ID
  README.md
  pipeline.toml
  octree.toml
  code-shas.json
  input-manifest.json
  logs/
  pipeline/
    staged/
    identifiers/
    pairing-resolution/
    merged/
  octree/
    routed/
    prepared/
    materialized/
    work/
    sidecars-work/
  products/
    stars-v2.octree
    identifiers-v2.order
    sidecars/
```

The project files resolve paths relative to this directory and pin exact raw,
package, and derived input identities. Commands are invoked with the explicit
project path rather than a repository-default configuration.

Use a run ID that includes its creation date, pipeline Git SHA, and a short hash
of the authored run configuration. Record the octree Git SHA separately.

## Scratch And Capacity

Bulk data and safe spill storage belong on `/data`. Fast NVMe scratch may be
used only with a strict size limit and enough free space left for the operating
system.

For a host with approximately 15 GiB RAM and eight cores, conservative initial
DuckDB settings are:

```text
DUCKDB_MEMORY_LIMIT=10GB
DUCKDB_THREADS=6
DUCKDB_PRESERVE_INSERTION_ORDER=false
```

Keep at least 300-350 GiB free on the bulk filesystem during the full Gaia,
merge, and octree build. Prefer an explicit `/data/fis/scratch/<run-id>/`
DuckDB directory when the maximum spill size is uncertain.

## Retention

- Keep verified raw acquisitions and acquisition evidence packages.
- Keep derived inputs that are expensive or required by publication
  provenance.
- Keep final releases and their checksums until an independently verified
  offsite copy exists.
- Delete run intermediates only after their final release and provenance
  package validate.
- Delete scratch after the owning command has completed or failed and its
  diagnostics have been captured.
- Review legacy material separately; its presence is never evidence that it is
  a valid input.

Keep the source host's copy of a newly transferred acquisition until the
destination has passed the full payload checksum manifest and at least the
first controlled consumer stage.
