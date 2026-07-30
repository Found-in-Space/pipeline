# Download Packages

Download packages are local, immutable evidence bundles for expensive source
catalog acquisitions. They are meant to preserve enough context to reuse,
audit, or publish a completed download without turning the git repository into
the data store.

The package is separate from the pipeline output contract. It captures source
download payloads and acquisition evidence; staged and merged Parquet releases
can be packaged separately.

## Package ID

Use a stable, human-readable ID:

```text
fis-gaia-dr3-full-YYYYMMDD-<acquisition_hash_8>-<git_sha_12>
```

For the completed 2026-06 Gaia full download:

```text
fis-gaia-dr3-full-20260627-b4db1f05-0a2780b72e96
```

The date is the UTC completion date from downloader state, not the packaging
date. The acquisition hash comes from `gaia-download.sqlite` metadata, and the
git SHA pins the pipeline code used for the run.

## Layout

Create packages under `data/packages/<package-id>/`:

```text
data/packages/<package-id>/
  RUN_ID
  README.md
  package.json
  config/
    pipeline.toml
    pyproject.toml
    uv.lock
    README.md
    LICENSE
  evidence/
    gaia-download.sqlite
    gaia-download-batches.json
    gaia-download-state-summary.json
    gaia-download-queries/
      count.adql
      b0001.adql
      ...
  manifests/
    gaia-votables-manifest.tsv
    gaia-download-queries-manifest.tsv
    supporting-files.sha256
    package-control.sha256
    gaia-votables.sha256
  payload/
    gaia-votables/
      gaia_full_b0153.vot.gz
      ...
  provenance/
    git-head.txt
    git-status-short.txt
    git-diff-stat.txt
    git-diff.patch
```

The state database is authoritative for final batch state. The query directory
may contain historical ADQL files from earlier plans or replans; keep the whole
directory because it is useful audit evidence, but use the state export to
identify the final downloaded batches.

## Hardlink Payloads

For local packages, hardlink large payload files instead of copying them:

```bash
mkdir -p data/packages/$RUN_ID/payload/gaia-votables
find data/catalogs/gaia -maxdepth 1 -type f -name '*.vot.gz' -print0 |
  sort -z |
  xargs -0 -I{} ln {} data/packages/$RUN_ID/payload/gaia-votables/
```

Hardlinks make the package tree usable as an upload source without allocating a
second copy of the raw data. Treat both paths as read-only: editing either path
edits the same underlying file. Deleting one path is safe as long as another
hardlink remains.

Before publishing, verify the hardlinks:

```bash
awk 'NR > 1 && $5 < 2 {bad++} END {exit bad ? 1 : 0}' \
  data/packages/$RUN_ID/manifests/gaia-votables-manifest.tsv
```

## Evidence And Manifests

Every package should include:

- the exact `pipeline.toml`;
- the pipeline `pyproject.toml` and `uv.lock`;
- the pipeline git HEAD, dirty status, and dirty patch if the run used
  uncommitted code;
- the generated Gaia ADQL bundle;
- the Gaia downloader state database;
- exported downloader state summaries in JSON;
- payload file names, byte sizes, original source paths, and link counts;
- SHA256 checksums for small support files;
- SHA256 checksums for raw payload files before publication or offsite upload.

Credentials and shell environment files, such as `.envrc`, must not be
packaged.

## Raw Payload Checksums

Raw payload checksums are expensive because they read the full download. Run
them in a long-lived terminal or tmux session:

```bash
cd data/packages/$RUN_ID
find payload/gaia-votables -type f -name '*.vot.gz' -print0 |
  sort -z |
  xargs -0 sha256sum > manifests/gaia-votables.sha256
```

Then refresh the package control checksum:

```bash
sha256sum package.json README.md RUN_ID manifests/*.tsv \
  manifests/supporting-files.sha256 manifests/gaia-votables.sha256 \
  > manifests/package-control.sha256
```

## Observed Gaia Full Sizing

The 2026-06 full Gaia DR3 download used `calculation_engine =
"dr3_ap_bj_core"` with no carry field sets. It selected Gaia source,
Bailer-Jones distance, and Gaia DR3 astrophysical-parameter columns needed by
the current Gaia stage.

Downloader state recorded:

| Metric | Value |
| --- | ---: |
| Final downloaded batches | 60 |
| HEALPix-3 count rows | 768 |
| Source rows | 1,467,744,818 |
| Downloaded bytes | 146,842,060,166 |
| Overall bytes per row | 100.046 |
| Mean batch bytes per row | 100.328 |
| Median batch bytes per row | 100.237 |
| p10 batch bytes per row | 96.893 |
| p25 batch bytes per row | 97.850 |
| p75 batch bytes per row | 103.420 |
| p90 batch bytes per row | 104.500 |
| Minimum batch bytes per row | 94.228 (`b0154`) |
| Maximum batch bytes per row | 104.920 (`b0223`) |

These numbers are compressed VOTable gzip result bytes per selected source row.
They are specific to the selected columns and Gaia Archive output format. Carry
field sets, different joins, or a different output format require a fresh
estimate.

For the current full query shape, `remote_result_bytes_per_row = 120` would
have covered the observed batch distribution with margin. Use a larger value,
such as `250`, when planning with new fields or when avoiding remote quota
surprises is more important than minimizing batch count.
