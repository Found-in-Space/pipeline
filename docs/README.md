# Pipeline Documentation

This repository builds the Found in Space publication view: Gaia, Hipparcos,
supporting identifiers, and curated overrides are processed into one canonical
HEALPix-sharded Parquet dataset.

The documentation is intentionally small:

- [Run configuration](run-configuration.md) explains how to run the pipeline,
  what `pipeline.toml` controls, how the published override catalog is acquired
  by Version DOI and checksum, and how named runs capture a reproducible
  publication run.
- [Download packages](download-packages.md) defines the local evidence bundle
  layout for completed source downloads, including hardlinked payloads,
  manifests, checksums, and observed Gaia byte-per-row sizing.
- [Controlled data storage](storage-layout.md) defines the durable raw,
  package, derived, run, release, scratch, and legacy namespaces plus hardlink
  and retention rules.
- [Scientific provenance](scientific-provenance.md) describes the source
  catalogs, calculation engines, field requirements, formulas, and quality
  provenance encoded by the staged catalog pipelines.
- [Gaia–Hipparcos pairing resolution](pairing-resolution.md) separates the
  policy-neutral evidence catalog from pipeline acceptance policy, records the
  corrected exploratory sizing figures, and lists the remaining implementation
  gaps.
- [Output contract](output-contract.md) defines the merged single-view output,
  deduplication policy, overrides, reports, and sidecars.

Read them in that order for a full release-oriented view of this pipeline.
