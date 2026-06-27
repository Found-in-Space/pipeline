# Pipeline Documentation

This repository builds the Found in Space publication view: Gaia, Hipparcos,
supporting identifiers, and curated overrides are processed into one canonical
HEALPix-sharded Parquet dataset.

The documentation is intentionally small:

- [Run configuration](run-configuration.md) explains how to run the pipeline,
  what `project.toml` controls, and what to capture for a reproducible
  publication run.
- [Download packages](download-packages.md) defines the local evidence bundle
  layout for completed source downloads, including hardlinked payloads,
  manifests, checksums, and observed Gaia byte-per-row sizing.
- [Scientific provenance](scientific-provenance.md) describes the source
  catalogs, calculation engines, field requirements, formulas, and quality
  provenance encoded by the staged catalog pipelines.
- [Output contract](output-contract.md) defines the merged single-view output,
  deduplication policy, overrides, reports, and sidecars.

Read them in that order for a full release-oriented view of this pipeline.
