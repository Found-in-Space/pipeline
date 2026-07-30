# Found in Space Pipeline

Prepare stellar catalog data for [foundin.space](https://foundin.space/).

This project turns Gaia, Hipparcos, supporting identifier feeds, and curated
manual corrections into the Found in Space publication view: a canonical
HEALPix-sharded Parquet dataset for downstream spatial builds.

The repository is the primary processing pipeline for producing that single
view, with supporting code for source downloads, staging, overrides, merge
decisions, and publication evidence.

## Quick Start

Requires Python 3.13 or newer and `uv`.

```bash
uv sync
uv run fis-pipeline --help
uv run fis-pipeline project init-run --profile small \
  /data/fis/runs/stars/stars-small-dev
uv run fis-pipeline project validate --project \
  /data/fis/runs/stars/stars-small-dev/pipeline.toml
uv run fis-pipeline overrides download --project \
  /data/fis/runs/stars/stars-small-dev/pipeline.toml
```

The named run directory keeps the exact configuration and immutable command
provenance together. Catalog payloads may remain on specialist volumes through
explicit paths in `pipeline.toml`. The `small` profile uses real catalog
downloads, including the scripted Gaia path, and pins the published stellar
override catalog by its release-specific Zenodo DOI and archive SHA-256.

For the guided path, start with [docs/README.md](docs/README.md).

## CLI

Main entry point:

```bash
uv run fis-pipeline --help
```

Command groups:

- `project` - create and validate ad-hoc or named controlled projects.
- `gaia` - download Gaia VOTables and process them into staged Parquet.
- `hip` - download and process Hipparcos.
- `gaia-to-hip` - download and build the Gaia-to-Hipparcos crossmatch sidecar.
- `identifiers` - download and build sparse name/designation sidecars.
- `overrides` - acquire a version-pinned Zenodo release and build
  merger-ready manual overrides from its YAML catalogs.
- `merge` - stream Gaia, Hipparcos, crossmatch, and overrides into merged shards.

See [docs/run-configuration.md](docs/run-configuration.md) for the release
oriented command flow and run-capture requirements.

## Project Layout

```text
src/foundinspace/pipeline/
  common/          shared coordinate and photometry helpers
  gaia/            Gaia input -> staged Parquet
  hipparcos/       Hipparcos input -> staged Parquet
  gaia_to_hip/     Gaia DR3 <-> Hipparcos crossmatch sidecar
  identifiers/     sparse name/designation sidecar
  overrides/       version-pinned acquisition and curated-row processing
  merge/           canonical merged output
  constants.py     shared schema and quality flags
  project.py       pipeline.toml contract and profile rendering
  provenance.py    controlled-run command evidence
  schema.py        supported output schema exports
  sharding.py      supported downstream sharding API
  validation.py    supported release-validation API
  cli.py           one CLI entry point
```

Checked-in project templates live in [profiles/](profiles/). They are TOML
templates only; catalog data is not stored in git.

Repository-only evidence collection and review workflows live under
`tools/curation/`; they are not installed as pipeline runtime modules.

## Documentation

- [docs/README.md](docs/README.md) - documentation entry point.
- [docs/run-configuration.md](docs/run-configuration.md) - usage, project file, Gaia query bundle, and publication run capture.
- [docs/scientific-provenance.md](docs/scientific-provenance.md) - source catalogs, calculations, field contracts, and quality provenance.
- [docs/pairing-resolution.md](docs/pairing-resolution.md) - policy-neutral Gaia/Hipparcos evidence, proposed pipeline resolution, corrected exploratory sizing, and readiness gaps.
- [docs/output-contract.md](docs/output-contract.md) - merged HEALPix output, deduplication, overrides, reports, and sidecars.
- [docs/sun-reference-policy.md](docs/sun-reference-policy.md) - why the solar origin and optional Sun object are outside the core stellar catalog.

## Development

```bash
uv run pytest -q
uv run ruff check
```

Agent and tooling notes are in [AGENTS.md](AGENTS.md).
