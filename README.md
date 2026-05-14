# Found in Space Pipeline

Prepare stellar catalog data for [foundin.space](https://foundin.space/).

This project turns Gaia and Hipparcos catalog sources into clean, documented
Parquet artifacts: staged catalog tables, identifier sidecars, manual override
tables, and a canonical HEALPix-sharded merge that can be consumed by downstream
octree builders and viewers.

The repository is both a production data-preparation tool and a learning path
for people who want to understand how stellar catalog data becomes a 3D star
dataset.

## Quick Start

Requires Python 3.13 or newer and `uv`.

```bash
uv sync
uv run fis-pipeline --help
uv run fis-pipeline project init --profile small project.toml
```

The `small` profile uses real catalog downloads where the repo already supports
automation. Gaia VOTable download automation is planned next; until then,
`[gaia] input_dir` is where Gaia files should be placed before running
`fis-pipeline gaia build`.

For the guided path, start with [docs/start-here.md](docs/start-here.md).

## CLI

Main entry point:

```bash
uv run fis-pipeline --help
```

Command groups:

- `project` - write starter project files.
- `gaia` - process Gaia VOTables into staged Parquet.
- `hip` - download and process Hipparcos.
- `gaia-to-hip` - download and build the Gaia-to-Hipparcos crossmatch sidecar.
- `identifiers` - download and build sparse name/designation sidecars.
- `overrides` - build merger-ready manual overrides from YAML.
- `merge` - stream Gaia, Hipparcos, crossmatch, and overrides into merged shards.

See [docs/reference/cli.md](docs/reference/cli.md) for the command reference.

## Project Layout

```text
src/foundinspace/pipeline/
  common/          shared coordinate and photometry helpers
  gaia/            Gaia input -> staged Parquet
  hipparcos/       Hipparcos input -> staged Parquet
  gaia_to_hip/     Gaia DR3 <-> Hipparcos crossmatch sidecar
  identifiers/     sparse name/designation sidecar
  overrides/       manual curated rows
  merge/           canonical merged output
  constants.py     shared schema and quality flags
  project.py       project.toml contract and profile rendering
  cli.py           one CLI entry point
```

Checked-in project templates live in [profiles/](profiles/). They are TOML
templates only; catalog data is not stored in git.

## Documentation

- [docs/README.md](docs/README.md) - documentation map.
- [docs/profiles.md](docs/profiles.md) - full and small project profiles.
- [docs/project-file.md](docs/project-file.md) - `project.toml` contract.
- [docs/reference/output-schema.md](docs/reference/output-schema.md) - output columns.
- [docs/reference/quality-flags.md](docs/reference/quality-flags.md) - packed quality flags.
- [docs/reference/merge-policy.md](docs/reference/merge-policy.md) - canonical merge policy.

## Development

```bash
uv run pytest -q
uv run ruff check
```

Agent and tooling notes are in [AGENTS.md](AGENTS.md).
