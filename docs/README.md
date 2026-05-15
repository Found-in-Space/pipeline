# Pipeline Documentation

This documentation is organized by how you want to approach the project.

## Run the Pipeline

- [Start here](start-here.md) for the shortest path through setup and commands.
- [Project file](project-file.md) explains the `project.toml` contract.
- [Profiles](profiles.md) explains the checked-in `full` and `small` templates.
- [Feed downloads](operations/feed-downloads.md) covers the current download workflow.
- [Gaia download](operations/gaia-download.md) walks through scripted and
  browser-assisted Gaia VOTable downloads.

## Understand the Data Choices

- [Parallax and distance](concepts/parallax-distance.md)
- [Magnitude and temperature](concepts/magnitude-temperature.md)
- [Coordinates and epochs](concepts/coordinates-epochs.md)
- [Merge policy](reference/merge-policy.md)
- [Output schema](reference/output-schema.md)
- [Quality flags](reference/quality-flags.md)

## Modify the Code

Each stage has a short ownership page:

- [Gaia](stages/gaia.md)
- [Hipparcos](stages/hipparcos.md)
- [Gaia to Hipparcos](stages/gaia-to-hip.md)
- [Identifiers](stages/identifiers.md)
- [Overrides](stages/overrides.md)
- [Merge](stages/merge.md)

For command details, see [reference/cli.md](reference/cli.md).
