# Found in Space — Pipeline

Part of [Found in Space](https://foundin.space/), a project that turns real astronomical measurements into interactive explorations of the solar neighbourhood.

This repository is the **data pipeline**: it takes raw catalogue data from ESA's [Gaia](https://www.cosmos.esa.int/web/gaia) mission and the Hipparcos survey, applies astrometry and photometry selection, cross-matches and merges the catalogues, and outputs clean, positioned star records ready for visualisation or further analysis.

## Output

The pipeline produces compressed Parquet tables with a fixed schema. Each row is a star with:

- **Sun-centric Cartesian coordinates** (ICRS frame, J2016.0): `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` (parsecs). The origin is the Sun; axes follow the ICRS convention.
- **Identifiers and source**: `source` (e.g. `"gaia"` or `"hip"`), `source_id`.
- **Photometry**: `mag_abs` (absolute magnitude), `teff` (effective temperature, K), `photometry_quality` (magnitude uncertainty).
- **Provenance and quality**: `quality_flags` (packed uint16: distance source, Teff source, photometry source, validity and review bits), `astrometry_quality` (e.g. fractional parallax error or Bailer-Jones interval width; finite, no inf).

See `OUTPUT_COLS` in `foundinspace.pipeline.constants` for the full list.

## How it works

Per-catalogue CLIs produce staging Parquet. The **merge** step (see `docs/mergers.md`) brings them together:

1. Run **Gaia** and **Hipparcos** pipelines on their respective inputs, and build the **Gaia↔HIP** mapping sidecar (`fis-pipeline gaia-to-hip build`; Hipparcos: `fis-pipeline hip build`).
2. Run an **overrides pipeline** that normalises a versioned **manual overrides** table (e.g. missing objects like the Sun, or replacements where Hipparcos binary solutions are poor).
3. **Merge** using a cross-match table, quality scoring for Gaia-vs-Hip pairs, with **manual overrides taking precedence** over automatic winners.
4. Emit a **dense** merged table, partitioned by **HEALPix**, containing one canonical row per star.
5. Emit **sparse sidecars** (identifiers, HD/Bayer designations, merge decisions) keyed by canonical `(source, source_id)`, so rare columns are not duplicated on every row.

## Installation

Requires Python ≥3.13. From the project root:

```bash
uv sync
```

Dependencies include `astropy`, `pandas`, `pyarrow`, and `votpipe` (see `pyproject.toml`).

## CLI

Entry point: **`fis-pipeline`** (or `python -m foundinspace.pipeline`).

| Command | Description |
|--------|--------------|
| `fis-pipeline gaia build --project PROJECT INPUT [INPUT ...]` | Read Gaia VOTable(s) (`.vot`, `.vot.gz`, `.vot.xz`), run the Gaia pipeline per batch, and write `{stem}.parquet` into `[gaia] output_dir`. Optional `[gaia] mag_limit` filters to `phot_g_mean_mag <= mag_limit`. |
| `fis-pipeline gaia-to-hip build --project PROJECT` | Download `gaiadr3.hipparcos2_best_neighbour` (skip if cached) and write `[gaia-to-hip] output_parquet` (Gaia↔HIP sidecar for the merger). |
| `fis-pipeline gaia-to-hip download --project PROJECT` | Only download `gaiadr3.hipparcos2_best_neighbour` into `[gaia-to-hip] download_ecsv`. |
| `fis-pipeline hip build --project PROJECT` | Download Hipparcos New Reduction catalog (skip if cached) and write to `[hip] output_parquet`. |
| `fis-pipeline hip download --project PROJECT` | Only download Hipparcos New Reduction catalog (`I/311/hip2`) to `[hip] download_ecsv`. |
| `fis-pipeline identifiers build --project PROJECT` | Download identifier source catalogs (skip if cached) and build a wide identifiers sidecar parquet keyed by `(source, source_id)` at `[identifiers] output_parquet`. |
| `fis-pipeline identifiers download --project PROJECT` | Only download identifier source catalogs (`I/239/hip_main`, `IV/27A/catalog`, `IV/27A/table3`) to ECSV targets in `[identifiers]`. |
| `fis-pipeline overrides build --project PROJECT` | Build merger-ready overrides Parquet from YAML (`OUTPUT_COLS` + override metadata) and write to `[overrides] output_parquet`. |
| `fis-pipeline merge build --project PROJECT` | Stream-merge Gaia + Hipparcos + crossmatch + overrides into HEALPix-partitioned Parquet under `[merge] output_dir/healpix/{pixel}/`, and write `merge_report.json` plus `merge_decisions.parquet`. Reads from `[gaia]`, `[hip]`, `[gaia-to-hip]`, `[overrides]`, and `[merge]` sections. |

### Project configuration

Pipeline commands require `--project path/to/project.toml`. The project file is the single source of truth for catalog/output paths and merge settings. Each CLI group has its own TOML section (`[gaia]`, `[gaia-to-hip]`, `[hip]`, `[identifiers]`, `[overrides]`, `[merge]`). Only the sections needed by the command you run must be present — missing sections fail fast on access. Runtime behavior settings also live in the project file (for example, `[gaia] mag_limit`).

Generate a starter file:

```bash
uv run fis-pipeline project init project.toml
```

Path values in the project file may be absolute or relative to the project file directory. Project TOML values do not expand environment variables.

Cross-section dependencies are explicit: `merge build` reads inputs from `[gaia]`, `[hip]`, `[gaia-to-hip]`, and `[overrides]`. `identifiers build` optionally reads `[gaia-to-hip] output_parquet` for Gaia DR3 cross-matching.

**Identifiers sidecar schema (`identifiers build`)**

The sidecar is intentionally **wide** and small, keyed by the compound key `(source, source_id)`:

- `source` (`string` — catalog namespace: `"hip"`, `"manual"`, etc.)
- `source_id` (`string` — identifier within that namespace)
- `gaia_source_id` (`Int64`, nullable — Gaia DR3 source identifier, populated for HIP stars via crossmatch)
- `hip_id` (`Int64`, nullable — Hipparcos catalog number)
- `hd` (`Int64`, nullable — Henry Draper catalog number)
- `bayer` (`string`, nullable — display value such as `"alpha Cas"`)
- `flamsteed` (`Int64`, nullable — Flamsteed number from IV/27A)
- `constellation` (`string`, nullable — constellation abbreviation from IV/27A)
- `proper_name` (`string`, nullable — proper name from IV/27A/table3 or override YAML)

Sources: Hipparcos-keyed entries are derived from Vizier catalogs (`I/239/hip_main`, `IV/27A`). The Gaia DR3 source ID for HIP stars comes from the crossmatch table when `[gaia-to-hip] output_parquet` is configured and present. Manual override stars with an `identifiers` block in their YAML are merged from packaged data, or from `[overrides] data_dir` if configured.

**Overrides table schema (`overrides build`)**

One row per YAML override star: same columns as `OUTPUT_COLS` plus `override_id`, `action`, `override_reason`, `override_policy_version`. Non-drop rows set `quality_flags` with `DIST_SRC_OVERRIDE` (see `constants.py`) and `FLAG_DIST_VALID`; `astrometry_quality` and `photometry_quality` are `0.0` (placeholders). `drop` rows keep `source` / `source_id` for targeting and null the remaining `OUTPUT_COLS` fields (including `ra_deg`, `dec_deg`, `r_pc`). `source_id` is stored as string for consistent merger matching.

## Pipeline stages (Gaia)

For each batch of Gaia data the pipeline:

1. **Astrometry** (`gaia.astrometry.select_astrometry_gaia`) — Multi-tier distance: Tier A = best of DR3 / BJ geometric / BJ photogeometric (quality-tested); Tier B = weak catalog fallback; Tier C = photometric (M_G + A_G); Tier D = synthetic prior. Sets `distance_use_pc`, `quality_flags`, `*_use_*` columns. No row dropped for bad astrometry alone.
2. **Photometry** (`gaia.photometry.assign_photometry_gaia`, `compute_mag_abs_gaia`, `compute_teff_gaia`) — Apparent magnitude (G), absolute magnitude (GSP-Phot / extinction-corrected / distance-modulus cascade), and effective temperature (spectroscopic → BP–RP → B–V → default).
3. **Coordinates** (`common.coords.calculate_coordinates_fast`) — Propagates positions to J2016.0 using proper motions (no radial velocity) and computes `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, plus `ra_deg`, `dec_deg`, `r_pc`.

Result columns are trimmed to `OUTPUT_COLS` and written as compressed Parquet (zstd).

## Code layout

```
src/foundinspace/
  pipeline/
    cli.py              # Click root; lazy subcommands "gaia", "gaia-to-hip", "hip", "identifiers", "merge", "overrides", "project"
    constants.py        # OUTPUT_COLS, quality_flags (DIST_SRC_*, TEFF_SRC_*, PHOT_SRC_*, FLAG_*), qf_* accessors
    __main__.py         # python -m entry
    common/
      coords.py         # calculate_coordinates, calculate_coordinates_fast (ICRS → x,y,z at J2016.0)
      photometry.py     # teff_to_rgb, teff_to_hex, bv_to_teff, bp_rp_to_teff
    gaia/
      cli.py            # gaia build
      pipeline.py      # Stream VOTable → batches → Parquet (main)
    gaia_to_hip/
      cli.py            # gaia-to-hip build, download
      download.py      # TAP fetch hipparcos2_best_neighbour → ECSV
      pipeline.py      # ECSV → gaia_hip_map.parquet
      astrometry.py    # select_astrometry_gaia (DR3 / BJ_GEO / BJ_PHOTOGEO)
      photometry.py    # assign_photometry_gaia, compute_mag_abs_gaia, compute_teff_gaia
    hipparcos/
      cli.py           # hip build, download
      download.py      # fetch Hipparcos ECSV from Vizier
      pipeline.py      # ECSV -> Hipparcos transforms -> parquet
      astrometry.py    # select_astrometry_hip (Hipparcos-only)
      photometry.py    # assign_photometry_hip, compute_mag_abs_hip, compute_teff_hip
    identifiers/
      cli.py           # identifiers build, download
      download.py      # fetch HIP/HD + Bayer/Flamsteed + proper-name catalogs
      pipeline.py      # build wide identifiers sidecar parquet
    overrides/
      cli.py           # overrides build
      loader.py        # load/normalize YAML override stars
      pipeline.py      # YAML → overrides.parquet for merger
```

## Key functions

- **`calculate_coordinates`** / **`calculate_coordinates_fast`** (`common.coords`) — Propagate ICRS positions to J2016.0 and add `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`, `ra_deg`, `dec_deg`, `r_pc`. The fast version uses pure NumPy (no Astropy) and assumes no radial velocity.
- **`select_astrometry_gaia`** (`gaia.astrometry`) — Four-tier distance selection (primary → weak catalog → photometric → synthetic prior) with `quality_flags` (uint16) and finite `astrometry_quality`; populates `distance_use_pc` and all `*_use_*` astrometry columns.
- **`assign_photometry_gaia`** / **`compute_mag_abs_gaia`** / **`compute_teff_gaia`** (`gaia.photometry`) — Gaia G mag, absolute magnitude cascade, and Teff cascade (spectroscopic → BP–RP → B–V → 5800 K default).

## Tests

```bash
uv run pytest
```

Tests live under `tests/` (e.g. `tests/common/test_coords.py` for coordinate propagation).
