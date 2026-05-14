# Output Schema

The dense staged and merged star tables use the columns in
`foundinspace.pipeline.constants.OUTPUT_COLS`.

| Column | Meaning |
| --- | --- |
| `source` | Catalog namespace, such as `gaia`, `hip`, or `manual`. |
| `source_id` | Identifier within `source`. |
| `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` | Sun-centered Cartesian ICRS position in parsecs. |
| `ra_deg`, `dec_deg`, `r_pc` | Sky position and distance at the canonical epoch. |
| `mag_abs` | Absolute magnitude used by downstream renderers. |
| `teff` | Effective temperature in kelvin. |
| `quality_flags` | Packed provenance and validity bits. |
| `astrometry_quality` | Finite score used for distance/astrometry comparisons. |
| `photometry_quality` | Magnitude uncertainty estimate. |

Catalog staging outputs may include auxiliary columns used by the merge policy,
for example Gaia `ruwe` and `phot_g_mean_mag`, or Hipparcos `Sn` and `Hpmag`.
Merged dense shards keep only the canonical output columns.

Identifiers are intentionally sparse and live in the identifiers sidecar, not in
the dense star table.
